package requeue_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	requeue "github.com/nickpoorman/nats-requeue"
	"github.com/nickpoorman/nats-requeue/internal/republisher"
	"github.com/nickpoorman/nats-requeue/protocol"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func cleanUp(t *testing.T, path string) {
	t.Cleanup(func() {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	})
}

func setup(t *testing.T) string {
	dir, err := ioutil.TempDir("", fmt.Sprintf("%s-*", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	cleanUp(t, dir)
	return dir
}

func Test_RequeueEndToEnd(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.NoLevel)
	done := true

	s := natsserver.RunDefaultServer()
	// defaultOpts := natsserver.DefaultTestOptions
	// defaultOpts.NoSigs = false
	// s := natsserver.RunServer(&defaultOpts)
	t.Cleanup(func() {
		s.Shutdown()
	})

	// Create a directory just for this test.
	dataDir := setup(t)

	// To not collide with other tests.
	subject := nats.NewInbox()

	clientURL := s.ClientURL()
	// clientURL := "localhost:4222"
	t.Logf("running nats on: %s", clientURL)

	// NATS connect Options.
	natsOpts := []nats.Option{
		nats.Name(fmt.Sprintf("test_requeue_%s", t.Name())),
		nats.MaxReconnects(10),
	}

	ctx, cancel := context.WithCancel(context.Background())

	rc, err := requeue.Connect(
		requeue.ConnectContext(ctx),
		requeue.DataDir(dataDir),
		requeue.NATSOptions(natsOpts),
		requeue.NATSServers(clientURL),
		requeue.NATSSubject(subject),
		requeue.NATSQueueName(requeue.DefaultNatsQueueName),
		requeue.RepublisherOptions(
			republisher.RepublishInterval(1*time.Second),
			// republisher.MaxInFlight(1),
		),
	)
	if err != nil {
		t.Fatalf("Error on requeue connect: %v", err)
	}

	nc, err := nats.Connect(
		clientURL,
		nats.DisconnectErrHandler(func(con *nats.Conn, err error) {
			if !done {
				t.Errorf("nats-producer: DisconnectErrHandler: %s", err.Error())
			}
		}),
		nats.ReconnectHandler(func(con *nats.Conn) {
			t.Logf("nats-producer: Got reconnected to %s!", con.ConnectedUrl())
		}),
		nats.ClosedHandler(func(con *nats.Conn) {
			t.Log("nats-producer: ClosedHandler")
		}),
		nats.ErrorHandler(func(con *nats.Conn, sub *nats.Subscription, err error) {
			t.Errorf("nats-producer: ErrorHandler: Got err: conn=%s sub=%s err=%w", con.Opts.Name, sub.Subject, err)
		}),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}

	var eventsMu sync.Mutex
	events := make(map[string]struct{})

	total := 100
	pending := int64(total)
	group, _ := errgroup.WithContext(context.Background())
	ch := make(chan int)

	var republishedWG sync.WaitGroup
	republishedWG.Add(total)

	originalSubject := "foo.bar.baz"
	_, err = nc.Subscribe(originalSubject, func(msg *nats.Msg) {
		t.Logf("got replayed message: %s", string(msg.Data))
		if err := msg.Respond(nil); err != nil {
			t.Fatal(fmt.Errorf("failed to respond to message: %w", err))
		}
		eventsMu.Lock()
		m := string(msg.Data)
		if _, ok := events[m]; ok {
			delete(events, m)
			republishedWG.Done()
			t.Logf("replay events remaining: %d", len(events))
		}
		eventsMu.Unlock()
	})
	assert.Nil(t, err)

	go func() {
		// Generate all our events
		for i := 0; i < total; i++ {
			ch <- i
		}
		close(ch)
	}()

	// Send some events for requeue to persist
	for i := 0; i < runtime.NumCPU(); i++ { // Throttle in flight requests
		group.Go(func(i int) func() error {
			return func() error {
				for i := range ch {
					err := func(i int) error {
						defer func() {
							left := atomic.AddInt64(&pending, -1)
							t.Logf("number left: %d", left)
						}()

						payload := buildPayload(i, originalSubject)
						eventsMu.Lock()
						events[string(payload.OriginalPayload)] = struct{}{}
						eventsMu.Unlock()

						msg, err := requeue.RetryRequest(nc, subject, payload.Bytes(), 15*time.Second, 100000)
						if err != nil {
							if err == requeue.RequeueRequestRetriesExceededError {
								return err
							}
							if nc.LastError() != nil {
								// log.Fatal().Msgf("%v for request", nc.LastError())
								return fmt.Errorf("last error for request: %w", nc.LastError())
							}
							// log.Fatal().Msgf("%v for request", err)
							return fmt.Errorf("for request: %w", err)
						}
						if len(msg.Data) > 0 {
							// For performace, we should be able to send ACK with an empty
							// payload.
							// t.Errorf("Expected the ACK to be empty but got %s", string(msg.Data))
							return fmt.Errorf("Expected the ACK to be empty but got %s", string(msg.Data))
						}
						// log.Debug().Msgf("got ack for: %d", i)
						return nil
					}(i)
					if err != nil {
						return err
					}
				}
				return nil
			}
		}(i))
	}

	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}

	// log.Info().Int64("pending", pending).Msg("left terminated")
	t.Logf("pending: %d", pending)

	// Wait for all the messages to have been republished
	republishedWG.Wait()

	t.Log("got all messages")

	done = true

	t.Log("canceling requeue context")
	cancel()

	<-rc.HasBeenClosed()
	t.Log("requeue closed")
}

func buildPayload(i int, originalSubject string) protocol.RequeueMessage {
	msg := protocol.DefaultRequeueMessage()
	msg.Retries = 1
	msg.TTL = uint64(5 * 24 * time.Hour)
	msg.Delay = uint64(1 * time.Nanosecond)
	msg.BackoffStrategy = protocol.BackoffStrategy_Exponential
	// msg.QueueName = "default"
	msg.OriginalSubject = originalSubject
	msg.OriginalPayload = []byte(fmt.Sprintf("my awesome payload %d", i))
	return msg
}
