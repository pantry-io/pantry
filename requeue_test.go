package requeue_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	requeue "github.com/nickpoorman/nats-requeue"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/ksuid"
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

func Test_RequeueConnect(t *testing.T) {
	// s := natsserver.RunDefaultServer()
	// defaultOpts := natsserver.DefaultTestOptions
	// defaultOpts.NoSigs = false
	// s := natsserver.RunServer(&defaultOpts)
	// t.Cleanup(func() {
	// 	s.Shutdown()
	// })

	dir := setup(t)
	dataPath := fmt.Sprintf("%s/%s_%s", dir, t.Name(), ksuid.New().String())
	subject := "requeue.msgs"

	// clientURL := s.ClientURL()
	clientURL := "localhost:4222"
	log.Info().Msgf("running nats on: %s", clientURL)

	// NATS connect Options.
	natsOpts := requeue.GetDefaultOptions().NatsOptions
	natsOpts = append(natsOpts, []nats.Option{
		nats.Name(fmt.Sprintf("test_requeue_%s", t.Name())),
		nats.MaxReconnects(10),
	}...)

	ctx, cancel := context.WithCancel(context.Background())

	rc, err := requeue.Connect(
		requeue.ConnectContext(ctx),
		requeue.BadgerDataPath(dataPath),
		requeue.NATSOptions(natsOpts),
		requeue.NATSServers(clientURL),
		requeue.NATSSubject(subject),
		requeue.NATSQueueName(requeue.DefaultNatsQueueName),
	)
	if err != nil {
		t.Fatalf("Error on requeue connect: %v", err)
	}

	nc, err := nats.Connect(
		clientURL,
		nats.DisconnectErrHandler(func(con *nats.Conn, err error) {
			log.Err(err).Msg("nats-producer: DisconnectErrHandler")
		}),
		nats.ReconnectHandler(func(con *nats.Conn) {
			log.Info().Msgf("nats-producer: Got reconnected to %s!", con.ConnectedUrl())
		}),
		nats.ClosedHandler(func(con *nats.Conn) {
			log.Info().Msg("nats-producer: ClosedHandler")
		}),
		nats.ErrorHandler(func(con *nats.Conn, sub *nats.Subscription, err error) {
			log.Err(err).Msgf("nats-producer: ErrorHandler: Got err: conn=%s sub=%s err=%v", con.Opts.Name, sub.Subject, err)
		}),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}

	total := 1000
	pending := int64(total)

	group, _ := errgroup.WithContext(context.Background())
	// Send some events for requeue to persist
	for i := 0; i < total; i++ {
		group.Go(func(i int) func() error {
			return func() error {
				defer func() {
					left := atomic.AddInt64(&pending, -1)
					log.Debug().Msgf("number left: %d", left)
				}()

				msg, err := nc.Request(subject, buildPayload(i), 3000*time.Minute)
				if err != nil {
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
			}
		}(i))
	}

	if err := group.Wait(); err != nil {
		log.Fatal().Err(err).Send()
	}

	log.Info().Int64("pending", pending).Msg("left terminated")

	cancel()

	// TODO: Verify the events were written to disk

	// TODO:
	// 1. Shut down the requeue instance
	// 2. Then start it back up
	// 3. Spin up a consumer on the original subject
	// 4. Verify we get the requeued messages.

	<-rc.HasBeenClosed()

	log.Info().Msg("right terminated.")
}

func buildPayload(i int) []byte {
	msg := requeue.DefaultRequeueMessage()
	msg.OriginalSubject = "foo.bar.baz"
	msg.OriginalPayload = []byte(fmt.Sprintf("my awesome payload %d", i))
	msg.Meta.BackoffStrategy = requeue.BackoffStrategy_Exponential
	msg.Meta.Delay = 1 * time.Second.Nanoseconds()
	msg.Meta.Retries = 1
	return msg.Bytes()
}
