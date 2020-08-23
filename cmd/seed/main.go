package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	requeue "github.com/nickpoorman/nats-requeue"
	"github.com/nickpoorman/nats-requeue/protocol"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

func usage() {
	fmt.Printf("Usage: requeue -d <data-dir> [-s server] [-creds file] [-sub subject] [-nats-queue-group group]\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

var showHelp = flag.Bool("h", false, "Show help message")
var natsListenSubject = flag.String("nats-listen-subject", requeue.DefaultNatsSubject, "The subject to listen for messages on. These are the messages that will be queued up.")
var natsURLs = flag.String("nats-urls", requeue.DefaultNatsServers, "Comma separated NATS server URLs ")
var natsUserCredsFile = flag.String("nats-creds", "", "User Credentials File")

func main() {
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	done := true

	// NATS connect Options.
	natsOpts := []nats.Option{
		nats.Name("data seeder"),
		nats.MaxReconnects(10),
		nats.DisconnectErrHandler(func(con *nats.Conn, err error) {
			if !done {
				log.Err(err).Msgf("nats-producer: DisconnectErrHandler: %s", err.Error())
			}
		}),
		nats.ReconnectHandler(func(con *nats.Conn) {
			log.Info().Msgf("nats-producer: Got reconnected to %s!", con.ConnectedUrl())
		}),
		nats.ClosedHandler(func(con *nats.Conn) {
			log.Info().Msg("nats-producer: ClosedHandler")
		}),
		nats.ErrorHandler(func(con *nats.Conn, sub *nats.Subscription, err error) {
			log.Err(err).Msgf("nats-producer: ErrorHandler: Got err: conn=%s sub=%s", con.Opts.Name, sub.Subject)
		}),
	}

	// NATS user UserCredentials.
	if *natsUserCredsFile != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(*natsUserCredsFile))
	}

	nc, err := nats.Connect(
		*natsURLs,
		natsOpts...,
	)
	if err != nil {
		log.Fatal().Err(err).Msgf("Error on connect")
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
		log.Info().Msgf("got replayed message: %s", string(msg.Data))
		if err := msg.Respond(nil); err != nil {
			log.Fatal().Err(fmt.Errorf("failed to respond to message: %w", err)).Send()
		}
		eventsMu.Lock()
		m := string(msg.Data)
		if _, ok := events[m]; ok {
			delete(events, m)
			republishedWG.Done()
			log.Info().Msgf("replay events remaining: %d", len(events))
		}
		eventsMu.Unlock()
	})
	if err != nil {
		log.Fatal().Err(err).Msg("nats subscribe")
	}

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
							log.Info().Msgf("number left: %d", left)
						}()

						payload := buildPayload(i, originalSubject)
						eventsMu.Lock()
						events[string(payload.OriginalPayload)] = struct{}{}
						eventsMu.Unlock()

						msg, err := requeue.RetryRequest(nc, *natsListenSubject, payload.Bytes(), 15*time.Second, 100000)
						if err != nil {
							if nc.LastError() != nil {
								return fmt.Errorf("last error for request: %w", nc.LastError())
							}
							return fmt.Errorf("for request: %w", err)
						}
						if len(msg.Data) > 0 {
							// For performace, we should be able to send ACK with an empty
							// payload.
							return fmt.Errorf("Expected the ACK to be empty but got %s", string(msg.Data))
						}
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
		log.Fatal().Err(err).Msg("problem with publish group")
	}

	log.Info().Msgf("pending: %d", pending)

	done = true

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
