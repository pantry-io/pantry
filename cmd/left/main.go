package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	requeue "github.com/nickpoorman/nats-requeue"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

func main() {
	subject := "requeue.msgs"
	clientURL := "localhost:4222"
	log.Info().Msgf("running nats on: %s", clientURL)

	nc, err := nats.Connect(
		clientURL,
		nats.DisconnectErrHandler(func(con *nats.Conn, err error) {
			log.Info().Msgf("nats-producer: DisconnectErrHandler: conn=%s sub=%s err=%v", con.Opts.Name, err)
		}),
		nats.ReconnectHandler(func(con *nats.Conn) {
			log.Info().Msgf("nats-producer: Got reconnected to %s!", con.ConnectedUrl())
		}),
		nats.ClosedHandler(func(con *nats.Conn) {
			log.Info().Msgf("nats-producer: ClosedHandler: conn=%s sub=%s", con.Opts.Name)
		}),
		nats.ErrorHandler(func(con *nats.Conn, sub *nats.Subscription, err error) {
			log.Err(err).Msgf("nats-producer: ErrorHandler: Got err: conn=%s sub=%s err=%v", con.Opts.Name, sub.Subject, err)
		}),
	)
	if err != nil {
		log.Fatal().Msgf("Error on connect: %v", err)
	}
	defer nc.Close()

	pending := int64(200000)

	group, _ := errgroup.WithContext(context.Background())
	// Send some events for requeue to persist
	for i := 0; i < 200000; i++ {
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

	log.Info().Msg("left terminated")
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
