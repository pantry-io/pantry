package requeue_test

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	requeue "github.com/nickpoorman/nats-requeue"
	"github.com/nickpoorman/nats-requeue/flatbuf"
	"github.com/segmentio/ksuid"
)

func Test_RequeueConnect(t *testing.T) {
	s := natsserver.RunDefaultServer()
	defer s.Shutdown()

	dataPath := fmt.Sprintf("%s/%s_%s", "/tmp/badger", t.Name(), ksuid.New().String())

	// NATS connect Options.
	natsOpts := requeue.GetDefaultOptions().NatsOptions
	natsOpts = append(natsOpts, []nats.Option{
		nats.Name(fmt.Sprintf("test_requeue_%s", t.Name())),
		nats.MaxReconnects(10),
	}...)

	rc, err := requeue.Connect(
		requeue.ConnectContext(context.Background()),
		requeue.BadgerDataPath(dataPath),
		requeue.NATSOptions(natsOpts),
		requeue.NATSServers(s.ClientURL()),
		requeue.NATSSubject("requeue.msgs"),
		requeue.NATSQueueName(requeue.DefaultNatsQueueName),
	)
	if err != nil {
		t.Fatal("Error on requeue connect: %v", err)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}

	// Send some events for requeue to persist
	for i := 0; i < 100; i++ {
		msg, err := nc.Request("requeue.msgs", payload, 2*time.Second)
		if err != nil {
			if nc.LastError() != nil {
				log.Fatalf("%v for request", nc.LastError())
			}
			log.Fatalf("%v for request", err)
		}
	}

	<-rc.HasBeenClosed()
}

// func buildEvent(i int) []byte {
	
// }