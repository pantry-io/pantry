package statspub

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/internal/key"
	"github.com/nickpoorman/nats-requeue/internal/queue"
	"github.com/nickpoorman/nats-requeue/protocol"
	"github.com/stretchr/testify/assert"
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

func TestPublish(t *testing.T) {
	// Create a tmp badger database
	dir := setup(t)
	openOpts := badger.DefaultOptions(dir)
	db, err := badger.Open(openOpts)
	assert.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	qManager, err := queue.NewManager(db)
	assert.NoError(t, err)

	// Create a queue
	queueName := "high-priority"
	msgQueue, err := qManager.CreateQueue(queue.QueueKey{Name: queueName})
	assert.NoError(t, err)
	t.Cleanup(func() {
		msgQueue.Close()
	})

	// Add some messages to our queue
	commitCb := func(err error) {
		assert.NoError(t, err)
	}
	kq := queue.NewQueueKeyForMessage(queueName, key.New(time.Now()))
	assert.NoError(t, msgQueue.AddMessage(kq.Bytes(), []byte("foo"), 24*time.Hour, commitCb))

	s := natsserver.RunRandClientPortServer()
	t.Cleanup(func() {
		s.Shutdown()
	})
	ncSub, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	t.Cleanup(func() {
		ncSub.Close()
	})

	instanceId := "Instance1234"
	wait := make(chan struct{})

	// Subscribe and wait for a message with stats.
	_, err = ncSub.Subscribe(StatsSubject, func(msg *nats.Msg) {
		ism := protocol.InstanceStatsMessageFromNATS(msg)
		validateInstanceStats(t, instanceId, queueName, ism)
		wait <- struct{}{}
	})
	assert.NoError(t, err)

	ncPub, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	t.Cleanup(func() {
		ncPub.Close()
	})

	spub, err := NewStatsPublisher(ncPub, qManager, instanceId, StatsPublishInterval(500*time.Millisecond))
	assert.NoError(t, err)
	t.Cleanup(func() {
		spub.Close()
	})

	<-wait
	// Done.
}

func validateInstanceStats(t *testing.T, instanceId string, queueName string, ism protocol.InstanceStatsMessage) {
	// Assert everything is correct.
	assert.Equal(t, instanceId, ism.InstanceId)
	queues := ism.Queues
	for _, q := range queues {
		assert.Equal(t, queueName, q.QueueName)
		assert.Equal(t, int64(1), q.Enqueued)
		assert.Equal(t, int64(0), q.InFlight) // Republisher isn't running.
	}
	assert.Len(t, ism.Queues, 1)
}
