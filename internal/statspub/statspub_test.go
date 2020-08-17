package statspub

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
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

// func TestPublish(t *testing.T) {
// 	// Create a tmp badger database
// 	dir := setup(t)
// 	openOpts := badger.DefaultOptions(dir)
// 	db, err := badger.Open(openOpts)
// 	assert.NoError(t, err)
// 	defer db.Close()

// 	qManager, err := queue.NewManager(db)
// 	assert.NoError(t, err)

// 	// Create a queue
// 	queueName := "high-priority"
// 	msgQueue, err := qManager.CreateQueue(queue.QueueKey{Name: queueName})
// 	assert.NoError(t, err)
// 	defer msgQueue.Close()

// 	commitCb := func(err error) {
// 		assert.NoError(t, err)
// 	}

// 	// Add some messages to our queue
// 	kq := queue.NewQueueKeyForMessage(queueName, key.New(time.Now()))
// 	assert.NoError(t, msgQueue.AddMessage(kq.Bytes(), []byte("foo"), 24*time.Hour, commitCb))

// 	s := natsserver.RunDefaultServer()
// 	defer s.Shutdown()
// 	ncSub, err := nats.Connect(s.ClientURL())
// 	assert.NoError(t, err)
// 	defer ncSub.Close()

// 	// Subscribe and wait for a message with stats.
// 	sub, err := ncSub.SubscribeSync(StatsSubject)
// 	assert.NoError(t, err)
// 	// assert.NoError(t, sub.AutoUnsubscribe(1))

// 	instanceId := "Instance1234"

// 	ncPub, err := nats.Connect(s.ClientURL())
// 	assert.NoError(t, err)
// 	defer ncPub.Close()
// 	spub, err := NewStatsPublisher(ncPub, qManager, instanceId, StatsPublishInterval(time.Hour))
// 	assert.NoError(t, err)
// 	defer spub.Close()

// 	// Call publish explicitly.
// 	assert.NoError(t, spub.publish())

// 	// Get the published stats message.
// 	msg, err := sub.NextMsg(time.Hour)
// 	assert.NoError(t, err)
// 	ism := protocol.InstanceStatsMessageFromNATS(msg)

// 	// Assert everything is correct.
// 	assert.Equal(t, instanceId, ism.InstanceId)
// }

func TestPublish(t *testing.T) {
	// Create a tmp badger database
	dir := setup(t)
	openOpts := badger.DefaultOptions(dir)
	db, err := badger.Open(openOpts)
	assert.NoError(t, err)
	defer db.Close()

	qManager, err := queue.NewManager(db)
	assert.NoError(t, err)

	// Create a queue
	queueName := "high-priority"
	msgQueue, err := qManager.CreateQueue(queue.QueueKey{Name: queueName})
	assert.NoError(t, err)
	defer msgQueue.Close()

	commitCb := func(err error) {
		assert.NoError(t, err)
		t.Log("committed message")
	}

	// Add some messages to our queue
	kq := queue.NewQueueKeyForMessage(queueName, key.New(time.Now()))
	assert.NoError(t, msgQueue.AddMessage(kq.Bytes(), []byte("foo"), 24*time.Hour, commitCb))

	s := natsserver.RunDefaultServer()
	defer s.Shutdown()
	ncSub, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer ncSub.Close()

	instanceId := "Instance1234"
	var wg sync.WaitGroup
	wg.Add(1)

	// Subscribe and wait for a message with stats.
	_, err = ncSub.Subscribe(StatsSubject, func(msg *nats.Msg) {
		ism := protocol.InstanceStatsMessageFromNATS(msg)

		// Assert everything is correct.
		assert.Equal(t, instanceId, ism.InstanceId)

		wg.Done()
	})
	assert.NoError(t, err)
	// assert.NoError(t, sub.AutoUnsubscribe(1))

	ncPub, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer ncPub.Close()

	// TODO: Creating a queue will create a stats publisher. No need for us to create this one manually...
	// spub, err := NewStatsPublisher(ncPub, qManager, instanceId, StatsPublishInterval(time.Hour))
	// assert.NoError(t, err)
	// defer spub.Close()

	// Call publish explicitly.
	// assert.NoError(t, )

	// Get the published stats message.
	// msg, err := sub.NextMsg(time.Hour)
	// assert.NoError(t, err)

	wg.Wait()
}
