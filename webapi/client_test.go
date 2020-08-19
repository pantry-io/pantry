package webapi

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/gorilla/websocket"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/internal/key"
	"github.com/nickpoorman/nats-requeue/internal/queue"
	"github.com/nickpoorman/nats-requeue/internal/statspub"
	"github.com/stretchr/testify/assert"
)

func TestWebsocket(t *testing.T) {
	// Create a tmp badger database
	openOpts := badger.DefaultOptions("").WithInMemory(true)
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
	for i := 0; i < 10; i++ {
		kq := queue.NewQueueKeyForMessage(queueName, key.New(time.Now()))
		assert.NoError(t, msgQueue.AddMessage(kq.Bytes(), []byte("foo"), 24*time.Hour, commitCb))
	}

	s := natsserver.RunRandClientPortServer()
	t.Cleanup(func() {
		s.Shutdown()
	})

	// Create the stats publisher
	instanceId := "Instance1234"
	ncPub, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	t.Cleanup(func() {
		ncPub.Close()
	})
	spub, err := statspub.NewStatsPublisher(ncPub, qManager, instanceId, statspub.StatsPublishInterval(500*time.Millisecond))
	assert.NoError(t, err)
	t.Cleanup(func() {
		spub.Close()
	})

	// ncSub, err := nats.Connect(s.ClientURL())
	// assert.NoError(t, err)
	// t.Cleanup(func() {
	// 	ncSub.Close()
	// })

	// Run the app
	mux, err := RunApp(s.ClientURL())
	assert.NoError(t, err)

	httpServ := httptest.NewServer(mux)
	defer httpServ.Close()

	// Connect to websocket and listen for status messages
	// Convert http://127.0.0.1 to ws://127.0.0.
	u := "ws" + strings.TrimPrefix(httpServ.URL, "http") + "/ws"
	// t.Log("addr: ", httpServ.Config.Addr)
	// u := url.URL{Scheme: "ws", Host: , Path: "/ws"}
	// u := httpServ.URL + "/ws"
	t.Logf("connecting to %s", u)

	c, _, err := websocket.DefaultDialer.Dial(u, nil)
	assert.NoError(t, err)
	defer c.Close()

	t.Log("writing stats subscribe message to server")

	// Subscribe to stats
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"c": 1}`)); err != nil {
		t.Fatal("send subscribe message", err)
	}

	t.Log("waiting for stats message from server")

	_, message, err := c.ReadMessage()
	assert.NoError(t, err)

	// Check the message
	var m StatsMessageEgress
	assert.NoError(t, json.Unmarshal(message, &m))

	// Assert the command is correct
	assert.Equal(t, StatsMessage, m.Command)
	assert.Equal(t, instanceId, m.Instance.InstanceId)
	assert.Equal(t, int64(10), m.Instance.Queues[0].Enqueued)
}
