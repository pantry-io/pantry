package statspub

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
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

// 	qManager, err := queue.NewManager(db)
// 	assert.NoError(t, err)

// 	// Create a queue
// 	queue, err := qManager.CreateQueue(queue.QueueKey{Name: "high-priority"})
// 	assert.NoError(t, err)

// 	s := natsserver.RunDefaultServer()
// 	nc, err := nats.Connect(s.ClientURL())
// 	assert.NoError(t, err)

// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	var ism protocol.InstanceStatsMessage
// 	sub, err := nc.Subscribe(StatsSubject, func(msg *nats.Msg) {
// 		ism = protocol.InstanceStatsMessageFromNATS(msg)
// 	})
// 	assert.NoError(t, err)
// 	sub.AutoUnsubscribe(1)

// 	instanceId = "Instance1234"

// 	NewStatsPublisher(nc, instanceId)

// 	// Wait for our message
// 	wg.Wait()
// 	ism.InstanceId
// }
