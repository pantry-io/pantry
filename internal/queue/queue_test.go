package queue

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nickpoorman/nats-requeue/internal/key"
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

func TestEarliestCheckpoint(t *testing.T) {
	// Create a tmp badger database
	dir := setup(t)
	openOpts := badger.DefaultOptions(dir)
	db, err := badger.Open(openOpts)
	if err != nil {
		t.Fatal(err)
	}

	// Create a Queue
	queueName := "testqueue"
	q, err := createQueue(db, queueName)
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]QueueKey, 0, 10)

	// Add some messages to the queue
	for i := 0; i < 10; i++ {
		qk := QueueKey{
			Namespace: QueuesNamespace,
			Name:      queueName,
			Bucket:    MessagesBucket,
			Key:       key.New(time.Now()),
		}
		keys = append(keys, qk)

		if err := db.Update(func(txn *badger.Txn) error {
			return txn.SetEntry(
				badger.NewEntry(
					qk.Bytes(),
					[]byte(fmt.Sprintf("message body %d", i)),
				),
			)
		}); err != nil {
			t.Fatal(err)
		}
		t.Logf("Added key [%s] [%d] \n", qk.PropertyPath(), i)
	}

	// Set the checkpoint to the second to last one
	secondToLastKey := keys[8]
	if err := q.UpdateCheckpoint(secondToLastKey.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Now search for the earliest Checkpoint
	earliest, err := q.EarliestCheckpoint(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, keys[0].PropertyPath(), ParseQueueKey(earliest).PropertyPath(), "they should be equal")
}
