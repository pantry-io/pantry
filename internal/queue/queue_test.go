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

func TestNewQueue(t *testing.T) {
	// Create a tmp badger database in memory.
	openOpts := badger.DefaultOptions("").
		WithInMemory(true).
		WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(openOpts)
	assert.NoError(t, err)
	defer db.Close()

	queueName := "testqueue"
	q := NewQueue(db, queueName)
	assert.Equal(t, queueName, q.name, "Queue names should be equal")
}

func TestEarliestCheckpoint(t *testing.T) {
	// Create a tmp badger database
	dir := setup(t)
	openOpts := badger.DefaultOptions(dir).
		WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(openOpts)
	assert.NoError(t, err)
	defer db.Close()

	// Create a Queue
	queueName := "testqueue"
	q, err := createQueue(db, queueName)
	assert.NoError(t, err)

	keys := make([]QueueKey, 0, 10)

	// Add some messages to the queue
	for i := 0; i < 10; i++ {
		qk := NewQueueKeyForMessage(queueName, key.New(time.Now()))
		keys = append(keys, qk)

		err := db.Update(func(txn *badger.Txn) error {
			return txn.SetEntry(
				badger.NewEntry(
					qk.Bytes(),
					[]byte(fmt.Sprintf("message body %d", i)),
				),
			)
		})
		assert.NoError(t, err)
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
