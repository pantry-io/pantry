package queues

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v2"
	"github.com/nickpoorman/nats-requeue/internal/key"
)

const queuePrefix = "queues."
const cursorKey = "cursor"

type Queue struct {
	db *badger.DB

	mu   sync.RWMutex
	name string

	cursor []byte
}

func createQueue(db *badger.DB, name string) (*Queue, error) {
	// Create the queue and persist it.
	q := &Queue{
		name:   name,
		cursor: key.First([]byte(name)), // set to the min possible value
	}

	// Save the queue state to disk
	err := q.db.Update(func(txn *badger.Txn) error {
		// Save the cursor for the queue
		if err := txn.Set([]byte(q.QueueKey(cursorKey)), q.cursor); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return q, err
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) UpdateCursor(cursor []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Update it in memory.
	q.cursor = cursor

	// Save it to disk.
	return q.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(q.QueueKey(cursorKey)), cursor)
	})
}

func (q *Queue) QueueKey(subKey string) string {
	return fmt.Sprintf("%s.%s", queueKey(q.Name()), subKey)
}

func queueKey(queueName string) string {
	return fmt.Sprintf("%s%s", queuePrefix, queueName)
}
