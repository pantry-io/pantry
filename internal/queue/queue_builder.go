package queue

import (
	"errors"

	"github.com/dgraph-io/badger/v2"
)

var (
	DifferentQueueNameError = errors.New("different queue name")
)

type kv struct {
	k QueueKey
	v []byte
}

// QueueBuilder is used to scan in the key, value pairs and incrementally build
// up a new queue from them.
type QueueBuilder struct {
	kvs  []kv
	name string
}

func NewQueueBuilder() *QueueBuilder {
	return &QueueBuilder{}
}

// Build will create a new Queue and then call Reset so that this builder may be
// resued.
func (q *QueueBuilder) Build(db *badger.DB) (*Queue, error) {
	newQ, err := NewQueue(db, q.name)
	if err != nil {
		return nil, err
	}
	q.Reset()
	return newQ, nil
}

// Set a key and value on the builder. This returns a DifferentQueueNameError if
// the key passed in does not match the existing queue.
func (q *QueueBuilder) Set(key, value []byte) error {
	qk := ParseQueueKey(key)
	if q.name != "" {
		// Set the name
		q.name = qk.Name
	} else if q.name != qk.Name {
		// Different names
		return DifferentQueueNameError
	}

	// Set the value
	q.kvs = append(q.kvs, kv{k: qk, v: value})
	return nil
}

// Reset the builder to it's original state.
func (q *QueueBuilder) Reset() {
	q.kvs = nil
	q.name = ""
}

// Returns true if this builder is still in it's zero value state meaning no
// queue can be built from it.
func (q *QueueBuilder) IsZero() bool {
	return q.name == "" && q.kvs == nil
}
