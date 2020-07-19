package queue

import (
	"sync"

	badger "github.com/dgraph-io/badger/v2"
)

// The manager manages the queues.
type Manager struct {
	db *badger.DB

	mu         sync.RWMutex
	queues     []*Queue
	priorities map[string]int
}

// NewManger creates a NewManager responsible for managing the queues.
func NewManager(db *badger.DB) (*Manager, error) {
	m := &Manager{
		db:         db,
		priorities: make(map[string]int),
	}
	if err := m.loadFromDisk(); err != nil {
		return nil, err
	}
	return m, nil
}

// loadFromDisk will load the queues from disk into memory.
func (m *Manager) loadFromDisk() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// List out all the queues under the namespace and load up each one.
	return m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(QueueKey{
			Namespace: QueuesNamespace,
			Bucket:    StateBucket,
		}.BucketPath() + ".")

		q := &Queue{}

		addQ := func() {
			m.addQueue(q)
			q = &Queue{}
		}

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			qk := ParseQueueKey(key)
			if q.name != "" && qk.Name != q.name {
				// Started a new queue
				addQ()
			}
			if err := q.SetKV(qk, value); err != nil {
				return err
			}
		}
		// Add the queue from the final iteration if there is one.
		if q.name != "" {
			addQ()
		}
		return nil
	})
}

func (m *Manager) CreateQueue(name string) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	queue, err := createQueue(m.db, name)
	if err != nil {
		return nil, err
	}

	// Save it in memory
	m.queues = append(m.queues, queue)

	return queue, nil
}

// Add the queue in memory.
// Should be called with lock acquired.
func (m *Manager) addQueue(q *Queue) {
	m.queues = append(m.queues, q)
}

func (m *Manager) Queues() []*Queue {
	return m.queues
}
