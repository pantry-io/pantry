package queues

import (
	"sync"

	"github.com/dgraph-io/badger/v2"
)

// The manager manages the queues.
type Manager struct {
	db *badger.DB

	mu     sync.RWMutex
	queues []*Queue
}

// NewManger creates a NewManager responsible for managing the queues.
func NewManager(db *badger.DB) *Manager {
	return &Manager{
		db: db,
	}
}

// load will load the queues for management.
func (m *Manager) load() error {
	// TODO: List out all the queues under the prefix

	return nil
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
