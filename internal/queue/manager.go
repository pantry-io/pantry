package queue

import (
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nickpoorman/nats-requeue/internal/ticker"
	"github.com/rs/zerolog/log"
)

// TODO: Set this to something much higher and allow to be pased to manager.
const checkQueueStatesInterval = 5 * time.Second

// The manager manages the queues.
type Manager struct {
	db                       *badger.DB
	checkQueueStatesInterval time.Duration

	mu     sync.RWMutex
	queues map[string]*Queue

	quit chan struct{}
	done chan struct{}
}

// NewManger creates a NewManager responsible for managing the queues.
func NewManager(db *badger.DB) (*Manager, error) {
	m := &Manager{
		db:                       db,
		checkQueueStatesInterval: checkQueueStatesInterval,
		queues:                   make(map[string]*Queue),
		quit:                     make(chan struct{}),
		done:                     make(chan struct{}),
	}
	if err := m.loadFromDisk(); err != nil {
		return nil, err
	}
	go m.initBackgroundTasks()
	return m, nil
}

func (m *Manager) initBackgroundTasks() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		wg.Wait()
		close(m.done)
	}()
	go func() {
		defer wg.Done()
		<-m.quit
		m.mu.Lock()
		var cWg sync.WaitGroup
		cWg.Add(len(m.queues))
		for _, v := range m.queues {
			go func(q *Queue) {
				defer cWg.Done()
				q.Close()
			}(v)
		}
		cWg.Wait()
	}()

	go func() {
		defer wg.Done()
		t := ticker.New(m.checkQueueStatesInterval)
		go func() {
			<-m.quit
			t.Stop()
		}()
		t.Loop(func() bool {
			m.checkQueueStates()
			return true
		})
	}()
}

// Check all the queue states to make sure we have not missed any.
func (m *Manager) checkQueueStates() {
	log.Debug().Msg("checking queue states")
	// Update the stats for each queue

}

func (m *Manager) UpsertQueueState(qk QueueKey) (*Queue, error) {
	m.mu.RLock()
	q, ok := m.queues[qk.Name]
	m.mu.RUnlock()
	if !ok {
		return m.CreateQueue(qk)
	}
	return q, nil
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
		}.BucketPrefix())

		// Keys are iterated over in order so we can build one queue at a time.
		builder := NewQueueBuilder()

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
			if err := builder.Set(key, value); err != nil {
				if err == DifferentQueueNameError {
					// We've reached a new queue.
					q, err := builder.Build(m.db)
					if err != nil {
						return err
					}
					// Add the queue to our manager.
					m.addQueue(q)
					continue // Move on to next
				}
				return err
			}
		}
		// Add the queue from the final iteration if there is one.
		if !builder.IsZero() {
			q, err := builder.Build(m.db)
			if err != nil {
				return err
			}
			// Add the queue to our manager.
			m.addQueue(q)
		}
		return nil
	})
}

func (m *Manager) CreateQueue(qk QueueKey) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := qk.Name
	if name == "" {
		return nil, fmt.Errorf("queue name cannot be blank")
	}

	// Only create the queue if it doesn't already exist.
	if q, ok := m.queues[name]; ok {
		// It exists and we don't need to create it.
		return q, nil
	}

	queue, err := createQueue(m.db, name)
	if err != nil {
		return nil, err
	}

	// Save it in memory
	m.addQueue(queue)

	return queue, nil
}

// Add the queue in memory.
// Should be called with lock acquired.
func (m *Manager) addQueue(q *Queue) {
	m.queues[q.name] = q
}

// Queues returns a snapshot of the queues from the time it's called.
func (m *Manager) Queues() []*Queue {
	m.mu.RLock()
	defer m.mu.RUnlock()

	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	return qs
}

func (m *Manager) Close() {
	close(m.quit)
	<-m.done
}

func (m *Manager) GetQueue(name string) (*Queue, bool) {
	m.mu.RLock()
	q, ok := m.queues[name]
	m.mu.RUnlock()
	return q, ok
}
