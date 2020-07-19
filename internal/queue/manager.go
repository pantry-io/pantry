package queue

import (
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
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
	wg.Add(1)
	go func() {
		wg.Wait()
		close(m.done)
	}()

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(m.checkQueueStatesInterval)
		for {
			select {
			case <-ticker.C:
				m.checkQueueStates()
			case <-m.quit:
				ticker.Stop()
				return
			}
		}
	}()
}

// Check all the queue states to make sure we have not missed any.
func (m *Manager) checkQueueStates() {
	log.Debug().Msg("checking queue states")
}

func (m *Manager) UpsertQueueState(qk QueueKey) error {
	m.mu.RLock()
	if _, ok := m.queues[qk.Name]; !ok {
		m.mu.RUnlock()
		if _, err := m.CreateQueue(qk); err != nil {
			return err
		}
		return nil
	}
	m.mu.RUnlock()
	return nil
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
			q.SetName(qk.Name)
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

func (m *Manager) CreateQueue(qk QueueKey) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Only create the queue if it doesn't already exist.
	if q, ok := m.queues[qk.Name]; ok {
		// It exists and we don't need to create it.
		return q, nil
	}

	queue, err := createQueue(m.db, qk.Name)
	if err != nil {
		return nil, err
	}

	// Save it in memory
	m.queues[qk.Name] = queue

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
