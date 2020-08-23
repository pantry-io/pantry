package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nickpoorman/nats-requeue/internal/ticker"
	"github.com/nickpoorman/nats-requeue/protocol"
	"github.com/rs/zerolog/log"
)

const (
	DefaultStatsRefreshInterval = 60 * time.Second
)

type QueueStatsOptions struct {
	refreshInterval time.Duration
}

func QueueStatsOptionsDefault() QueueStatsOptions {
	return QueueStatsOptions{
		refreshInterval: DefaultStatsRefreshInterval,
	}
}

// QueueStatsOption is a function on the options for Reaper.
type QueueStatsOption func(*QueueStatsOptions) error

// ReapInterval sets the interval in which to check for zombied instances.
func ReapInterval(refreshInterval time.Duration) QueueStatsOption {
	return func(o *QueueStatsOptions) error {
		o.refreshInterval = refreshInterval
		return nil
	}
}

type QueueStats struct {
	quit   chan struct{}
	doneWg sync.WaitGroup
	opts   QueueStatsOptions

	db *badger.DB

	mu        sync.RWMutex
	queueName string

	// Messages can expire from the TTL.
	// Due to this we will need to rebuild the stats periodically.
	// This count is eventually consistent.
	count int64

	// This should always be consistent.
	inFlight int64
}

func NewQueueStats(db *badger.DB, queueName string, options ...QueueStatsOption) (*QueueStats, error) {
	if queueName == "" {
		return nil, fmt.Errorf("queue name cannot be empty")
	}

	opts := QueueStatsOptionsDefault()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	qs := &QueueStats{
		quit:      make(chan struct{}),
		opts:      opts,
		db:        db,
		queueName: queueName,
	}

	go func() { _ = qs.refreshStats() }() // Refresh stats now.
	go qs.initBackgroundTasks()
	return qs, nil
}

func (qs *QueueStats) initBackgroundTasks() {
	qs.mu.Lock()
	defer qs.mu.Unlock()

	qs.doneWg.Add(1)

	// Stats refresh
	go func() {
		defer qs.doneWg.Done()
		t := ticker.New(qs.opts.refreshInterval)
		go func() {
			<-qs.quit
			t.Stop()
		}()
		t.Loop(func() bool {
			_ = qs.refreshStats()
			return true
		})
	}()

}

// Close will stop the QueueStats background tasks.
func (qs *QueueStats) Close() {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	close(qs.quit)
	qs.doneWg.Wait()
}

func (qs *QueueStats) AddCount(num int64) {
	atomic.AddInt64(&qs.count, num)
}

func (qs *QueueStats) AddInFlight(num int64) {
	atomic.AddInt64(&qs.inFlight, num)
}

func (qs *QueueStats) refreshStats() error {
	// Lock so that we don't ever end up running two refreshes at once for this
	// queue.
	qs.mu.Lock()
	defer qs.mu.Unlock()

	name := qs.queueName
	seek := FirstMessage(name)
	until := LastMessage(name)
	prefix := PrefixOf(seek.Bytes(), until.Bytes())

	var count int64

	err := qs.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := tx.NewIterator(opts)
		defer it.Close()

		log.Debug().
			Str("seek", seek.String()).
			Str("until", until.String()).
			Bytes("prefix", opts.Prefix).
			Msg("Queue: Range: starting iterator")

		for it.Seek(seek.Bytes()); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() { // Do we need this?
				continue
			}
			count++
		}
		return nil
	})
	if err != nil {
		return err
	}

	log.Debug().Msgf("storing new stats count: %d", count)

	// Update the count
	atomic.StoreInt64(&qs.count, count)

	return err
}

func (qs *QueueStats) QueueStatsMessage() protocol.QueueStatsMessage {
	qs.mu.RLock()
	defer qs.mu.RUnlock()

	enqueued := qs.count
	if enqueued < 0 {
		enqueued = 0
	}
	return protocol.QueueStatsMessage{
		QueueName: qs.queueName,
		Enqueued:  enqueued,
		InFlight:  qs.inFlight,
	}
}
