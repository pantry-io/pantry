package queue

import (
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nickpoorman/nats-requeue/internal/ticker"
	"github.com/rs/zerolog/log"
)

const (
	statsRefreshInterval = 60 * time.Second
)

type queueStats struct {
	quit   chan struct{}
	doneWg sync.WaitGroup

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

func newQueueStats(db *badger.DB, queueName string) *queueStats {
	qs := &queueStats{
		quit:      make(chan struct{}),
		db:        db,
		queueName: queueName,
	}
	var wg sync.WaitGroup
	wg.Add(1)

	go qs.initBackgroundTasks()
	return qs
}

func (qs *queueStats) initBackgroundTasks() {
	qs.doneWg.Add(1)

	// Stats refresh
	go func() {
		defer qs.doneWg.Done()
		t := ticker.New(statsRefreshInterval)
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

// Close will stop the queueStats background tasks.
func (qs *queueStats) Close() {
	close(qs.quit)
	qs.doneWg.Wait()
}

func (qs *queueStats) SetName(name string) {
	qs.mu.Lock()
	qs.queueName = name
	qs.mu.Unlock()
}

func (qs *queueStats) AddCount(num int64) {
	atomic.AddInt64(&qs.count, num)
}

func (qs *queueStats) AddInFlight(num int64) {
	atomic.AddInt64(&qs.inFlight, num)
}

func (qs *queueStats) refreshStats() error {
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

	// Update the count
	atomic.StoreInt64(&qs.count, count)

	return err
}

func (qs *queueStats) ToMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["count"] = atomic.LoadInt64(&qs.count)
	m["inFlight"] = atomic.LoadInt64(&qs.inFlight)

	// We can end up with a negative count due to this being eventually
	// consistent.
	if m["count"].(int64) < 0 {
		m["count"] = 0
	}

	return m
}
