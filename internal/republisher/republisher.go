package requeuer

import (
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/nickpoorman/nats-requeue/internal/queues"
)

type Republisher struct {
	db       *badger.DB
	qManager *queues.Manager

	mu sync.RWMutex

	quit chan struct{}
	done chan struct{}
}

func NewRepublisher(db *badger.DB, qManager *queues.Manager, interval time.Duration) *Republisher {
	rq := &Republisher{
		db:       db,
		qManager: qManager,
	}
	go rq.loop(interval)

	return rq
}

// loop will on the interval provided call republish until this
// Republisher is told to stop.
func (rp *Republisher) loop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			rp.republish()
		case <-rp.quit:
			ticker.Stop()
			close(rp.done)
			return
		}
	}
}

// republish check all the queues for new messages
// that are ready to be sent and trigger a publish for any that are.
func (rp *Republisher) republish() {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	queues := rp.qManager.Queues()
	for _, q := range queues {
		q.ReadFromCheckpoint()
	}
}
