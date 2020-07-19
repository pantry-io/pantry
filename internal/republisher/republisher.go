package republisher

import (
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/flatbuf"
	"github.com/nickpoorman/nats-requeue/internal/queues"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/ksuid"
)

const DefaultACKTimeout = 15 * time.Second

type KV struct {
	K []byte
	V []byte
}

type Republisher struct {
	db       *badger.DB
	qManager *queues.Manager
	nc       *nats.Conn

	mu sync.RWMutex

	quit chan struct{}
	done chan struct{}
}

func New(nc *nats.Conn, db *badger.DB, qManager *queues.Manager, interval time.Duration) *Republisher {
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

	// TODO: Rework this so that this output chan has been prioritized based on
	// queue.
	// TODO: Close this chan when we are done with this function
	ch := make(chan KV)
	// defer func(){
	// 	close(ch)
	// }()

	now := time.Now() // Read up until now
	for _, queue := range rp.qManager.Queues() {
		go func(q *queues.Queue) {
			err := q.ReadFromCheckpoint(now, func(key, value []byte) bool {
				ch <- KV{key, value}
				return true
			})
			if err != nil {
				log.Err(err).Msg("ReadFromCheckpoint error")
			}
		}(queue)
	}

	for kv := range ch {
		fb := flatbuf.GetRootAsRequeueMessage(kv.V, 0)
		log.Debug().
			Str("msg", string(fb.OriginalPayloadBytes())).
			Msg("republishing message")

		subj := string(fb.OriginalSubject())
		data := fb.OriginalPayloadBytes()

		_, err := rp.nc.Request(subj, data, 15*time.Second) // TODO: Make this timeout configurable.
		if err != nil {
			log.Err(err).
				Str("msg", string(fb.OriginalPayloadBytes())).
				Msg("error doing Request for message")
			// Requeue the message to disk for a future time.
			rp.requeueMessageToDisk(kv, fb)
			continue
		}
		// Got the ACK.
		// Success. Remove the message from disk.
		rp.removeMessageFromDisk(kv, fb)
	}
}

// TODO: Requeue the message to disk for a future time.
func (rp *Republisher) requeueMessageToDisk(kv KV, fb *flatbuf.RequeueMessage) {
	// TODO: If this process were to shut off before we requeue to disk,
	// we could end up with data on disk that is zombied and won't be picked up
	// because of our checkpoint. To solve this, we can have another goroutine in the background
	// that infrequeuently checks for messages that are not marked as deleted, but are before our checkpoint.

	entry, err := rp.createEntry(kv, fb)
	if err != nil {
		log.Err(err).
			Str("msg", string(fb.OriginalPayloadBytes())).
			Msg("problem creating the Entry")
	}

	err = rp.db.Update(func(txn *badger.Txn) error {
		// First insert our new entry
		err := txn.SetEntry(entry)
		if err != nil {
			log.Err(err).Msg("requeueMessageToDisk: problem calling SetEntry")
		}

		// Then delete our existing key
		err = txn.Delete(kv.K)
		if err != nil {
			return err
		}

		return nil
	})
	log.Err(err).
		Str("msg", string(fb.OriginalPayloadBytes())).
		Msg("error removing message from disk")
}

// TODO: Remove the message from disk.
func (rp *Republisher) removeMessageFromDisk(kv KV, fb *flatbuf.RequeueMessage) {
	err := rp.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(kv.K)
	})
	log.Err(err).
		Str("msg", string(fb.OriginalPayloadBytes())).
		Msg("error removing message from disk")
}

// TODO: There is probably some opportunity to factor some of this out with the
// requeue original writes since they are basically doing the same thing.
func (rp *Republisher) createEntry(kv KV, fb *flatbuf.RequeueMessage) (*badger.Entry, error) {
	meta := fb.Meta(nil)

	// TODO: We need to change the delay based on the BackoffStrategy.
	// for now we'll just do fixed backoff.
	delay := time.Now().Add(time.Duration(meta.Delay()))
	kuid, err := ksuid.NewRandomWithTime(delay)
	if err != nil {
		log.Err(err).Msg("problem creating a new ksuid")
		return nil, err
	}
	qk := queues.NewQueueKeyForMessage(string(meta.PersistenceQueue()), kuid.String())

	// TODO: Need to rebuild the value we pass here. It should have updated meta data with the retries ticking down.

	return badger.NewEntry(qk.Bytes(), kv.V).WithTTL(time.Duration(meta.Ttl())), nil
}
