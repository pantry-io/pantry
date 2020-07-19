package republisher

import (
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/flatbuf"
	"github.com/nickpoorman/nats-requeue/internal/queue"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/ksuid"
)

const DefaultACKTimeout = 15 * time.Second

type Republisher struct {
	db       *badger.DB
	qManager *queue.Manager
	nc       *nats.Conn

	mu sync.RWMutex

	quit chan struct{}
	done chan struct{}
}

func New(nc *nats.Conn, db *badger.DB, qManager *queue.Manager, interval time.Duration) *Republisher {
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
			// TODO: Cancel any thats currently trying to republish but hasn't sent the Request yet.
			close(rp.done)
			return
		}
	}
}

// republish check all the queues for new messages
// that are ready to be sent and trigger a publish for any that are.
func (rp *Republisher) republish() {
	log.Debug().Msg("republisher triggered.")
	rp.mu.Lock()
	defer rp.mu.Unlock()

	qs := rp.qManager.Queues()

	if len(qs) == 0 {
		// If there are no queues then there is nothing for us to do.
		return
	}

	// TODO: Rework this so that this output chan has been prioritized based on
	// queue.
	// TODO: Close this chan when we are done with this function
	ch := make(chan queue.QueueItem)
	// defer func(){
	// 	close(ch)
	// }()

	var wg sync.WaitGroup
	wg.Add(len(qs))

	go func() {
		wg.Wait()
		close(ch)
	}()

	now := time.Now() // Read up until now
	for _, que := range qs {
		go func(q *queue.Queue) {
			defer wg.Done()

			checkpoint, err := q.ReadFromCheckpoint(now, func(qi queue.QueueItem) bool {
				// TODO(nickpoorman): This is blocking the transaction from
				// being able to close. If this becomes an issue,
				// may need to buffer these up.
				ch <- qi
				return true
			})
			if err != nil {
				log.Err(err).Msg("called to ReadFromCheckpoint failed")
			}
			// Update the checkpoint
			if err := q.UpdateCheckpoint(checkpoint); err != nil {
				log.Err(err).Msg("call to UpdateCheckpoint failed")
			}
		}(que)
	}

	for qi := range ch {
		fb := flatbuf.GetRootAsRequeueMessage(qi.V, 0)
		if qi.IsExpired() {
			// Don't send a message with an expired TTL.
			// TTL will take care of removing the message from disk for us.
			continue
		}

		log.Debug().
			Str("msg", string(fb.OriginalPayloadBytes())).
			Msg("republishing message")

		subj := string(fb.OriginalSubject())
		data := fb.OriginalPayloadBytes()

		_, err := rp.nc.Request(subj, data, DefaultACKTimeout) // TODO: Make this timeout configurable.
		if err != nil {
			log.Err(err).
				Str("msg", string(fb.OriginalPayloadBytes())).
				Msg("error doing Request for message")

			// We just spent a retry.
			// So if retires == 1 it will now be zero and we should throw away the message.
			// If retires > 1 then there are retries still left to be spent.
			if fb.Retries() > 1 {
				// Requeue the message to disk for a future time.
				rp.requeueMessageToDisk(qi, fb)
				continue
			}
		}
		// Got the ACK or ran out of retries.
		// Remove the message from disk.
		rp.removeMessageFromDisk(qi, fb)
	}
}

// TODO: Requeue the message to disk for a future time.
func (rp *Republisher) requeueMessageToDisk(qi queue.QueueItem, fb *flatbuf.RequeueMessage) {
	// TODO: If this process were to shut off before we requeue to disk,
	// we could end up with data on disk that is zombied and won't be picked up
	// because of our checkpoint. To solve this, we can have another goroutine in the background
	// that infrequeuently checks for messages that are not marked as deleted, but are before our checkpoint.

	entry, err := rp.createEntry(qi, fb)
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
		err = txn.Delete(qi.K)
		if err != nil {
			return err
		}

		return nil
	})
	log.Err(err).
		Str("msg", string(fb.OriginalPayloadBytes())).
		Msg("error removing message from disk")
}

func (rp *Republisher) removeMessageFromDisk(qi queue.QueueItem, fb *flatbuf.RequeueMessage) {
	err := rp.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(qi.K)
	})
	log.Err(err).
		Str("msg", string(fb.OriginalPayloadBytes())).
		Msg("error removing message from disk")
}

func (rp *Republisher) createEntry(qi queue.QueueItem, fb *flatbuf.RequeueMessage) (*badger.Entry, error) {
	// TODO: We need to change the delay based on the BackoffStrategy.
	// for now we'll just do fixed backoff.
	delay := time.Now().Add(time.Duration(fb.Delay()))
	kuid, err := ksuid.NewRandomWithTime(delay)
	if err != nil {
		log.Err(err).Msg("problem creating a new ksuid")
		return nil, err
	}
	qk := queue.NewQueueKeyForMessage(string(fb.PersistenceQueue()), kuid.String())

	// Update the message with the new retry count, ttl, etc.
	if err := adjMsgBeforeRequeueToDisk(qi, fb); err != nil {
		return nil, err
	}

	return badger.NewEntry(qk.Bytes(), qi.V).WithTTL(time.Duration(fb.Ttl())), nil
}

func adjMsgBeforeRequeueToDisk(qi queue.QueueItem, fb *flatbuf.RequeueMessage) error {
	// Because we just retried, subtract 1 from the number of retries left.
	retries := fb.Retries()
	if retries <= 1 {
		panic("retries should be checked before getting to this point")
	}
	ok := fb.MutateRetries(retries - 1)
	if !ok {
		return fmt.Errorf("unable to mutate retries on RequeueMessage flatbuffer to: %d", retries-1)
	}

	// We don't want to write the message back to disk with the same retry it
	// had before. So this time we update the ttl that is left if there is one.
	ttl := fb.Ttl()
	if ttl != 0 {
		newTTL := uint64(qi.DurationUntilExpires())
		ok := fb.MutateTtl(newTTL)
		if !ok {
			return fmt.Errorf("unable to mutate ttl on RequeueMessage flatbuffer from: %d to: %d", ttl, newTTL)
		}
	}
	return nil
}
