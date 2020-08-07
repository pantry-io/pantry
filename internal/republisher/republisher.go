package republisher

import (
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/flatbuf"
	"github.com/nickpoorman/nats-requeue/internal/key"
	"github.com/nickpoorman/nats-requeue/internal/queue"
	"github.com/nickpoorman/nats-requeue/protocol"
	"github.com/rs/zerolog/log"
)

const DefaultACKTimeout = 15 * time.Second

var CheckpointCorrectionInterval = 60 * time.Second

type Republisher struct {
	db       *badger.DB
	qManager *queue.Manager
	nc       *nats.Conn

	// The publish interval
	pubInterval time.Duration

	mu sync.RWMutex

	quit chan struct{}
	done chan struct{}
}

func New(nc *nats.Conn, db *badger.DB, qManager *queue.Manager, pubInterval time.Duration) *Republisher {
	rq := &Republisher{
		db:          db,
		nc:          nc,
		qManager:    qManager,
		pubInterval: pubInterval,
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	go rq.initBackgroundTasks()

	return rq
}

func (rp *Republisher) initBackgroundTasks() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		wg.Wait()
		close(rp.done)
	}()

	// republish loop
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(rp.pubInterval)
		for {
			select {
			case <-ticker.C:
				rp.republish()
			case <-rp.quit:
				ticker.Stop()
				return
			}
		}
	}()

	// Our checkpoint is optimistic. To solve the checkpoint getting ahead of
	// our data, we have a task that runs every now and then checks for messages
	// that are not marked as deleted, but are before our checkpoint. If we find
	// any messages before our checkpoint we will reset the checkpoint.
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(CheckpointCorrectionInterval)
		for {
			select {
			case <-ticker.C:
				rp.correctCheckpoint()
			case <-rp.quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (rp *Republisher) Close() {
	close(rp.quit)
	<-rp.done
}

// republish check all the queues for new messages
// that are ready to be sent and trigger a publish for any that are.
// This is called in a loop on an interval.
func (rp *Republisher) republish() {
	log.Debug().Msg("republisher: republish: triggered.")
	rp.mu.Lock()
	defer rp.mu.Unlock()

	qs := rp.qManager.Queues()
	log.Debug().Msgf("republisher: republish: number of queues to process: %d", len(qs))

	if len(qs) == 0 {
		// If there are no queues then there is nothing for us to do.
		return
	}

	ch := make(chan queue.QueueItem)
	var wg sync.WaitGroup
	wg.Add(len(qs))

	go func() {
		wg.Wait()
		close(ch)
	}()

	maxTimeToRead := time.Now() // Read up until now.
	for _, que := range qs {
		go func(q *queue.Queue) {
			defer wg.Done()
			log.Debug().Msgf("republisher: republish: processing queue: %s", q.Name())
			checkpoint, err := q.ReadFromCheckpoint(maxTimeToRead, func(qi queue.QueueItem) bool {
				// Note: This function is blocking the Badger transaction from closing.
				select {
				// If rp.quit is closed, we stop this early and checkpoint where we're at.
				case <-rp.quit:
					return false
				case ch <- qi:
					log.Debug().Msg("republisher: republish: processing queue item")
					return true
				}
			})
			if err != nil {
				log.Err(err).Msg("call to ReadFromCheckpoint failed")
				return
			}
			log.Debug().Msgf("republisher: republish: processed queue: %s. Updating checkpoint: %s", q.Name(), checkpoint.String())
			// Update the checkpoint
			if err := q.UpdateCheckpoint(checkpoint); err != nil {
				log.Err(err).Msg("call to UpdateCheckpoint failed")
				return
			}
		}(que)
	}

	// TODO: This is doing 1 request at a time. We may want to bump this up with go routines + a limiter.
	for qi := range ch {
		fb := flatbuf.GetRootAsRequeueMessage(qi.V, 0)

		log.Debug().
			Str("msg", string(fb.OriginalPayloadBytes())).
			Msg("republishing message")

		if qi.IsExpired() {
			// Don't send a message with an expired TTL.
			// TTL will take care of removing the message from disk for us.
			log.Debug().
				Str("msg", string(fb.OriginalPayloadBytes())).
				Msg("message is expired")
			continue
		}

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

// Requeue the message to disk for a future time.
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
	if err != nil {
		return
	}
}

func (rp *Republisher) removeMessageFromDisk(qi queue.QueueItem, fb *flatbuf.RequeueMessage) {
	err := rp.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(qi.K)
	})
	if err != nil {
		log.Err(err).
			Str("msg", string(fb.OriginalPayloadBytes())).
			Msg("error removing message from disk")
	}
}

func (rp *Republisher) createEntry(qi queue.QueueItem, fb *flatbuf.RequeueMessage) (*badger.Entry, error) {
	// TODO: We need to change the delay based on the BackoffStrategy.
	// for now we'll just do fixed backoff.
	delay := time.Now().Add(time.Duration(fb.Delay()))
	qk := queue.NewQueueKeyForMessage(protocol.GetQueueName(fb), key.New(delay))

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

// TODO: Write a test for this.
func (rp *Republisher) correctCheckpoint() {
	// Grab the earliest checkpoint.
	rp.mu.Lock()
	defer rp.mu.Unlock()

	qs := rp.qManager.Queues()
	log.Debug().Msgf("republisher: correctCheckpoint: number of queues to process: %d", len(qs))

	if len(qs) == 0 {
		// If there are no queues then there is nothing for us to do.
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(qs))

	untilNow := time.Now()

	for _, que := range qs {
		go func(q *queue.Queue) {
			defer wg.Done()
			log.Debug().Msgf("republisher: correctCheckpoint: processing queue: %s", q.Name())
			checkpoint, err := q.EarliestCheckpoint(untilNow)
			if err != nil {
				log.Err(err).Msg("call to EarliestCheckpoint failed")
				return
			}

			log.Debug().Msgf("republisher: correctCheckpoint: processed queue: %s. Updating checkpoint: %s", q.Name(), checkpoint.String())
			// Update the checkpoint
			if err := q.UpdateCheckpointCond(checkpoint, func(cp queue.Checkpoint) bool {
				return q.CompareCheckpoint(checkpoint) == 1
			}); err != nil {
				log.Err(err).Msg("call to UpdateCheckpoint failed")
				return
			}
		}(que)
	}

	wg.Wait()
}
