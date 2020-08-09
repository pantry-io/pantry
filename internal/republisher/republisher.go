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
	"github.com/nickpoorman/nats-requeue/internal/ticker"
	"github.com/nickpoorman/nats-requeue/protocol"
	"github.com/rs/zerolog/log"
)

const (
	// On this interval, the queues will be scanned for messages
	// that are ready to be published.
	DefaultACKTimeout = 15 * time.Second

	// The amount of time a request (publish) will wait to be acknowledged
	// by the reviever. In the event an acknowledgement it not received,
	// the message will be placed back into the queue.
	DefaultRepublisherInterval = 15 * time.Second

	// On this interval, the queues will be scanned for any messages that might
	// exist before our current checkpoint. If any such messages are found, the
	// checkpoint will be updated to the found message key.
	DefaultCheckpointCorrectionInterval = 60 * time.Second

	// The concurrent limit for messages in flight waiting for a response. When
	// set to -1 there is no limit. A limit should be set in production
	// environments to avoid overloading the consumers.
	DefaultMaxInFlight = -1
)

// Options can be used to set custom options for a Republisher.
type Options struct {
	// On this interval, the queue will be scanned for messages
	// that are ready to be published.
	pubInterval time.Duration

	// The amount of time a request (publish) will wait to be acknowledged
	// by the reviever. In the event an acknowledgement it not received,
	// the message will be placed back into the queue.
	ackTimeout time.Duration

	// On this interval, the queues will be scanned for any messages that might
	// exist before our current checkpoint. If any such messages are found, the
	// checkpoint will be updated to the found message key.
	checkpointCorrectionInterval time.Duration

	// The concurrent limit for messages in flight waiting for a response. When
	// set to -1 there is no limit. A limit should be set in production
	// environments to avoid overloading the consumers.
	maxInFlight int
}

func GetDefaultOptions() Options {
	return Options{
		pubInterval:                  DefaultRepublisherInterval,
		ackTimeout:                   DefaultACKTimeout,
		checkpointCorrectionInterval: DefaultCheckpointCorrectionInterval,
		maxInFlight:                  DefaultMaxInFlight,
	}
}

// Option is a function on the options for a Republisher.
type Option func(*Options) error

// On this interval, the queue will be scanned for messages
// that are ready to be published.
func RepublishInterval(interval time.Duration) Option {
	return func(o *Options) error {
		o.pubInterval = interval
		return nil
	}
}

// The amount of time a request (publish) will wait to be acknowledged by the
// reviever. In the event an acknowledgement it not received, the message will
// be placed back into the queue.
func AckTimeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.ackTimeout = timeout
		return nil
	}
}

// On this interval, the queues will be scanned for any messages that might
// exist before our current checkpoint. If any such messages are found, the
// checkpoint will be updated to the found message key.
func CheckpointCorrectionInterval(interval time.Duration) Option {
	return func(o *Options) error {
		o.checkpointCorrectionInterval = interval
		return nil
	}
}

// The concurrent limit for messages in flight waiting for a response. When set
// to -1 there is no limit. A limit should be set in production environments to
// avoid overloading the consumers.
func MaxInFlight(concurrent int) Option {
	return func(o *Options) error {
		o.maxInFlight = concurrent
		return nil
	}
}

type Republisher struct {
	db       *badger.DB
	qManager *queue.Manager
	nc       *nats.Conn

	opts Options

	mu sync.RWMutex

	quit chan struct{}
	done chan struct{}
}

func New(nc *nats.Conn, db *badger.DB, qManager *queue.Manager, options ...Option) (*Republisher, error) {
	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	rq := &Republisher{
		db:       db,
		nc:       nc,
		qManager: qManager,
		opts:     opts,
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go rq.initBackgroundTasks()

	return rq, nil
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
		t := ticker.New(rp.opts.pubInterval)
		go func() {
			<-rp.quit
			t.Stop()
		}()
		t.Loop(func() bool {
			rp.republish()
			return true
		})
	}()

	// Our checkpoint is optimistic. To solve the checkpoint getting ahead of
	// our data, we have a task that runs every now and then checks for messages
	// that are not marked as deleted, but are before our checkpoint. If we find
	// any messages before our checkpoint we will reset the checkpoint.
	go func() {
		defer wg.Done()
		t := ticker.New(rp.opts.checkpointCorrectionInterval)
		go func() {
			<-rp.quit
			t.Stop()
		}()
		t.Loop(func() bool {
			rp.correctCheckpoint()
			return true
		})
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

	writeCh := make(chan queue.QueueItem)
	var wg sync.WaitGroup
	wg.Add(len(qs))

	go func() {
		wg.Wait()
		close(writeCh)
	}()

	untilTime := time.Now() // Read up until now.
	for _, que := range qs {
		go func(q *queue.Queue) {
			defer wg.Done()
			rp.processQueue(q, writeCh, untilTime)
		}(que)
	}

	// Based on our max in flight limit, create workers to publish messages and
	// wait for acknowledgements.
	concurrency := rp.opts.maxInFlight
	var pubWg sync.WaitGroup
	readCh := make(chan queue.QueueItem)
	var running int
	for qi := range writeCh {
		select {
		// If there is a worker waiting for a message then use it.
		case readCh <- qi:
			continue
		default:
			if concurrency == -1 || running < concurrency {
				// No workers available, so create a new one.
				running++
				log.Debug().Msgf("republisher: republish: spinning up new worker: %d", running)

				pubWg.Add(1)
				go func() {
					defer pubWg.Done()
					rp.publishMessages(readCh)
				}()
			}
			readCh <- qi
		}
	}
	close(readCh)
	pubWg.Wait()
}

func (rp *Republisher) processQueue(q *queue.Queue, ch chan<- queue.QueueItem, untilTime time.Time) {
	log.Debug().Msgf("republisher: republish: processing queue: %s", q.Name())
	checkpoint, err := q.ReadFromCheckpoint(untilTime, func(qi queue.QueueItem) bool {
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
}

func (rp *Republisher) publishMessages(ch <-chan queue.QueueItem) {
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
	// If this process were to shut off before we requeue to disk, we could end
	// up with data on disk that is zombied and won't be picked up because of
	// our checkpoint. To solve this, we have another goroutine in the
	// background that infrequeuently checks for messages that are not marked as
	// deleted, but are before our checkpoint.

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
