package queue

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	badgerInternal "github.com/nickpoorman/nats-requeue/internal/badger"
	"github.com/nickpoorman/nats-requeue/internal/debug"
	"github.com/nickpoorman/nats-requeue/internal/key"
	"github.com/rs/zerolog/log"
)

// TODO: We should remove this and use QueueKey or at lease rename this to QueueKeyRaw
type Checkpoint []byte

func (c Checkpoint) String() string {
	if len(c) == 0 {
		return "EMPTY_CHECKPOINT"
	}
	return ParseQueueKey(c).String()
}

func (c Checkpoint) Bytes() []byte {
	return c
}

func (c Checkpoint) Key() key.Key {
	return ParseQueueKey(c).Key
}

type Queue struct {
	quit        chan struct{}
	done        chan struct{}
	db          *badger.DB
	batchWriter *badgerInternal.BatchedWriter

	mu         sync.RWMutex
	name       string
	checkpoint Checkpoint
	Stats      *QueueStats
}

func NewQueue(db *badger.DB, name string) (*Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("new queue: queue name cannot be empty")
	}

	qStats, err := NewQueueStats(db, name)
	if err != nil {
		return nil, fmt.Errorf("new queue: %w", err)
	}

	q := &Queue{
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
		db:          db,
		batchWriter: badgerInternal.NewBatchedWriter(db, 15*time.Millisecond),
		name:        name,
		checkpoint:  FirstMessage(name).Bytes(), // set to the min possible value
		Stats:       qStats,
	}

	go func() {
		<-q.quit
		q.mu.Lock()
		// Close batch writer.
		if q.batchWriter != nil {
			q.batchWriter.Close()
		}
		// Close queue stats.
		if q.Stats != nil {
			q.Stats.Close()
		}
		q.mu.Unlock()
		close(q.done)
	}()
	return q, nil
}

// TODO: Combine this with NewQueue
func createQueue(db *badger.DB, name string) (*Queue, error) {
	// Create the queue and persist it.
	q, err := NewQueue(db, name)
	if err != nil {
		return nil, fmt.Errorf("create queue: %w", err)
	}
	q.checkpoint = FirstMessage(name).Bytes() // set to the min possible value

	// Save the queue state to disk
	if err := q.db.Update(func(txn *badger.Txn) error {
		// Save the checkpoint for the queue
		if err := txn.Set(
			NewQueueKeyForState(q.name, CheckpointProperty).Bytes(),
			q.checkpoint,
		); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return q, nil
}

// Close will stop the queue background tasks.
func (q *Queue) Close() {
	close(q.quit)
	<-q.done
}

func (q *Queue) Name() string {
	return q.name
}

// CompareCheckpoint will compare the passed checkpoint to the existign for the
// queue.
// The result will be 0 if q==b, -1 if q < b, and +1 if q > b.
func (q *Queue) CompareCheckpoint(b Checkpoint) int {
	return bytes.Compare(q.checkpoint, b)
}

// UpdateCheckpoint will update the checkpoint for this queue.
func (q *Queue) UpdateCheckpoint(checkpoint Checkpoint) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.updateCheckpoint(checkpoint)
}

// UpdateCheckpointCond will update the checkpoint if the passed in cond
// callback returns true.
func (q *Queue) UpdateCheckpointCond(checkpoint Checkpoint, cond func(Checkpoint) bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if cond != nil {
		if !cond(q.checkpoint) {
			return nil
		}
	}
	return q.updateCheckpoint(checkpoint)
}

// This must be called with a lock acquired.
func (q *Queue) updateCheckpoint(checkpoint Checkpoint) error {
	debug.Assert(len(checkpoint) != 0, "trying to set an empty checkpoint")
	if err := q.saveCheckpoint(checkpoint); err != nil {
		return err
	}
	q.setCheckpoint(checkpoint)
	return nil
}

// setCheckpoint will set the checkpoint in memory.
// Should be called with lock acquired.
func (q *Queue) setCheckpoint(checkpoint Checkpoint) {
	// Update it in memory.
	q.checkpoint = checkpoint
}

// saveCheckpoint will save the checkpoint on disk.
// Should be called with lock acquired.
func (q *Queue) saveCheckpoint(checkpoint Checkpoint) error {
	// Save it to disk.
	return q.db.Update(func(txn *badger.Txn) error {
		return txn.Set(
			NewQueueKeyForState(q.name, CheckpointProperty).Bytes(),
			checkpoint,
		)
	})
}

func (q *Queue) SetKV(qk QueueKey, v []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	switch qk.PropertyString() {
	case CheckpointProperty: // queues.high.checkpoint
		q.checkpoint = v
	default:
		err := fmt.Errorf("queue: SetKV: unknown property: %s", string(qk.Property))
		log.Debug().Msgf(err.Error())
		return err
	}
	return nil
}

// Range performs a range query against the storage. It calls f sequentially for
// each key and value present in the store. If f returns false, range stops the
// iteration. The implementation must guarantee that the keys are
// lexigraphically sorted.
// The checkpoint returned will either be the original seek passed to this
// function or the last successfully processed key. If f returns false, the key
// for that iteration will not be the checkpoint.
func (q *Queue) Range(seek, until QueueKey, f func(QueueItem) bool) (Checkpoint, error) {
	checkpoint := seek.Bytes()
	err := q.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = PrefixOf(seek.Bytes(), until.Bytes())
		it := tx.NewIterator(opts)
		defer it.Close()

		log.Debug().
			Str("seek", seek.String()).
			Str("until", until.String()).
			Bytes("prefix", opts.Prefix).
			Msg("Queue: Range: starting iterator")

		// Seek the prefix and check the key so we can quickly exit the iteration.
		for it.Seek(seek.Bytes()); it.Valid(); it.Next() {
			item := it.Item()
			log.Debug().
				Str("seek", seek.String()).
				Str("until", until.String()).
				Bytes("prefix", opts.Prefix).
				Str("item.Key", ParseQueueKey(item.Key()).String()).
				Msg("Queue: Range: iterator: got item")

			if item.IsDeletedOrExpired() { // Not sure if this is necessary.
				log.Debug().
					Str("seek", seek.String()).
					Str("until", until.String()).
					Bytes("prefix", opts.Prefix).
					Str("item.Key", ParseQueueKey(item.Key()).String()).
					Msg("Queue: Range: iterator: item is expired")
				continue
			}

			key := item.KeyCopy(nil)
			if bytes.Compare(key, until.Bytes()) > 0 {
				return nil // Stop if we've reached the end
			}

			// Fetch the value
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !f(QueueItem{K: key, V: value, ExpiresAt: item.ExpiresAt()}) {
				log.Debug().
					Str("seek", seek.String()).
					Str("until", until.String()).
					Str("prefix", string(opts.Prefix)).
					Msg("Queue: Range: callback returned false. Stopping range.")
				return nil
			}
			checkpoint = key
		}
		return nil
	})
	return checkpoint, err
}

// ReadFromCheckpoint should begin reading in all the events from the checkpoint
// up until the provided Time.
func (q *Queue) ReadFromCheckpoint(until time.Time, f func(QueueItem) bool) (Checkpoint, error) {
	q.mu.RLock()
	name := q.name
	checkpoint := q.checkpoint
	q.mu.RUnlock()

	debug.Assert(len(checkpoint) != 0, fmt.Sprintf("checkpoint has not been initialized for queue: %s", name))

	untilQK := NewQueueKeyForMessage(name, key.New(until))
	log.Debug().
		Str("queue", name).
		Str("checkpoint", checkpoint.String()).
		Msg("Queue: ReadFromCheckpoint: calling range")
	return q.Range(ParseQueueKey(checkpoint), untilQK, f)
}

// EarliestCheckpoint will return the earliest Checkpoint up until the specified time.
// It does this by looking for the earliest message that has not been deleted.
func (q *Queue) EarliestCheckpoint(until time.Time) (Checkpoint, error) {
	q.mu.RLock()
	name := q.name
	checkpoint := q.checkpoint
	q.mu.RUnlock()

	untilQK := NewQueueKeyForMessage(name, key.New(until))
	log.Debug().
		Str("queue", name).
		Str("checkpoint", checkpoint.String()).
		Msg("Queue: EarliestCheckpoint: calling range")

	first := true
	return q.Range(FirstMessage(name), untilQK, func(qi QueueItem) bool {
		// We have to do two iterations because Range returns the last
		// checkpoint that this callback returned true for.
		if first {
			first = false
			// continue
			return true
		} else {
			return false
		}
	})
}

// AddMessage will add a message to the queue and execute the callback function cb once committed.
// Any TTL less than or equal to zero will be ignored.
func (q *Queue) AddMessage(key []byte, value []byte, ttl time.Duration, cb func(error)) error {
	// Validate the key
	debug.Assert(assertMessageQueueKeyIsValid(key, q.name), "message queue key is invalid")

	entry := badger.NewEntry(key, value)
	if ttl > 0 {
		entry = entry.WithTTL(ttl)
	}
	if err := q.batchWriter.SetEntry(entry, func(e error) {
		// Update the stats.
		q.Stats.AddCount(1)
		// Exec the callback.
		if cb != nil {
			cb(e)
		}
	}); err != nil {
		err = fmt.Errorf("add message: %w", err)
		log.Err(err).Msg("problem calling SetEntry")
		return err
	}
	return nil
}
