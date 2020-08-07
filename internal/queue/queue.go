package queue

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nickpoorman/nats-requeue/internal/key"
	"github.com/rs/zerolog/log"
)

type Checkpoint []byte

func (c Checkpoint) String() string {
	return string(c)
}

type Queue struct {
	db *badger.DB

	mu         sync.RWMutex
	name       string
	checkpoint Checkpoint
}

func createQueue(db *badger.DB, name string) (*Queue, error) {
	// Create the queue and persist it.
	q := &Queue{
		db:         db,
		name:       name,
		checkpoint: FirstMessage(name).Bytes(), // set to the min possible value
	}

	// Save the queue state to disk
	err := q.db.Update(func(txn *badger.Txn) error {
		// Save the checkpoint for the queue
		if err := txn.Set(
			NewQueueKeyForState(q.name, CheckpointProperty).Bytes(),
			q.checkpoint,
		); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return q, err
}

func (q *Queue) Name() string {
	return q.name
}

// UpdateCheckpoint will update the checkpoint for this queue.
func (q *Queue) UpdateCheckpoint(checkpoint Checkpoint) error {
	q.mu.Lock()
	defer q.mu.Unlock()

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

func (q *Queue) SetName(name string) {
	q.name = name
}

type QueueItem struct {
	// K is the key of the item.
	K []byte

	// V is the value of the item.
	V []byte

	// ExpiresAt is a Unix time, the number of seconds elapsed
	// since January 1, 1970 UTC.
	ExpiresAt uint64
}

// IsExpired returns true if this item has expired.
func (qi QueueItem) IsExpired() bool {
	return qi.ExpiresAt <= uint64(time.Now().Unix())
}

// ExpiresAtTime returns the Time this item will expire.
func (qi QueueItem) ExpiresAtTime() time.Time {
	return time.Unix(int64(qi.ExpiresAt), 0)
}

// DurationUntilExpires returns a duration indicating how much time until this item
// expires.
func (qi QueueItem) DurationUntilExpires() time.Duration {
	return time.Until(qi.ExpiresAtTime())
}

// Range performs a range query against the storage. It calls f sequentially for
// each key and value present in the store. If f returns false, range stops the
// iteration. The implementation must guarantee that the keys are
// lexigraphically sorted.
// The checkpoint returned will either be the original seek passed to this
// function or the last successfully processed key. If f returns false, the key
// for that iterantion will not be the checkpoint.
func (q *Queue) Range(seek, until QueueKey, f func(QueueItem) bool) (Checkpoint, error) {
	checkpoint := seek.Bytes()
	err := q.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = PrefixOf(seek.Bytes(), until.Bytes())
		it := tx.NewIterator(opts)
		defer it.Close()

		log.Debug().
			Str("seek", string(seek.Bytes())).
			Str("until", string(until.Bytes())).
			Str("prefix", string(opts.Prefix)).
			Msg("Queue: Range: starting iterator")

		// Seek the prefix and check the key so we can quickly exit the iteration.
		for it.Seek(seek.Bytes()); it.Valid(); it.Next() {
			item := it.Item()
			log.Debug().
				Str("seek", string(seek.Bytes())).
				Str("until", string(until.Bytes())).
				Str("prefix", string(opts.Prefix)).
				Str("item.Key", string(item.Key())).
				Msg("Queue: Range: iterator: got item")

			if item.IsDeletedOrExpired() { // Not sure if this is necessary.
				log.Debug().
					Str("seek", string(seek.Bytes())).
					Str("until", string(until.Bytes())).
					Str("prefix", string(opts.Prefix)).
					Str("item.Key", string(item.Key())).
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
					Str("seek", string(seek.Bytes())).
					Str("until", string(until.Bytes())).
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
		Msg("Queue: ReadFromCheckpoint: calling range")

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
