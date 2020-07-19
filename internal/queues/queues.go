package queues

import (
	"bytes"
	"fmt"
	"sync"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/nickpoorman/nats-requeue/internal/key"
	"github.com/rs/zerolog/log"
)

type Queue struct {
	db *badger.DB

	mu         sync.RWMutex
	name       string
	checkpoint []byte
}

func createQueue(db *badger.DB, name string) (*Queue, error) {
	// Create the queue and persist it.
	q := &Queue{
		name:       name,
		checkpoint: key.First([]byte(name)), // set to the min possible value
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
func (q *Queue) UpdateCheckpoint(checkpoint []byte) error {
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
func (q *Queue) setCheckpoint(checkpoint []byte) {
	// Update it in memory.
	q.checkpoint = checkpoint
}

// saveCheckpoint will save the checkpoint on disk.
// Should be called with lock acquired.
func (q *Queue) saveCheckpoint(checkpoint []byte) error {
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

	switch qk.Property {
	case NameProperty:
		q.name = string(v)
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
func (q *Queue) Range(seek, until key.Key, f func(key, value []byte) bool) error {
	return q.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = key.PrefixOf(seek, until)
		it := tx.NewIterator(opts)
		defer it.Close()

		// Seek the prefix and check the key so we can quickly exit the iteration.
		for it.Seek(seek.Bytes()); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.Compare(key, until.Bytes()) > 0 {
				return nil // Stop if we're reached the end
			}

			// Fetch the value
			if value, err := item.ValueCopy(nil); err != nil && f(key, value) {
				return nil
			}
		}
		return nil
	})
}

// func (q *Queue) ReadFromCheckpoint(until time.Time) {
// 	entryKey, err := key.NewWithTime(c.persistenceQueuePrefix(persistenceQueue), delay)
// 	if err != nil {
// 		log.Err(err).Msg("problem creating a new key")
// 		if c.Opts.BadgerWriteMsgErr != nil {
// 			c.Opts.BadgerWriteMsgErr(msg, err)
// 		}
// 		return
// 	}

// 	q.Range(key.FromBytes(q.checkpoint))
// }
