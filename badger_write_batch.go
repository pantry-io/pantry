/*
 * Copyright 2020 Nick Poorman
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package requeue

import (
	"sync"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/rs/zerolog/log"
)

type WriteBatchCommitCB func(error)

var InitCommitCBsCapacity = 2000

// WriteBatch holds the necessary info to perform batched writes.
type WriteBatch struct {
	sync.Mutex
	txn       *badger.Txn
	db        *badger.DB
	throttle  *y.Throttle
	err       error
	commitCBs []WriteBatchCommitCB
}

// NewWriteBatch creates a new WriteBatch. This provides a way to conveniently do a lot of writes,
// batching them up as tightly as possible in a single transaction and using callbacks to avoid
// waiting for them to commit, thus achieving good performance. This API hides away the logic of
// creating and committing transactions. Due to the nature of SSI guaratees provided by Badger,
// blind writes can never encounter transaction conflicts (ErrConflict).
func NewWriteBatch(db *badger.DB) *WriteBatch {
	return &WriteBatch{
		db:        db,
		txn:       db.NewTransaction(true),
		throttle:  y.NewThrottle(16),
		commitCBs: make([]WriteBatchCommitCB, 0, InitCommitCBsCapacity),
	}
}

// SetMaxPendingTxns sets a limit on maximum number of pending transactions while writing batches.
// This function should be called before using WriteBatch. Default value of MaxPendingTxns is
// 16 to minimise memory usage.
func (wb *WriteBatch) SetMaxPendingTxns(max int) {
	wb.throttle = y.NewThrottle(max)
}

// Cancel function must be called if there's a chance that Flush might not get
// called. If neither Flush or Cancel is called, the transaction oracle would
// never get a chance to clear out the row commit timestamp map, thus causing an
// unbounded memory consumption. Typically, you can call Cancel as a defer
// statement right after NewWriteBatch is called.
//
// Note that any committed writes would still go through despite calling Cancel.
func (wb *WriteBatch) Cancel() {
	if err := wb.throttle.Finish(); err != nil {
		log.Err(err).Msgf("WatchBatch.Cancel error while finishing: %v", err)
	}
	wb.txn.Discard()
}

func (wb *WriteBatch) callback(err error) {
	// sync.WaitGroup is thread-safe, so it doesn't need to be run inside wb.Lock.
	defer wb.throttle.Done(err)

	wb.Lock()
	defer wb.Unlock()

	wb.execCommitHandlers(err)

	if err == nil {
		return
	}

	if wb.err != nil {
		return
	}
	wb.err = err
}

// Should be called with lock acquired.
func (wb *WriteBatch) execCommitHandlers(err error) {
	if len(wb.commitCBs) == 0 {
		// no callbacks to exec
		return
	}

	log.Info().Msgf("number of callbacks: %d", len(wb.commitCBs))

	for _, cb := range wb.commitCBs {
		if cb != nil {
			go cb(err)
		}
	}

	// Reset the callback list
	wb.commitCBs = make([]WriteBatchCommitCB, 0, InitCommitCBsCapacity)
}

func (wb *WriteBatch) addCommitHandler(cb WriteBatchCommitCB) {
	wb.commitCBs = append(wb.commitCBs, cb)
}

// Should be called with lock acquired.
func (wb *WriteBatch) txnSentEntry(e *badger.Entry, cb WriteBatchCommitCB) error {
	err := wb.txn.SetEntry(e)
	if err != nil {
		return err
	}
	wb.addCommitHandler(cb)
	return nil
}

// Should be called with lock acquired.
func (wb *WriteBatch) handleEntry(e *badger.Entry, cb WriteBatchCommitCB) error {
	if err := wb.txnSentEntry(e, cb); err != badger.ErrTxnTooBig {
		return err
	}
	// Txn has reached it's zenith. Commit now.
	if cerr := wb.commit(); cerr != nil {
		return cerr
	}
	// This time the error must not be badger.ErrTxnTooBig, otherwise, we make the
	// error permanent.
	if err := wb.txnSentEntry(e, cb); err != nil {
		wb.err = err
		return err
	}
	return nil
}

// SetEntry is the equivalent of Txn.SetEntry.
func (wb *WriteBatch) SetEntry(e *badger.Entry, cb WriteBatchCommitCB) error {
	wb.Lock()
	defer wb.Unlock()
	return wb.handleEntry(e, cb)
}

// Set is equivalent of Txn.Set().
func (wb *WriteBatch) Set(k, v []byte, cb WriteBatchCommitCB) error {
	e := &badger.Entry{Key: k, Value: v}
	return wb.SetEntry(e, cb)
}

// Should be called with lock acquired.
func (wb *WriteBatch) txnDelete(k []byte, cb WriteBatchCommitCB) error {
	err := wb.txn.Delete(k)
	if err != nil {
		return err
	}
	wb.addCommitHandler(cb)
	return nil
}

// Delete is equivalent of Txn.Delete.
func (wb *WriteBatch) Delete(k []byte, cb WriteBatchCommitCB) error {
	wb.Lock()
	defer wb.Unlock()

	if err := wb.txnDelete(k, cb); err != badger.ErrTxnTooBig {
		return err
	}
	if err := wb.commit(); err != nil {
		return err
	}
	if err := wb.txnDelete(k, cb); err != nil {
		wb.err = err
		return err
	}
	return nil
}

// Caller to commit must hold a write lock.
func (wb *WriteBatch) commit() error {
	if wb.err != nil {
		return wb.err
	}
	if err := wb.throttle.Do(); err != nil {
		return err
	}
	wb.txn.CommitWith(wb.callback)
	wb.txn = wb.db.NewTransaction(true)
	// wb.txn.commitTs = wb.commitTs
	return wb.err
}

// Flush must be called at the end to ensure that any pending writes get committed to Badger. Flush
// returns any error stored by WriteBatch.
func (wb *WriteBatch) Flush() error {
	wb.Lock()
	_ = wb.commit()
	wb.txn.Discard()
	wb.Unlock()

	if err := wb.throttle.Finish(); err != nil {
		// At this point we have thrown away the transaction.
		// Exec the callbacks on the transactions that haven't finished.
		wb.execCommitHandlers(err)
		return err
	}

	// At this point we have thrown away the transaction.
	// Exec the callbacks on the transactions that haven't finished.
	wb.execCommitHandlers(wb.err)
	return wb.err
}

// Error returns any errors encountered so far. No commits would be run once an error is detected.
func (wb *WriteBatch) Error() error {
	wb.Lock()
	defer wb.Unlock()
	return wb.err
}
