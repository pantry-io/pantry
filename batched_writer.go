package requeue

import (
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
)

type batchedWriter struct {
	db *badger.DB

	mu sync.RWMutex
	wb *WriteBatch

	quit chan struct{}
	done chan struct{}
}

func newBatchedWriter(db *badger.DB, d time.Duration) *batchedWriter {
	bw := &batchedWriter{
		db:   db,
		wb:   NewWriteBatch(db),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	go bw.loop(d)

	return bw
}

// On duration, call flush so we don't end up with writes waiting too long to be
// committed.
func (bw *batchedWriter) loop(d time.Duration) {
	ticker := time.NewTicker(d)
	for {
		select {
		case <-ticker.C:
			bw.flush(false)
		case <-bw.quit:
			ticker.Stop()
			bw.flush(true)
			close(bw.done)
			return
		}
	}
}

func (bw *batchedWriter) flush(last bool) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	log.Debug().Msg("flushing writes to badger")
	if err := bw.wb.Flush(); err != nil {
		// Not the best error handling, but you could have some kind of callback too.
		log.Err(err).Msgf("could not flush: %v", err)
	}
	if last {
		bw.wb = nil
	} else {
		bw.wb = NewWriteBatch(bw.db)
	}
}

func (bw *batchedWriter) Close() {
	close(bw.quit)
	<-bw.done
}

func (bw *batchedWriter) Set(k, v []byte, cb WriteBatchCommitCB) error {
	bw.mu.RLock()
	defer bw.mu.RUnlock()
	return bw.wb.Set(k, v, cb)
}

// Calls to Flush always reset the transaction. If there was an error then the
// transaction wasn't committed but a previous one may have been.
// Calls to Set may try to commit the transaction.