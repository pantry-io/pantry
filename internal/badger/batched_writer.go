package badger

import (
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/rs/zerolog/log"
)

type BatchedWriter struct {
	db *badger.DB
	d  time.Duration

	mu sync.RWMutex
	wb *WriteBatch

	quit chan struct{}
	done chan struct{}

	flushKicked bool
}

func NewBatchedWriter(db *badger.DB, d time.Duration) *BatchedWriter {
	bw := &BatchedWriter{
		db:   db,
		d:    d,
		wb:   NewWriteBatch(db),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	go bw.loop(d)

	return bw
}

// On duration, call flush so we don't end up with writes waiting too long to be
// committed.
func (bw *BatchedWriter) loop(d time.Duration) {
	// ticker := time.NewTicker(d)
	// for {
	// 	select {
	// case <-ticker.C:
	// 	bw.flush(false)
	// case <-bw.quit:
	<-bw.quit
	// ticker.Stop()
	bw.flush(true)
	close(bw.done)
	// return
	// }
	// }
}

func (bw *BatchedWriter) flush(last bool) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	if bw.flushKicked {
		log.Info().Msg("batched-writer: flushing writes to badger")
		if err := bw.wb.Flush(); err != nil {
			// Not the best error handling, but you could have some kind of callback too.
			log.Err(err).Msgf("batched-writer: could not flush: %v", err)
		}
		bw.flushKicked = false
		if last {
			bw.wb = nil
		} else {
			bw.wb = NewWriteBatch(bw.db)
		}
	}
}

func (bw *BatchedWriter) Close() {
	close(bw.quit)
	<-bw.done
}

func (bw *BatchedWriter) Set(k, v []byte, cb WriteBatchCommitCB) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	// Create a timeout
	err := bw.wb.Set(k, v, cb)
	if !bw.flushKicked {
		bw.flushKicked = true
		go func() {
			<-time.After(bw.d)
			bw.flush(false)
		}()
	}
	return err
}

func (bw *BatchedWriter) SetEntry(e *badger.Entry, cb WriteBatchCommitCB) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	// Create a timeout
	err := bw.wb.SetEntry(e, cb)
	if !bw.flushKicked {
		bw.flushKicked = true
		go func() {
			<-time.After(bw.d)
			bw.flush(false)
		}()
	}
	return err
}

func (bw *BatchedWriter) WriteKVList(kvList *pb.KVList, cb WriteBatchCommitCB) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(kvList.Kv))
	var err error
	var errOnce sync.Once
	for _, kv := range kvList.Kv {
		e := badger.Entry{Key: kv.Key, Value: kv.Value}
		if len(kv.UserMeta) > 0 {
			e.UserMeta = kv.UserMeta[0]
		}
		if err := bw.SetEntry(&e, func(e error) {
			defer wg.Done()
			if e != nil {
				errOnce.Do(func() {
					err = e
				})
			}
		}); err != nil {
			return err
		}
	}
	go func() {
		wg.Wait()
		cb(err)
	}()

	return err
}

// Calls to Flush always reset the transaction. If there was an error then the
// transaction wasn't committed but a previous one may have been.
// Calls to Set may try to commit the transaction.
