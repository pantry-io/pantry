package reaper

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
	badgerInternal "github.com/nickpoorman/nats-requeue/internal/badger"
	"github.com/nickpoorman/nats-requeue/internal/ticker"
	"github.com/rs/zerolog/log"
)

const (
	// The interval in which to check for zombied instances.
	DefaultReapInterval = 30 * time.Second
)

// ReapedCallbackFunc is a callback to trigger when an instance is reaped.
type ReapedCallbackFunc func(dir, instanceId string)

type Options struct {
	// The interval in which to check for zombied instances.
	reapInterval time.Duration

	// Callbacks to trigger when an instance is reaped.
	reapedCallbacks []ReapedCallbackFunc
}

func GetDefaultOptions() Options {
	return Options{
		reapInterval:    DefaultReapInterval,
		reapedCallbacks: make([]ReapedCallbackFunc, 0),
	}
}

// Option is a function on the options for Reaper.
type Option func(*Options) error

// ReapInterval sets the interval in which to check for zombied instances.
func ReapInterval(reapInterval time.Duration) Option {
	return func(o *Options) error {
		o.reapInterval = reapInterval
		return nil
	}
}

// ReapedCallbacks appends a callback to trigger when an instance is reaped.
func ReapedCallbacks(callbacks ...ReapedCallbackFunc) Option {
	return func(o *Options) error {
		for _, cb := range callbacks {
			if cb != nil {
				o.reapedCallbacks = append(o.reapedCallbacks, cb)
			}
		}
		return nil
	}
}

type Reaper struct {
	dst            *badger.DB
	dataDir        string
	dstInstanceDir string
	opts           Options

	quit chan struct{}
}

func NewReaper(dst *badger.DB, dataDir string, dstInstanceDir string, options ...Option) (*Reaper, error) {
	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	reaper := &Reaper{
		dst:            dst,
		dataDir:        dataDir,
		dstInstanceDir: dstInstanceDir,
		opts:           opts,
		quit:           make(chan struct{}),
	}
	go reaper.initBackgroundTasks()
	return reaper, nil
}

func (r *Reaper) initBackgroundTasks() {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		t := ticker.New(r.opts.reapInterval)
		go func() {
			<-r.quit
			t.Stop()
		}()
		t.Loop(func() bool {
			_ = r.reap()
			return true
		})
	}()
}

// Close will stop the reaper from running.
// If there are any running tasks we should probably allow those to finish
// first.
func (r *Reaper) Close() {
	close(r.quit)
}

// Returns all the file paths in the dataDir with the current instance excluded.
func (r *Reaper) getOtherInstanceIds() ([]string, error) {
	// Loop over instances in the dir
	dir, err := os.Open(r.dataDir)
	if err != nil {
		return nil, fmt.Errorf("unable to open data directory for reaper: %w", err)
	}
	defer dir.Close()
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, fmt.Errorf("unable to list directory files for reaper: %w", err)
	}

	outPaths := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		instanceId := file.Name()
		path := badgerInternal.InstanceDir(r.dataDir, instanceId)
		if path == r.dstInstanceDir {
			continue
		}
		outPaths = append(outPaths, instanceId)
	}
	return outPaths, nil
}

func (r *Reaper) reap() error {
	log.Debug().Msg("reaper running")

	instanceIds, err := r.getOtherInstanceIds()
	if err != nil {
		log.Err(err).Msg("problem gathering other instance paths")
		return err
	}

	log.Debug().Msgf("reaper found instances: %v", instanceIds)

	for _, instanceId := range instanceIds {
		instancePath := badgerInternal.InstanceDir(r.dataDir, instanceId)

		// Try to merge the instance on that directory.
		// This will only succeed if the badger directory is not already locked.
		merged, err := r.mergeInstance(instancePath)
		if err != nil {
			log.Err(err).
				Str("instancePath", instancePath).
				Msg("unable to merge instance for directory")
			return err
		}
		if !merged {
			// Could not merge. This could be because it was locked.
			continue
		}

		// Clean up by removing the instance directory from disk.
		log.Debug().
			Str("instancePath", instancePath).
			Msgf("removing instance directory from disk")
		if err := r.removeInstance(instancePath); err != nil {
			log.Err(err).
				Str("instancePath", instancePath).
				Msg("unable to remove instance directory")
			return err
		}

		r.triggerReapedCallbacks(r.dataDir, instanceId)
	}

	return nil
}

func (r *Reaper) mergeInstance(instancePath string) (bool, error) {
	instance, err := r.openBadgerInstance(instancePath)
	if err != nil {
		return false, err
	}
	if instance == nil {
		log.Debug().Msgf("instance is locked: %s", instancePath)
		// Instance is locked.
		return false, nil
	}
	defer instance.Close()

	log.Debug().
		Str("instancePath", instancePath).
		Msg("attempting to merge badger instance")

	if err := copyBadger(r.dst, instance); err != nil {
		return false, fmt.Errorf("merge instance: problem copying badger: %w", err)
	}

	// Drop all the data we've migrated.
	if err := instance.DropAll(); err != nil {
		return false, fmt.Errorf("merge instance: problem dropping database: %w", err)
	}

	return true, nil
}

// This will try to acquire the directory lock before removing it.
// It then swallows the ".../LOCK: no such file or directory" error since we
// have already deleted the lock file.
func (r *Reaper) removeInstance(instancePath string) error {
	// It's possible another instance could try to reap this instance
	// in the meantime. To prevent that, we try to acquire the lock first.
	dirLockGuard, err := badgerInternal.AcquireDirectoryLock(
		instancePath,
		badgerInternal.LockFile,
		false,
	)
	if err != nil {
		return err
	}
	defer func() {
		if dirLockGuard != nil {
			// This will most likely return an error since we have removed
			// the file. That's ok.
			_ = dirLockGuard.Release()
		}
	}()

	log.Info().Msgf("removing instance directory: %s", instancePath)

	if err := os.RemoveAll(instancePath); err != nil {
		return fmt.Errorf("problem removing instance: %w", err)
	}
	return nil
}

func (r *Reaper) openBadgerInstance(path string) (*badger.DB, error) {
	instance, err := badgerInternal.Open(path)
	if err != nil {
		// Really don't like this. We should probably check syscall.EWOULDBLOCK
		// by unwrapping. Badger isn't currently using wrapping.
		if !strings.Contains(err.Error(), syscall.EWOULDBLOCK.Error()) {
			log.Err(err).Msg("problem opening badger instance")
			return instance, err
		}
		// Ignore the "resource temporarily unavailable" errors.
		// Return that we didn't merge the instance.
		return nil, nil
	}
	return instance, nil
}

func (r *Reaper) triggerReapedCallbacks(dir, instanceId string) {
	for _, cb := range r.opts.reapedCallbacks {
		if cb == nil {
			continue
		}
		go cb(dir, instanceId)
	}
}

func copyBadger(dst, src *badger.DB) error {
	streamReader := src.NewStream()

	// -- Optional settings
	streamReader.LogPrefix = "Reaper.Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	streamReader.KeyToList = func(key []byte, itr *badger.Iterator) (*pb.KVList, error) {
		list := &pb.KVList{}
		if !itr.Valid() {
			return list, nil
		}
		item := itr.Item()
		if item.IsDeletedOrExpired() {
			return list, nil
		}
		if !bytes.Equal(key, item.Key()) {
			// Break out on the first encounter with another key.
			return list, nil
		}

		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		kv := &pb.KV{
			Key:       item.KeyCopy(nil),
			Value:     valCopy,
			UserMeta:  []byte{item.UserMeta()},
			Version:   item.Version(),
			ExpiresAt: item.ExpiresAt(),
		}
		list.Kv = append(list.Kv, kv)
		return list, nil
	}

	// -- End of optional settings.

	wb := dst.NewWriteBatch()
	defer wb.Cancel()

	streamReader.Send = func(list *pb.KVList) error {
		return wb.Write(list)
	}

	// Run the stream
	// TODO: We should do our best to complete but if we need to shut down, we should handle that case.
	if err := streamReader.Orchestrate(context.Background()); err != nil {
		return err
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("problem flushing batch writer: %w", err)
	}

	return nil
}
