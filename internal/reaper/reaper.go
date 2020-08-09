package reaper

import (
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	badgerInternal "github.com/nickpoorman/nats-requeue/internal/badger"
	"github.com/nickpoorman/nats-requeue/internal/ticker"
	"github.com/rs/zerolog/log"
)

const (
	// The interval in which to check for zombied instances.
	DefaultReapInterval = 60 * time.Second
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
	dir         string
	instanceDir string
	opts        Options

	quit chan struct{}
}

func NewReaper(dir string, instanceDir string, options ...Option) (*Reaper, error) {
	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	reaper := &Reaper{
		dir:         dir,
		instanceDir: instanceDir,
		opts:        opts,
		quit:        make(chan struct{}),
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
			r.reap()
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

func (r *Reaper) reap() {
	// Loop over instances in the dir
	dir, err := os.Open(r.dir)
	if err != nil {
		log.Err(err).Msg("unable to open data directory for reaper")
		return
	}
	defer dir.Close()
	files, err := dir.Readdir(-1)
	if err != nil {
		log.Err(err).Msg("unable to list directory files for reaper")
		return
	}
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		instanceId := file.Name()
		// Try to merge the instance on that directory.
		// This will only succeed if the badger directory is not already locked.
		merged, err := r.mergeInstance(r.dir, instanceId)
		if err != nil {
			log.Err(err).
				Str("instanceId", instanceId).
				Msg("unable to merge instance for directory")
			return
		}

		if merged {
			r.triggerReapedCallbacks(r.dir, instanceId)
		}
	}
}

func (r *Reaper) mergeInstance(dir, instanceId string) (bool, error) {
	path := badgerInternal.InstanceDir(dir, instanceId)

	log.Debug().
		Str("instanceId", instanceId).
		Str("dir", dir).
		Str("path", path).
		Msg("attempting to merge badger instance")

	instance, err := badgerInternal.Open(path)
	if err != nil {
		// Really don't like this. We should probably check syscall.EWOULDBLOCK
		// by unwrapping. Badger isn't currently using wrapping.
		if !strings.Contains(err.Error(), syscall.EWOULDBLOCK.Error()) {
			log.Err(err).Msg("problem opening badger instance")
			return false, err
		}
		// Ignore the "resource temporarily unavailable" errors.
		// Return that we didn't merge the instance.
		return false, nil
	}
	defer instance.Close()

	// TODO: Do merge here

	return true, nil
}

func (r *Reaper) triggerReapedCallbacks(dir, instanceId string) {
	for _, cb := range r.opts.reapedCallbacks {
		if cb == nil {
			continue
		}
		go cb(dir, instanceId)
	}
}
