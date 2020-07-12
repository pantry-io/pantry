package requeue

import "sync"

var (
	// Dummy channel for nil closers.
	dummyCloserChan = make(chan struct{})
)

// Closer holds the two things we need to close a goroutine and wait for it to finish: a chan
// to tell the goroutine to shut down, and a WaitGroup with which to wait for it to finish shutting
// down.
type Closer struct {
	closed  chan struct{}
	waiting sync.WaitGroup
}

// NewCloser constructs a new Closer, with an initial count on the WaitGroup.
func NewCloser(initial int) *Closer {
	ret := &Closer{closed: make(chan struct{})}
	ret.waiting.Add(initial)
	return ret
}

// AddRunning Add()'s delta to the WaitGroup.
func (lc *Closer) AddRunning(delta int) {
	lc.waiting.Add(delta)
}

// Signal signals the HasBeenClosed signal.
func (lc *Closer) Signal() {
	close(lc.closed)
}

// HasBeenClosed gets signaled when Signal() is called.
func (lc *Closer) HasBeenClosed() <-chan struct{} {
	if lc == nil {
		return dummyCloserChan
	}
	return lc.closed
}

// Done calls Done() on the WaitGroup.
func (lc *Closer) Done() {
	if lc == nil {
		return
	}
	lc.waiting.Done()
}

// Wait waits on the WaitGroup.  (It waits for NewCloser's initial value, AddRunning, and Done
// calls to balance out.)
func (lc *Closer) Wait() {
	lc.waiting.Wait()
}

// SignalAndWait calls Signal(), then Wait().
func (lc *Closer) SignalAndWait() {
	lc.Signal()
	lc.Wait()
}
