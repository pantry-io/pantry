package gate

import "sync/atomic"

const (
	closedState = 0
	openState   = 1
)

type Counter struct {
	state int32
	i     int64
}

// Open the gate. While the gate is opened the counter will not be adjusted.
func (c *Counter) Open() {
	atomic.StoreInt32(&c.state, openState)
}

// Close the gate. While the gate is closed the counter will be adjusted.
func (c *Counter) Close() {
	atomic.StoreInt32(&c.state, closedState)
}

func (c *Counter) Add(i int64) {
	if atomic.LoadInt32(&c.state) == openState {
		return
	}
	atomic.AddInt64(&c.i, i)
}

func (c *Counter) Value() int64 {
	return atomic.LoadInt64(&c.i)
}
