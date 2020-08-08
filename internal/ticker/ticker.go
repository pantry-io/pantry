package ticker

import (
	"time"
)

type Ticker struct {
	ticker *time.Ticker
	quit   chan struct{}
}

func New(d time.Duration) *Ticker {
	return &Ticker{
		ticker: time.NewTicker(d),
		quit:   make(chan struct{}),
	}
}

// Loop will run the provided function fn on a loop. Once Stop() has been called,
// the loop will not run even if there are pending ticks from the ticker.
// If the provided function fn returns false then the loop will terminate.
func (t *Ticker) Loop(fn func() bool) {
	defer t.ticker.Stop()
	for {
		select {
		// Don't run this iteration of the loop if we've already been told to stop.
		case <-t.quit:
			return
		default:
			select {
			case <-t.quit:
				return
			case <-t.ticker.C:
				if !fn() {
					return
				}
			}
		}
	}
}

func (t *Ticker) Stop() {
	close(t.quit)
}
