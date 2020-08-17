package requeue

import (
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go"
)

func RetryRequest(nc *nats.Conn, subject string, payload []byte, timeout time.Duration, times int) (*nats.Msg, error) {
	var msg *nats.Msg
	// An operation that may fail.
	operation := func() error {
		m, err := nc.Request(subject, payload, timeout)
		if err != nil {
			// Any permanent errors?
			// Permanent(err)
			return err
		}
		msg = m
		return nil
	}
	return msg, backoff.Retry(operation, backoff.NewExponentialBackOff())
}
