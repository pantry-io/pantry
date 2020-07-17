package requeue

import (
	"errors"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go"
)

var (
	RequeueRequestRetriesExceededError = errors.New("requeue: retries exceeded")
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

	err := backoff.Retry(operation, backoff.NewExponentialBackOff())
	// if err != nil {
	// 	// Handle error.
	// 	return
	// }

	// backoff := 1 * time.Second
	// for i := 0; i < times; i++ {
	// 	// Are there any critical errors we should return early from?
	// 	if err == nil {
	// 		return msg, nil
	// 	}
	// 	time.Sleep(backoff)
	// 	backoff = backoff * 2
	// }
	// return nil, RequeueRequestRetriesExceededError
	return msg, err
}
