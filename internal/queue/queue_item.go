package queue

import "time"

type QueueItem struct {
	// K is the key of the item.
	K []byte

	// V is the value of the item.
	V []byte

	// ExpiresAt is a Unix time, the number of seconds elapsed
	// since January 1, 1970 UTC.
	ExpiresAt uint64
}

// IsExpired returns true if this item has expired.
func (qi QueueItem) IsExpired() bool {
	return qi.ExpiresAt <= uint64(time.Now().Unix())
}

// ExpiresAtTime returns the Time this item will expire.
func (qi QueueItem) ExpiresAtTime() time.Time {
	return time.Unix(int64(qi.ExpiresAt), 0)
}

// DurationUntilExpires returns a duration indicating how much time until this item
// expires.
func (qi QueueItem) DurationUntilExpires() time.Duration {
	return time.Until(qi.ExpiresAtTime())
}
