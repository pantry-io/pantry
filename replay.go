package requeue

type BackoffStrategy string

const (
	BackoffStrategy_Fixed       = BackoffStrategy("fixed")
	BackoffStrategy_Exponential = BackoffStrategy("exponential")
)

// Things we need to save in order to replay this message:
//  + The subject it was originally supposed to go to.
//	+ The number of times it should be retried.
//  + The TTL for when the message should expire.
//  + The delay before it should be retried again.
//  + Backoff strategy. i.e. fixed interval or exponential
type RequeueMeta struct {
	//  The original subject of the message.
	OriginalSubject string

	// The number of times requeue should be attempted.
	Retries int64

	// The TTL for when the msssage should expire. This is useful for ensuring
	// messages are not retried after a certain time.
	MsgTTL int64

	// The delay before the message should be replayed in nanoseconds.
	Delay int64

	// Backoff strategy that will be used for determining the next delay should
	// the message fail to be acknowledged on replay. i.e. fixed interval or
	// exponential
	BackoffStrategy BackoffStrategy
}

type RequeueMsg struct {
	RequeueMeta RequeueMeta
	Body        []byte
}
