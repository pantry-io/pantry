package requeue

import (
	"bytes"
	"encoding"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/flatbuf"
)

// BackoffStrategy mirrors the flatbuf enum.
type BackoffStrategy int8

const (
	BackoffStrategy_Undefined BackoffStrategy = iota
	BackoffStrategy_Exponential
	BackoffStrategy_Fixed
)

// Things we need to save in order to replay this message:
//  + The subject it was originally supposed to go to.
//	+ The number of times it should be retried.
//  + The TTL for when the message should expire.
//  + The delay before it should be retried again.
//  + Backoff strategy. i.e. fixed interval or exponential
type RequeueMessage struct {
	// The number of times requeue should be attempted.
	Retries uint64

	// The TTL for when the msssage should expire. This is useful for ensuring
	// messages are not retried after a certain time.
	TTL uint64

	// The delay before the message should be replayed in nanoseconds.
	Delay uint64

	// Backoff strategy that will be used for determining the next delay should
	// the message fail to be acknowledged on replay. i.e. fixed interval or
	// exponential
	BackoffStrategy BackoffStrategy

	// The original subject of the message.
	OriginalSubject string

	// Original message payload.
	OriginalPayload []byte
}

func DefaultRequeueMessage() RequeueMessage {
	return RequeueMessage{
		BackoffStrategy: BackoffStrategy_Undefined,
	}
}

func RequeueMessageFromNATS(msg *nats.Msg) RequeueMessage {
	m := DefaultRequeueMessage()
	// Unmarshal currently doesn't return any errors
	_ = m.UnmarshalBinary(msg.Data)
	return m
}

func (r *RequeueMessage) Bytes() []byte {
	b := flatbuffers.NewBuilder(0)
	msg := r.toFlatbuf(b)
	b.Finish(msg)
	return b.FinishedBytes()
}

func (r *RequeueMessage) MarshalBinary() ([]byte, error) {
	return r.Bytes(), nil
}

func (r *RequeueMessage) NewReader() io.Reader {
	return bytes.NewReader(r.Bytes())
}

func (r *RequeueMessage) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsRequeueMessage(data, 0)
	r.fromFlatbuf(m)
	return nil
}

func (r *RequeueMessage) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	originalSubject := b.CreateByteString([]byte(r.OriginalSubject))
	originalPayload := b.CreateByteVector(r.OriginalPayload)

	flatbuf.RequeueMessageStart(b)
	flatbuf.RequeueMessageAddRetries(b, r.Retries)
	flatbuf.RequeueMessageAddTtl(b, r.TTL)
	flatbuf.RequeueMessageAddDelay(b, r.Delay)
	flatbuf.RequeueMessageAddBackoffStrategy(b, r.backoffStrategyToFlatbuf())
	flatbuf.RequeueMessageAddOriginalSubject(b, originalSubject)
	flatbuf.RequeueMessageAddOriginalPayload(b, originalPayload)
	return flatbuf.RequeueMessageEnd(b)
}

func (r *RequeueMessage) fromFlatbuf(m *flatbuf.RequeueMessage) {
	r.Retries = m.Retries()
	r.TTL = m.Ttl()
	r.Delay = m.Delay()
	r.BackoffStrategy = BackoffStrategy(m.BackoffStrategy())
	r.OriginalSubject = string(m.OriginalSubject())
	r.OriginalPayload = m.OriginalPayloadBytes()
}

func (r *RequeueMessage) backoffStrategyToFlatbuf() flatbuf.BackoffStrategy {
	if r.BackoffStrategy > BackoffStrategy_Fixed {
		return flatbuf.BackoffStrategyUndefined
	}
	return flatbuf.BackoffStrategy(r.BackoffStrategy)
}

var (
	_ encoding.BinaryMarshaler   = (*RequeueMessage)(nil)
	_ encoding.BinaryUnmarshaler = (*RequeueMessage)(nil)
)
