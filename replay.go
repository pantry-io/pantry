package requeue

import (
	"encoding"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nickpoorman/nats-requeue/flatbuf"
)

type BackoffStrategy string

const (
	BackoffStrategy_Fixed       = BackoffStrategy("fixed")
	BackoffStrategy_Exponential = BackoffStrategy("exponential")
	BackoffStrategy_Unknown     = BackoffStrategy("unknown")
)

// Things we need to save in order to replay this message:
//  + The subject it was originally supposed to go to.
//	+ The number of times it should be retried.
//  + The TTL for when the message should expire.
//  + The delay before it should be retried again.
//  + Backoff strategy. i.e. fixed interval or exponential
type RequeueMeta struct {
	// The number of times requeue should be attempted.
	Retries int64

	// The TTL for when the msssage should expire. This is useful for ensuring
	// messages are not retried after a certain time.
	TTL int64

	// The delay before the message should be replayed in nanoseconds.
	Delay int64

	// Backoff strategy that will be used for determining the next delay should
	// the message fail to be acknowledged on replay. i.e. fixed interval or
	// exponential
	BackoffStrategy BackoffStrategy
}

func (r *RequeueMeta) MarshalBinary() ([]byte, error) {
	b := flatbuffers.NewBuilder(0)
	meta := r.toFlatbuf(b)
	b.Finish(meta)
	return b.FinishedBytes(), nil
}

func (r *RequeueMeta) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsRequeueMeta(data, 0)
	r.fromFlatbuf(m)
	return nil
}

func (r *RequeueMeta) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	backoffStrategy := b.CreateByteString([]byte(r.BackoffStrategy))

	flatbuf.RequeueMetaStart(b)
	flatbuf.RequeueMetaAddRetries(b, r.Retries)
	flatbuf.RequeueMetaAddTtl(b, r.TTL)
	flatbuf.RequeueMetaAddDelay(b, r.Delay)
	flatbuf.RequeueMetaAddBackoffStrategy(b, backoffStrategy)
	return flatbuf.RequeueMetaEnd(b)
}

func (r *RequeueMeta) fromFlatbuf(m *flatbuf.RequeueMeta) {
	r.Retries = m.Retries()
	r.TTL = m.Ttl()
	r.Delay = m.Delay()

	switch BackoffStrategy(string(m.BackoffStrategy())) {
	case BackoffStrategy_Fixed:
		r.BackoffStrategy = BackoffStrategy_Fixed
	case BackoffStrategy_Exponential:
		r.BackoffStrategy = BackoffStrategy_Exponential
	default:
		r.BackoffStrategy = BackoffStrategy_Unknown
	}
}

type RequeueMsg struct {
	Meta RequeueMeta

	// The original subject of the message.
	OriginalSubject string

	// Original message payload.
	OriginalPayload []byte
}

func (r *RequeueMsg) MarshalBinary() ([]byte, error) {
	b := flatbuffers.NewBuilder(0)
	msg := r.toFlatbuf(b)
	b.Finish(msg)
	return b.FinishedBytes(), nil
}

func (r *RequeueMsg) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsRequeueMessage(data, 0)
	r.fromFlatbuf(m)
	return nil
}

func (r *RequeueMsg) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	meta := r.Meta.toFlatbuf(b)

	originalSubject := b.CreateByteString([]byte(r.OriginalSubject))
	originalPayload := b.CreateByteVector(r.OriginalPayload)

	flatbuf.RequeueMessageStart(b)
	flatbuf.RequeueMessageAddOriginalSubject(b, originalSubject)
	flatbuf.RequeueMessageAddOriginalPayload(b, originalPayload)
	flatbuf.RequeueMessageAddMeta(b, meta)
	return flatbuf.RequeueMessageEnd(b)
}

func (r *RequeueMsg) fromFlatbuf(m *flatbuf.RequeueMessage) {
	r.Meta.fromFlatbuf(m.Meta(nil))
	r.OriginalSubject = string(m.OriginalSubject())
	r.OriginalPayload = m.OriginalPayloadBytes()
}

var (
	_ encoding.BinaryMarshaler   = (*RequeueMeta)(nil)
	_ encoding.BinaryUnmarshaler = (*RequeueMeta)(nil)
	_ encoding.BinaryMarshaler   = (*RequeueMsg)(nil)
	_ encoding.BinaryUnmarshaler = (*RequeueMsg)(nil)
)
