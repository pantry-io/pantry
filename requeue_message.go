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
type RequeueMeta struct {
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
}

func DefaultRequeueMeta() RequeueMeta {
	return RequeueMeta{
		Retries:         0,
		TTL:             0,
		Delay:           0,
		BackoffStrategy: BackoffStrategy_Undefined,
	}
}

func (r *RequeueMeta) Bytes() []byte {
	b := flatbuffers.NewBuilder(0)
	meta := r.toFlatbuf(b)
	b.Finish(meta)
	return b.FinishedBytes()
}

func (r *RequeueMeta) MarshalBinary() ([]byte, error) {
	return r.Bytes(), nil
}

func (r *RequeueMeta) NewReader() io.Reader {
	return bytes.NewReader(r.Bytes())
}

func (r *RequeueMeta) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsRequeueMeta(data, 0)
	r.fromFlatbuf(m)
	return nil
}

func (r *RequeueMeta) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	flatbuf.RequeueMetaStart(b)
	flatbuf.RequeueMetaAddRetries(b, r.Retries)
	flatbuf.RequeueMetaAddTtl(b, r.TTL)
	flatbuf.RequeueMetaAddDelay(b, r.Delay)
	flatbuf.RequeueMetaAddBackoffStrategy(b, r.backoffStrategyToFlatbuf())
	return flatbuf.RequeueMetaEnd(b)
}

func (r *RequeueMeta) backoffStrategyToFlatbuf() flatbuf.BackoffStrategy {
	if r.BackoffStrategy > BackoffStrategy_Fixed {
		return flatbuf.BackoffStrategyUndefined
	}
	return flatbuf.BackoffStrategy(r.BackoffStrategy)
}

func (r *RequeueMeta) fromFlatbuf(m *flatbuf.RequeueMeta) {
	r.Retries = m.Retries()
	r.TTL = m.Ttl()
	r.Delay = m.Delay()
	r.BackoffStrategy = BackoffStrategy(m.BackoffStrategy())
}

type RequeueMessage struct {
	Meta RequeueMeta

	// The original subject of the message.
	OriginalSubject string

	// Original message payload.
	OriginalPayload []byte
}

func DefaultRequeueMessage() RequeueMessage {
	return RequeueMessage{
		Meta: DefaultRequeueMeta(),
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
	meta := r.Meta.toFlatbuf(b)

	originalSubject := b.CreateByteString([]byte(r.OriginalSubject))
	originalPayload := b.CreateByteVector(r.OriginalPayload)

	flatbuf.RequeueMessageStart(b)
	flatbuf.RequeueMessageAddOriginalSubject(b, originalSubject)
	flatbuf.RequeueMessageAddOriginalPayload(b, originalPayload)
	flatbuf.RequeueMessageAddMeta(b, meta)
	return flatbuf.RequeueMessageEnd(b)
}

func (r *RequeueMessage) fromFlatbuf(m *flatbuf.RequeueMessage) {
	r.Meta.fromFlatbuf(m.Meta(nil))
	r.OriginalSubject = string(m.OriginalSubject())
	r.OriginalPayload = m.OriginalPayloadBytes()
}

var (
	_ encoding.BinaryMarshaler   = (*RequeueMeta)(nil)
	_ encoding.BinaryUnmarshaler = (*RequeueMeta)(nil)
	_ encoding.BinaryMarshaler   = (*RequeueMessage)(nil)
	_ encoding.BinaryUnmarshaler = (*RequeueMessage)(nil)
)
