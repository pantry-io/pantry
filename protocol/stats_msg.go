package protocol

import (
	"encoding"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/flatbuf"
)

type InstanceStatsMessage struct {
	InstanceId string              `json:"instance_id"`
	Queues     []QueueStatsMessage `json:"queues"`
}

func DefaultInstanceStatsMessage() InstanceStatsMessage {
	return InstanceStatsMessage{}
}

func InstanceStatsMessageFromNATS(msg *nats.Msg) InstanceStatsMessage {
	m := DefaultInstanceStatsMessage()
	// Unmarshal currently doesn't return any errors
	_ = m.UnmarshalBinary(msg.Data)
	return m
}

func (i *InstanceStatsMessage) Bytes() []byte {
	b := flatbuffers.NewBuilder(0)
	msg := i.toFlatbuf(b)
	b.Finish(msg)
	return b.FinishedBytes()
}

func (i *InstanceStatsMessage) MarshalBinary() ([]byte, error) {
	return i.Bytes(), nil
}

func (i *InstanceStatsMessage) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsInstanceStatsMessage(data, 0)
	i.fromFlatbuf(m)
	return nil
}

func (i *InstanceStatsMessage) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	// Add the queues to the builder.
	queueOffsets := make([]flatbuffers.UOffsetT, len(i.Queues))
	for i, q := range i.Queues {
		queueOffsets[i] = q.toFlatbuf(b)
	}

	// Add the offsets for the queues in reverse so we maintain order.
	flatbuf.InstanceStatsMessageStartQueuesVector(b, len(i.Queues))
	for i := len(queueOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(queueOffsets[i])
	}

	queues := b.EndVector(len(queueOffsets))

	instanceId := b.CreateByteString([]byte(i.InstanceId))
	flatbuf.InstanceStatsMessageStart(b)
	flatbuf.InstanceStatsMessageAddInstanceId(b, instanceId)
	flatbuf.InstanceStatsMessageAddQueues(b, queues)
	return flatbuf.InstanceStatsMessageEnd(b)
}

func (i *InstanceStatsMessage) fromFlatbuf(m *flatbuf.InstanceStatsMessage) {
	i.InstanceId = string(m.InstanceId())
	i.Queues = make([]QueueStatsMessage, m.QueuesLength())
	for idx := range i.Queues {
		obj := &flatbuf.QueueStatsMessage{}
		if ok := m.Queues(obj, idx); !ok {
			continue
		}
		i.Queues[idx].fromFlatbuf(obj)
	}
}

type QueueStatsMessage struct {
	QueueName string `json:"queue_name"`
	Enqueued  int64  `json:"enqueued"`
	InFlight  int64  `json:"in_flight"`
}

func (q *QueueStatsMessage) Bytes() []byte {
	b := flatbuffers.NewBuilder(0)
	msg := q.toFlatbuf(b)
	b.Finish(msg)
	return b.FinishedBytes()
}

func (q *QueueStatsMessage) MarshalBinary() ([]byte, error) {
	return q.Bytes(), nil
}

func (q *QueueStatsMessage) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsQueueStatsMessage(data, 0)
	q.fromFlatbuf(m)
	return nil
}

func (q *QueueStatsMessage) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	queueName := b.CreateByteString([]byte(q.QueueName))

	flatbuf.QueueStatsMessageStart(b)
	flatbuf.QueueStatsMessageAddQueueName(b, queueName)
	flatbuf.QueueStatsMessageAddEnqueued(b, q.Enqueued)
	flatbuf.QueueStatsMessageAddInFlight(b, q.InFlight)
	return flatbuf.RequeueMessageEnd(b)
}

func (q *QueueStatsMessage) fromFlatbuf(m *flatbuf.QueueStatsMessage) {
	q.QueueName = string(m.QueueName())
	q.Enqueued = m.Enqueued()
	q.InFlight = m.InFlight()
}

var (
	_ encoding.BinaryMarshaler   = (*InstanceStatsMessage)(nil)
	_ encoding.BinaryUnmarshaler = (*InstanceStatsMessage)(nil)
	_ encoding.BinaryMarshaler   = (*QueueStatsMessage)(nil)
	_ encoding.BinaryUnmarshaler = (*QueueStatsMessage)(nil)
)
