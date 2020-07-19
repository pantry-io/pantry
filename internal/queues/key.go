package queues

import (
	"fmt"
	"strings"
)

// --------------------------------------------------------------------------
// KSUID

const (
	// A string-encoded minimum value for a KSUID
	minStringEncoded = "000000000000000000000000000"

	// A string-encoded maximum value for a KSUID
	maxStringEncoded = "aWgEPTl1tmebfsQzFP4bxwgy80V"
)

// --------------------------------------------------------------------------

// All messages are stored under the _q namespace.
// Queues each have their own name under the _m and _s buckets, e.g., _q._s.high.
// Buckets are used to group properties. For example all messages are written
// to the _m bucket and all state properties are written to the _s bucket.
//
// Some examples:
// _q._m.high.aWgEPTl1tmebfsQzFP4bxwgy80V
// _q._s.high.checkpoint
// _q._s.medium.checkpoint
// _q._s.low.checkpoint
// _q._s.low.other_state_property
const sep = "."
const QueuesNamespace = "_q"

const MessagesBucket = "_m"

const StateBucket = "_s"
const CheckpointProperty = "checkpoint"
const NameProperty = "name"

type QueueKey struct {
	Namespace string
	Bucket    string
	Name      string
	Property  string
}

func NewQueueKeyForMessage(queue, property string) QueueKey {
	return QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    MessagesBucket,
		Name:      queue,
		Property:  property,
	}
}

func NewQueueKeyForState(queue, property string) QueueKey {
	return QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    StateBucket,
		Name:      queue,
		Property:  property,
	}
}

func ParseQueueKey(k []byte) QueueKey {
	spl := strings.Split(string(k), sep)
	return QueueKey{
		Namespace: spl[0],
		Bucket:    spl[1],
		Name:      spl[2],
		Property:  spl[3],
	}
}

func (q QueueKey) Bytes() []byte {
	return []byte(q.PropertyPath())
}

func (q QueueKey) BucketPath() string {
	return fmt.Sprintf("%s.%s", q.Namespace, q.Bucket)
}

func (q QueueKey) NamePath() string {
	return fmt.Sprintf("%s.%s.%s", q.Namespace, q.Bucket, q.Name)
}

func (q QueueKey) PropertyPath() string {
	return fmt.Sprintf("%s.%s.%s.%s", q.Namespace, q.Bucket, q.Name, q.Property)
}

// PrefixOf a common prefix between two keys (common leading bytes) which is
// then used as a prefix for Badger to narrow down SSTables to traverse.
func (q QueueKey) PrefixOf(untilQK QueueKey) []byte {
	var prefix []byte
	seek := q.Bytes()
	until := untilQK.Bytes()

	// Calculate the minimum length
	length := len(seek)
	if len(until) < length {
		length = len(until)
	}

	// Iterate through the bytes and append common ones
	for i := 0; i < length; i++ {
		if seek[i] != until[i] {
			break
		}
		prefix = append(prefix, seek[i])
	}
	return prefix
}

// FirstMessage returns the smallest possible key given the queue.
func FirstMessage(queue string) QueueKey {
	return NewQueueKeyForMessage(queue, minStringEncoded)
}

// LastMessage returns the largest possible key given the queue.
func LastMessage(queue string) QueueKey {
	return NewQueueKeyForMessage(queue, maxStringEncoded)
}

// ---------------------------------------------------------------------------

// // RawMessageQueueKey represents a lexicographically sorted key
// type RawMessageQueueKey []byte

// func ksuidLen() int {
// 	return len(ksuid.Max)
// }

// func New(prefix []byte) (RawMessageQueueKey, error) {
// 	return NewWithTime(prefix, time.Now())
// }

// func NewWithTime(prefix []byte, t time.Time) (RawMessageQueueKey, error) {
// 	k, err := ksuid.NewRandomWithTime(t)
// 	if err != nil {
// 		return RawMessageQueueKey{}, err
// 	}
// 	return FromParts(prefix, k), nil
// }

// func FromParts(prefix []byte, k ksuid.KSUID) RawMessageQueueKey {
// 	pl := len(prefix)
// 	key := make([]byte, 0, pl+ksuidLen())
// 	key = append(key, prefix...)
// 	copy(key[pl:], k.Bytes())
// 	return key
// }

// func FromBytes(key []byte) RawMessageQueueKey {
// 	return RawMessageQueueKey(key)
// }

// // First returns the smallest possible key given the prefix.
// func First(prefix []byte) RawMessageQueueKey {
// 	k := make([]byte, len(prefix)+ksuidLen())
// 	copy(k, []byte(prefix))
// 	return k
// }

// // Last returns the largest possible key given the prefix.
// func Last(prefix []byte) RawMessageQueueKey {
// 	return FromParts(prefix, ksuid.Max)
// }

// func (k RawMessageQueueKey) Bytes() []byte {
// 	return k[:]
// }

// func (k RawMessageQueueKey) Prefix() []byte {
// 	// KSUID is at the end, so prefix is everything before that.
// 	return k[:len(k)-ksuidLen()]
// }

// func (k RawMessageQueueKey) KSUID() ksuid.KSUID {
// 	// KSUID is at the end after the prefix.
// 	id, err := ksuid.FromBytes(k[len(k)-ksuidLen():])
// 	if err != nil {
// 		panic(err)
// 	}
// 	return id
// }

// // Next returns the next RawMessageQueueKey after k. The prefix remains the same.
// func (k RawMessageQueueKey) Next() RawMessageQueueKey {
// 	return FromParts(k.Prefix(), k.KSUID().Next())
// }

// // Next returns the previous RawMessageQueueKey before k. The prefix remains the same.
// func (k RawMessageQueueKey) Prev() RawMessageQueueKey {
// 	return FromParts(k.Prefix(), k.KSUID().Prev())
// }

// func (k RawMessageQueueKey) Clone() RawMessageQueueKey {
// 	k2 := make([]byte, len(k))
// 	copy(k2[:], k[:])
// 	return k2
// }

// func (k RawMessageQueueKey) String() string {
// 	return string(k.Bytes())
// }
