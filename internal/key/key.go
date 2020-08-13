package key

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

const Size = 24

// Key represents a lexicographically sorted key.
type Key []byte

var (
	// seq is used as a sequence number.
	// It is okay if this overflows if you are producing
	// less than 18446744073709551615 keys per second.
	seq uint64

	// instanceID is a unique instance id. This may be used
	// so that keys are unique across instances.
	instanceID = newUID()

	Min = Key{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	Max = Key{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
)

func newUID() uint64 {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(b)
}

// New generates a new key.
func New(time time.Time) Key {
	out := make([]byte, Size)
	binary.BigEndian.PutUint64(out[0:8], uint64(time.Unix()))
	binary.BigEndian.PutUint64(out[8:16], atomic.AddUint64(&seq, 1))
	binary.BigEndian.PutUint64(out[16:24], instanceID)
	return out
}

// Clone returns a close of a key.
// func Clone(k Key) Key {
// 	b := make(Key, 16)
// 	copy(b, k)
// 	return b[:len(k)]
// }

// Compare returns an integer comparing two keys lexicographically.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
// A nil argument is equivalent to an empty slice.
func Compare(a, b Key) int {
	return bytes.Compare(a, b)
}

// PrefixOf a common prefix between two keys (common leading bytes) which is
// then used as a prefix for Badger to narrow down SSTables to traverse.
func PrefixOf(seek, until Key) []byte {
	var prefix []byte

	// Calculate the minimum length
	length := len(seek)
	if len(until) < length {
		length = len(until)
	}

	// Iterate through the bytes and append common ones.
	for i := 0; i < length; i++ {
		if seek[i] != until[i] {
			break
		}
		prefix = append(prefix, seek[i])
	}
	return prefix
}

// First returns the smallest possible key.
func First() Key {
	return make([]byte, Size)
}

// Last returns the largest possible key.
func Last() Key {
	out := make([]byte, Size)
	for i := 0; i < Size; i++ {
		out[i] = math.MaxUint8
	}
	return out
}

func (k Key) UnixTimestamp() uint64 {
	return binary.BigEndian.Uint64(k[0:8])
}

func (k Key) Seq() uint64 {
	return binary.BigEndian.Uint64(k[8:16])
}

func (k Key) InstanceID() uint64 {
	return binary.BigEndian.Uint64(k[16:24])
}

// Do not try to sort the string representation.
func (k Key) String() string {
	return base64.StdEncoding.EncodeToString(k)
}

func (k Key) Print() string {
	return fmt.Sprintf(
		"%d.%d.%d",
		k.UnixTimestamp(),
		k.Seq(),
		k.InstanceID(),
	)
}

func (k Key) Bytes() []byte {
	return k
}
