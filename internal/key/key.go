package key

import (
	"time"

	"github.com/segmentio/ksuid"
)

// Key represents a lexicographically sorted key
type Key []byte

func ksuidLen() int {
	return len(ksuid.Max)
}

func New(prefix []byte) (Key, error) {
	return NewWithTime(prefix, time.Now())
}

func NewWithTime(prefix []byte, t time.Time) (Key, error) {
	k, err := ksuid.NewRandomWithTime(t)
	if err != nil {
		return Key{}, err
	}
	return FromParts(prefix, k), nil
}

func FromParts(prefix []byte, k ksuid.KSUID) Key {
	pl := len(prefix)
	key := make([]byte, 0, pl+ksuidLen())
	key = append(key, prefix...)
	copy(key[pl:], k.Bytes())
	return key
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

	// Iterate through the bytes and append common ones
	for i := 0; i < length; i++ {
		if seek[i] != until[i] {
			break
		}
		prefix = append(prefix, seek[i])
	}
	return prefix
}

// First returns the smallest possible key given the prefix.
func First(prefix []byte) Key {
	k := make([]byte, len(prefix)+ksuidLen())
	copy(k, []byte(prefix))
	return k
}

// Last returns the largest possible key given the prefix.
func Last(prefix []byte) Key {
	return FromParts(prefix, ksuid.Max)
}

func (k Key) Bytes() []byte {
	return k[:]
}

func (k Key) Prefix() []byte {
	// KSUID is at the end, so prefix is everything before that.
	return k[:len(k)-ksuidLen()]
}

func (k Key) KSUID() ksuid.KSUID {
	// KSUID is at the end after the prefix.
	id, err := ksuid.FromBytes(k[len(k)-ksuidLen():])
	if err != nil {
		panic(err)
	}
	return id
}

// Next returns the next Key after k. The prefix remains the same.
func (k Key) Next() Key {
	return FromParts(k.Prefix(), k.KSUID().Next())
}

// Next returns the previous Key before k. The prefix remains the same.
func (k Key) Prev() Key {
	return FromParts(k.Prefix(), k.KSUID().Prev())
}

func (k Key) Clone() Key {
	k2 := make([]byte, len(k))
	copy(k2[:], k[:])
	return k2
}
