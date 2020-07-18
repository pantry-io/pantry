package key

import (
	"encoding/binary"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/twmb/murmur3"
)

const ksuidSize = 20
const prefixSize = 16
const size = ksuidSize + prefixSize

// Key represents a lexicographically sorted key
type Key [size]byte

func New(prefix string) (Key, error) {
	return NewWithTime(prefix, time.Now())
}

func NewWithTime(prefix string, t time.Time) (Key, error) {
	kid, err := ksuid.NewRandomWithTime(t)
	if err != nil {
		return Key{}, err
	}
	k := Key{}
	binary.BigEndian.PutUint64(k[0:8], murmur3.StringSum64(prefix))
	copy(k[8:], kid.Bytes())
	return k, nil
}

// PrefixOf a common prefix between two keys (common leading bytes) which is
// then used as a prefix for Badger to narrow down SSTables to traverse.
func PrefixOf(seek, until Key) []byte {
	var prefix []byte

	// Iterate through the bytes and append common ones
	for i := 0; i < size; i++ {
		if seek[i] != until[i] {
			break
		}
		prefix = append(prefix, seek[i])
	}
	return prefix
}

// First returns the smallest possible key given the prefix.
func First(prefix string) Key {
	// k := make([]byte, len(prefix)+ksuidSize)
	k := Key{}
	binary.BigEndian.PutUint64(k[0:8], murmur3.StringSum64(prefix))
	return k
}

// Last returns the largest possible key given the prefix.
func Last(prefix string) Key {
	k := Key{}
	binary.BigEndian.PutUint64(k[0:8], murmur3.StringSum64(prefix))
	copy(k[8:], ksuid.Max.Bytes())
	return k
}

func (k Key) Bytes() []byte {
	return k[:]
}
