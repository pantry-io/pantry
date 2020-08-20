package sequence

import (
	"sync"

	"github.com/segmentio/ksuid"
)

type Sequence struct {
	mu  sync.Mutex
	seq ksuid.Sequence
}

func NewSequence() *Sequence {
	return &Sequence{
		seq: ksuid.Sequence{
			Seed: ksuid.New(),
		},
	}
}

// This must be called with lock acquired.
// It returns the next element in the sequence.
func (s *Sequence) resetSeq() ksuid.KSUID {
	s.seq = ksuid.Sequence{
		Seed: ksuid.New(),
	}
	k, _ := s.seq.Next()
	return k
}

// Next returns the next KSUID in a sequence. If the KSUID sequence is
// exhausted, it will automatically create a new KSUID sequence.
func (s *Sequence) Next() ksuid.KSUID {
	s.mu.Lock()
	k, err := s.seq.Next()
	if err != nil {
		return s.resetSeq()
	}
	s.mu.Unlock()
	return k
}
