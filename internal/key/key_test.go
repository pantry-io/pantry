package key

import (
	"testing"
	"time"

	"github.com/segmentio/ksuid"
)

func TestNew(t *testing.T) {
	t := time.Now()
	key := NewWithTime("foobar.", t)
	ksuid.FromParts()
}
