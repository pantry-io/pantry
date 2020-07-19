package key_test

import (
	"bytes"
	"testing"

	"github.com/nickpoorman/nats-requeue/internal/queues"
	"github.com/segmentio/ksuid"
)

func Test_Bytes(t *testing.T) {
	uid := ksuid.New()
	k1 := queues.NewQueueKeyForMessage("high", uid.String())

	k2 := queues.FromParts([]byte(k1.NamePath()), uid)

	if bytes.Compare(k1.Bytes(), k2.Bytes()) != 0 {
		t.Errorf("k1 != k2 | k1=%+v k2=%+v", k1.Bytes(), k2.Bytes())
	}
}
