package key_test

import (
	"testing"

	"github.com/segmentio/ksuid"
)

func Test_Bytes(t *testing.T) {
	uid := ksuid.New()
	k1 := NewQueueKeyForMessage("high", uid)
	
}
