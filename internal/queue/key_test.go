package queue

import (
	"bytes"
	"testing"
	"time"

	"github.com/nickpoorman/nats-requeue/internal/key"
	"github.com/stretchr/testify/assert"
)

func TestBytesSortable(t *testing.T) {
	queueName := "testqueue"
	t1 := time.Unix(1, 100)
	k1 := key.New(t1)
	k2 := key.New(t1)
	qk1 := NewQueueKeyForMessage(queueName, k1)
	qk2 := NewQueueKeyForMessage(queueName, k2)

	assert.Equal(t, -1, bytes.Compare(qk1.Bytes(), qk2.Bytes()), "qk1 should be less than qk2")
}

func TestParseQueueKey(t *testing.T) {
	// Create a key
	queueName := "testqueue"
	t1 := time.Unix(1, 100)
	k1 := key.New(t1)
	qk1 := NewQueueKeyForMessage(queueName, k1)

	qk1.PropertyPath()

	// Get bytes
	by := qk1.Bytes()

	// Parse the bytes
	qk2 := ParseQueueKey(by)

	assert.Equal(t, QueuesNamespace, qk2.Namespace, "qk2 namespace should be correct")
	assert.Equal(t, MessagesBucket, qk2.Bucket, "qk2 namespace should be correct")
	assert.Equal(t, queueName, qk2.Name, "qk2 name should be testqueue")
	assert.Equal(t, k1, qk2.Key, "qk2 name should be testqueue")
}

func TestNewQueueKeyForMessage(t *testing.T) {
	queueName := "testqueue"
	qk := NewQueueKeyForMessage(queueName, key.Min)
	want := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    MessagesBucket,
		Name:      queueName,
		Key:       key.Min,
	}
	assert.Equal(t, want, qk)
	assert.Equal(t, string(want.Bytes()), string(qk.Bytes()))
}

func TestPrefixOf(t *testing.T) {
	want := "_q._m.testqueue."
	queueName := "testqueue"
	qk1 := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    MessagesBucket,
		Name:      queueName,
		Key:       key.Min,
	}
	qk2 := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    MessagesBucket,
		Name:      queueName,
		Key:       key.Max,
	}
	prefix := PrefixOf(qk1.Bytes(), qk2.Bytes())
	assert.Equal(t, want, string(prefix))
}
