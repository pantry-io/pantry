package queue

import (
	"bytes"
	"fmt"
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

	str := qk1.PropertyPath()
	assert.Equal(t, fmt.Sprintf("_q._m.testqueue.%s", qk1.Key.String()), str)

	// Get bytes
	by := qk1.Bytes()

	// Parse the bytes
	qk2 := ParseQueueKey(by)

	assert.Equal(t, QueuesNamespace, qk2.Namespace, "qk2 namespace should be correct")
	assert.Equal(t, MessagesBucket, qk2.Bucket, "qk2 namespace should be correct")
	assert.Equal(t, queueName, qk2.Name, "qk2 name should be testqueue")
	assert.Equal(t, k1, qk2.Key, "qk2 name should be testqueue")
}

func TestParseQueueKeyMin(t *testing.T) {
	queueName := "testqueue"
	k1 := key.Min
	qk1 := NewQueueKeyForMessage(queueName, k1)

	str := qk1.PropertyPath()
	assert.Equal(t, "_q._m.testqueue.0.0.0", str)

	// Get bytes
	by := qk1.Bytes()

	// Parse the bytes
	qk2 := ParseQueueKey(by)

	assert.Equal(t, QueuesNamespace, qk2.Namespace, "qk2 namespace should be correct")
	assert.Equal(t, MessagesBucket, qk2.Bucket, "qk2 namespace should be correct")
	assert.Equal(t, queueName, qk2.Name, "qk2 name should be testqueue")
	assert.Equal(t, k1, qk2.Key, "qk2 name should be testqueue")
}

func TestParseQueueKeyMax(t *testing.T) {
	queueName := "testqueue"
	k1 := key.Max
	qk1 := NewQueueKeyForMessage(queueName, k1)

	str := qk1.PropertyPath()
	assert.Equal(t, "_q._m.testqueue.18446744073709551615.18446744073709551615.18446744073709551615", str)

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
	qk1 := FirstMessage(queueName)
	qk2 := LastMessage(queueName)
	prefix := PrefixOf(qk1.Bytes(), qk2.Bytes())
	assert.Equal(t, want, string(prefix))
}

func TestBucketPath(t *testing.T) {
	want := "_q._s"
	qk := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    StateBucket,
		Name:      "testqueue",
		Property:  "foo",
	}
	assert.Equal(t, want, qk.BucketPath())
}

func TestBucketPrefix(t *testing.T) {
	want := "_q._s."
	qk := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    StateBucket,
		Name:      "testqueue",
		Property:  "foo",
	}
	assert.Equal(t, want, qk.BucketPrefix())
}

func TestNamePath(t *testing.T) {
	want := "_q._s.testqueue"
	qk := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    StateBucket,
		Name:      "testqueue",
		Property:  "foo",
	}
	assert.Equal(t, want, qk.NamePath())
}

func TestNamePrefix(t *testing.T) {
	want := "_q._s.testqueue."
	qk := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    StateBucket,
		Name:      "testqueue",
		Property:  "foo",
	}
	assert.Equal(t, want, qk.NamePrefix())
}

func TestPropertyPath(t *testing.T) {
	want := "_q._s.testqueue.foo"
	qk := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    StateBucket,
		Name:      "testqueue",
		Property:  "foo",
	}
	assert.Equal(t, want, qk.PropertyPath())
}

func TestPropertyPrefix(t *testing.T) {
	want := "_q._s.testqueue.foo."
	qk := QueueKey{
		Namespace: QueuesNamespace,
		Bucket:    StateBucket,
		Name:      "testqueue",
		Property:  "foo",
	}
	assert.Equal(t, want, qk.PropertyPrefix())
}
