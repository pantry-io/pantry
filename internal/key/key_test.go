package key

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	t1 := time.Unix(1, 100)
	// Tests that we only have second precision.
	t2 := t1.Add(1 * time.Nanosecond)
	k1 := New(t1)
	k2 := New(t2)
	k3 := New(t1)

	fmt.Printf("%s k1\n", k1)
	fmt.Printf("%s k2\n", k2)
	fmt.Printf("%s k3\n", k3)

	assert.NotEqual(t, k1, k2, "should not be equal")
	assert.Equal(t, -1, bytes.Compare(k1, k2), "k1 should be less than k2")

	assert.NotEqual(t, k1, k3, "should not be equal")
	assert.Equal(t, -1, bytes.Compare(k1, k3), "k1 should be less than k3")

	assert.Equal(t, -1, bytes.Compare(k2, k3), "k2 should be less than k3")
}

func TestCompate(t *testing.T) {
	t1 := time.Unix(1, 100)
	t2 := t1.Add(1 * time.Second)
	k1 := New(t1)
	k2 := New(t2)

	assert.Equal(t, -1, Compare(k1, k2), "k1 should be less than k2")
	assert.Equal(t, 1, Compare(k2, k1), "k2 should be greater than k1")
	assert.Equal(t, 0, Compare(k1, k1), "k1 should be equal to k1")
	assert.Equal(t, 0, Compare(k2, k2), "k2 should be equal to k2")
}

func TestString64(t *testing.T) {
	t1 := time.Unix(1, 100)
	// Tests that we only have second precision.
	t2 := t1.Add(1 * time.Nanosecond)
	k1 := New(t1)
	k2 := New(t2)
	k3 := New(t1)

	fmt.Printf("%s k1\n", k1)
	fmt.Printf("%s k2\n", k2)
	fmt.Printf("%s k3\n", k3)

	assert.NotEqual(t, k1, k2, "should not be equal")
	assert.Equal(t, -1, bytes.Compare(k1, k2), "k1 should be less than k2")

	assert.NotEqual(t, k1, k3, "should not be equal")
	assert.Equal(t, -1, bytes.Compare(k1, k3), "k1 should be less than k3")

	assert.Equal(t, -1, bytes.Compare(k2, k3), "k2 should be less than k3")
}
