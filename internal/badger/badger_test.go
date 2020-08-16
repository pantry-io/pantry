package badger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInstanceDir(t *testing.T) {
	dir := "mydir"
	instanceId := "123456"
	path := InstanceDir(dir, instanceId)
	assert.Equal(t, "mydir/123456", path)
}
