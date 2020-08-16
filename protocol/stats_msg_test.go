package protocol

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInstanceStatsMessageMarshalUnmarshalBinary(t *testing.T) {
	queues := make([]QueueStatsMessage, 5)
	for i := range queues {
		queues[i].QueueName = fmt.Sprintf("Q%d", i)
		queues[i].Enqueued = 103
		queues[i].InFlight = 22
	}
	ism := InstanceStatsMessage{
		InstanceId: "Inst1234",
		Queues:     queues,
	}

	// Serialize
	ismBytes, err := ism.MarshalBinary()
	assert.NoError(t, err)

	// Deserialize
	out := &InstanceStatsMessage{}
	assert.NoError(t, out.UnmarshalBinary(ismBytes))

	assert.Equal(t, "Inst1234", out.InstanceId)
	assert.Equal(t, queues, out.Queues)
}
