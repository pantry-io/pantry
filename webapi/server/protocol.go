package server

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nickpoorman/nats-requeue/internal/sequence"
	"github.com/nickpoorman/nats-requeue/protocol"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/gjson"
)

type Command int

const (
	InvalidCommand Command = iota
	StatsSubscribe
	StatsMessage
)

var (
	InvalidJSONError            = errors.New("invalid json")
	InvalidCommandError         = errors.New("invalid command")
	InvalidProtocolMessageError = errors.New("invalid protocol message")
)

type ProtocolMessage struct {
	c   *Client
	msg []byte
}

func NewProtocolMessage(c *Client, msg []byte) ProtocolMessage {
	return ProtocolMessage{c: c, msg: msg}
}

func (p ProtocolMessage) Command() (Command, error) {
	// The message must be valid JSON.
	if !gjson.ValidBytes(p.msg) {
		return InvalidCommand, InvalidJSONError
	}
	result := gjson.GetBytes(p.msg, "c")

	// All messages must have a command.
	if !result.Exists() {
		return InvalidCommand, InvalidProtocolMessageError
	}
	if result.Type != gjson.Number {
		return InvalidCommand, InvalidProtocolMessageError
	}

	return Command(result.Int()), nil
}

func ProtocolMessageHandler(pm ProtocolMessage) error {
	cmd, err := pm.Command()
	if err != nil {
		return err
	}
	switch cmd {
	case InvalidCommand:
		return InvalidCommandError
	case StatsSubscribe:
		return handleStatsSubscribe(pm)
	}
	return nil
}

type StatsSubscribeMessage struct {
}

func handleStatsSubscribe(pm ProtocolMessage) error {
	var m StatsSubscribeMessage
	if err := json.Unmarshal(pm.msg, &m); err != nil {
		return fmt.Errorf("problem decoding json: %w", err)
	}

	// TODO: Authorize the user has access to this topic. Are they an admin?

	// Subscribe to the NATS topic.
	if err := pm.c.statsSubscribe(func(msg *nats.Msg) {
		// Any messages we get, we'll forward onto our websocket.
		ism := &protocol.InstanceStatsMessage{}
		if err := ism.UnmarshalBinary(msg.Data); err != nil {
			// Log out that there was an error with the stats message.
			log.Err(err).Msg("problem decoding the protocol stats message")
			return
		}
		out := NewStatsMessageEgress(*ism)
		payload, err := json.Marshal(out)
		if err != nil {
			log.Err(err).Msg("problem encoding the stats message")
			return
		}

		// Send the payload
		pm.c.send <- payload

	}); err != nil {
		return fmt.Errorf("problem subscribing to NATS topic: %w", err)
	}

	return nil
}

var uniqueEgressSeq = sequence.NewSequence()

type EgressMessage struct {
	Key ksuid.KSUID `json:"seq"`
}

func NewEgressMessage() EgressMessage {
	return EgressMessage{
		Key: uniqueEgressSeq.Next(),
	}
}

type StatsMessageEgress struct {
	EgressMessage
	Command  Command                       `json:"c"`
	Instance protocol.InstanceStatsMessage `json:"instance"`
}

func NewStatsMessageEgress(p protocol.InstanceStatsMessage) StatsMessageEgress {
	return StatsMessageEgress{
		EgressMessage: NewEgressMessage(),
		Command:       StatsMessage,
		Instance:      p,
	}
}
