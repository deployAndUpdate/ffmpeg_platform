package queue

import (
	"context"
	"encoding/json"
	"errors"
)

// Target selects the RabbitMQ routing destination.
type Target string

const (
	TargetMain  Target = "main"
	TargetRetry Target = "retry"
)

// Message is the dispatch payload published to RabbitMQ.
type Message struct {
	JobID   string `json:"job_id"`
	Attempt int    `json:"attempt"`
}

// Publisher sends job dispatch messages.
type Publisher interface {
	Ping(ctx context.Context) error
	Publish(ctx context.Context, target Target, msg Message) error
	Close() error
}

// Consumer receives dispatch messages (used by workers and tests).
type Consumer interface {
	Consume(ctx context.Context, handler func(ctx context.Context, msg Message) error) error
	Close() error
}

// EncodeMessage serializes a dispatch message.
func EncodeMessage(msg Message) ([]byte, error) {
	return json.Marshal(msg)
}

// DecodeMessage parses a dispatch message body.
func DecodeMessage(body []byte) (Message, error) {
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return Message{}, err
	}
	if msg.JobID == "" {
		return Message{}, errors.New("job_id is required")
	}
	return msg, nil
}
