package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	// kafka-go is aliased to avoid conflict with this package's own name.
	kafka "github.com/segmentio/kafka-go"
)

// RequestEvent captures what we need from each request to detect abuse patterns downstream.
type RequestEvent struct {
	IPAddress string    `json:"ip_address"`
	Endpoint  string    `json:"endpoint"`
	APIKey    string    `json:"api_key"`
	Timestamp time.Time `json:"timestamp"`
	Allowed   bool      `json:"allowed"`
}

// NewProducer creates a Kafka writer connected to the given broker and topic.
func NewProducer(brokerAddress, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

// Log serializes a request event and writes it to Kafka.
// The IP address is used as the message key so events from the same IP
// are routed to the same partition — preserving order for risk analysis.
func Log(producer *kafka.Writer, event RequestEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("kafka: failed to serialize event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.IPAddress),
		Value: data,
	}

	if err := producer.WriteMessages(context.Background(), msg); err != nil {
		return fmt.Errorf("kafka: failed to write message: %w", err)
	}

	return nil
}
