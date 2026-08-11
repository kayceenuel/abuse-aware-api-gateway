package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// RequestEvent captures what we need from each request to detect abuse patterns.
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
