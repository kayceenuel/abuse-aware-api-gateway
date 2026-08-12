package kafka

import (
	"context"
	"encoding/json"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

// Start connects to Kafka and processes incoming request events in a loop.
// It runs indefinitely and should be called in a separate goroutine.
func Start(brokerAddress, topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		GroupID: "gateway-consumers",
		Topic:   topic,
	})
	defer r.Close()

	log.Printf("consumer started — broker: %s, topic: %s", brokerAddress, topic)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("consumer: failed to read message: %v", err)
			continue
		}

		var event RequestEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("consumer: failed to deserialize event from key %s: %v", string(m.Key), err)
			continue
		}

		// TODO: pass event to risk scorer
	}
}