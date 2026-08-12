package kafka

import (
	"context"
	"encoding/json"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func Start(brokerAddress, topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		GroupID: "gateway_consumer",
		Topic:   topic,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("error reading message: %v", err)
			continue
		}

		var event RequestEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("failed to deserialize event: %v", err)
			continue
		}
		// TODO: pass event to risk scorer
	}
}
