package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// RiskScorer detects abuse patterns and tightens limits for offending IPs.
type RiskScorer struct {
	client            *redis.Client // to read & write counters
	stuffingThreshold int           // uique usernames per IP before flagging credential stuffing 
	scrapingThreshold int           // search requests per IP before flagging scraping
}

// NewRiskScorer creates a RiskScorer with a given Redis client and thresholds.
func NewRiskScorer(client *redis.Client, stuffingThreshold, scrapringThreshold int) *RiskScorer {
	return &RiskScorer{
		client:            client,
		stuffingThreshold: stuffingThreshold,
		scrapingThreshold: scrapringThreshold,
	}
}

func (rs *RiskScorer) Score(event RequestEvent) error {
	stuffingKey := "stuffing:" + event.IPAddress
	scrapingKey := "scraping:" + event.IPAddress

	// Credential stuffing - tracking unique username attempted from the IP.
	if event.EndPoint == "/login" {
		if err := rs.client.SAdd(ctx, stuffingkey, event.Username).Err(); err != nil {
			return fmt.Errof("risk scorer: sadd failed: %w", err)
		}
		count, err := rs.client.SCard(ctx, stuffingKey).Result()
		if err != nil {
			return fmt.Errorf("risk scorer: scard failed: %w", err)
		}
	}
}

