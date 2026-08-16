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

// Score analyses a request event and tightens Redis limits if abuse is detected.
func (rs *RiskScorer) Score(event RequestEvent) error {
	stuffingKey := "stuffing:" + event.IPAddress
	scrapingKey := "scraping:" + event.IPAddress

	// Credential stuffing — track unique usernames attempted from this IP.
	if event.Endpoint == "/login" {
		if err := rs.client.SAdd(ctx, stuffingKey, event.Username).Err(); err != nil {
			return fmt.Errorf("risk scorer: sadd failed: %w", err)
		}
		count, err := rs.client.SCard(ctx, stuffingKey).Result()
		if err != nil {
			return fmt.Errorf("risk scorer: scard failed: %w", err)
		}
		if count > int64(rs.stuffingThreshold) {
			rs.client.Set(ctx, "limit:"+event.IPAddress, 1, time.Hour)
		}
	}

	// Scraping — track total search requests from this IP.
	if event.Endpoint == "/search" {
		count, err := rs.client.Incr(ctx, scrapingKey).Result()
		if err != nil {
			return fmt.Errorf("risk scorer: incr failed: %w", err)
		}
		if count > int64(rs.scrapingThreshold) {
			rs.client.Set(ctx, "limit:"+event.IPAddress, 1, time.Hour)
		}
	}

	return nil
}
