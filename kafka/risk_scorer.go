package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

const (
	detectionWindow          = 10 * time.Minute
	credentialStuffingPoints = 2
	scrapingPoints           = 1
	riskThreshold            = 10
)

// RiskScorer detects abuse patterns and tightens Redis limits for risky IPs.
type RiskScorer struct {
	client *redis.Client
}

// NewRiskScorer creates a RiskScorer with a Redis client.
func NewRiskScorer(client *redis.Client) *RiskScorer {
	return &RiskScorer{
		client: client,
	}
}

// Score analyses a request event and updates the risk score for its IP.
func (rs *RiskScorer) Score(event RequestEvent) error {
	stuffingKey := "stuffing:" + event.IPAddress
	scrapingKey := "scraping:" + event.IPAddress
	riskKey := "risk:" + event.IPAddress

	var riskScore int64

	// Credential stuffing — only a new username increases the risk score.
	if event.Endpoint == "/login" {
		added, err := rs.client.SAdd(ctx, stuffingKey, event.Username).Result()
		if err != nil {
			return fmt.Errorf("risk scorer: sadd failed: %w", err)
		}

		if err := rs.client.Expire(ctx, stuffingKey, detectionWindow).Err(); err != nil {
			return fmt.Errorf("risk scorer: expire stuffing key failed: %w", err)
		}

		if added == 1 {
			riskScore, err = rs.client.IncrBy(ctx, riskKey, credentialStuffingPoints).Result()
			if err != nil {
				return fmt.Errorf("risk scorer: increment risk score failed: %w", err)
			}
		}
	}

	// Scraping — every search request increases the risk score.
	if event.Endpoint == "/search" {
		_, err := rs.client.Incr(ctx, scrapingKey).Result()
		if err != nil {
			return fmt.Errorf("risk scorer: incr failed: %w", err)
		}

		if err := rs.client.Expire(ctx, scrapingKey, detectionWindow).Err(); err != nil {
			return fmt.Errorf("risk scorer: expire scraping key failed: %w", err)
		}

		riskScore, err = rs.client.IncrBy(ctx, riskKey, scrapingPoints).Result()
		if err != nil {
			return fmt.Errorf("risk scorer: increment risk score failed: %w", err)
		}
	}

	if riskScore > 0 {
		if err := rs.client.Expire(ctx, riskKey, detectionWindow).Err(); err != nil {
			return fmt.Errorf("risk scorer: expire risk key failed: %w", err)
		}
	}

	if riskScore >= riskThreshold {
		if err := rs.client.Set(ctx, "limit:"+event.IPAddress, 1, time.Hour).Err(); err != nil {
			return fmt.Errorf("risk scorer: set limit failed: %w", err)
		}
	}

	return nil
}
