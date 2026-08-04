package rate_limit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background() // context allows you to manage deadlines and handle cancellations for requests.

type RateLimiter struct {
	client *redis.Client

	// token bucket config
	bucketSize int
	refillRate int
	// sliding window config
	windowSize  time.Duration
	maxRequests int
}

func NewRateLimiter(client *redis.Client, bucketSize int, refillRate int, windowSize time.Duration, maxRequests int) *RateLimiter {
	return &RateLimiter{
		client:      client,
		bucketSize:  bucketSize,
		refillRate:  refillRate,
		windowSize:  windowSize,
		maxRequests: maxRequests,
	}
}

func (rl *RateLimiter) AllowTokenBucket(apiKey string) (bool, error) {
	tokensKey := "token_bucket:" + apiKey + ":tokens"
	lastRefillKey := "token_bucket:" + apiKey + ":last_refill"

	// Get current token count
	tokens, err := rl.client.Get(ctx, tokensKey).Int()
	if err == redis.Nil {
		// new API key - start with a full bucket
		tokens = rl.bucketSize
		rl.client.Set(ctx, tokensKey, tokens, 0)
		rl.client.Set(ctx, lastRefillKey, time.Now().Unix(), 0)
	} else if err != nil {
		return false, err
	}

	// calculate refill based on time elapsed since last refill
	lastRefill, err := rl.client.Get(ctx, lastRefillKey).Int64() // int64 is used to store Unix timestamps
	if err != nil {
		return false, err
	}
	elapsed := time.Now().Unix() - lastRefill
	refilled := int(elapsed) * rl.refillRate
	if refilled > 0 {
		tokens = min(tokens+refilled, rl.bucketSize)
		rl.client.Set(ctx, tokensKey, tokens, 0)
		rl.client.Set(ctx, lastRefillKey, time.Now().Unix(), 0)
	}

	
}
