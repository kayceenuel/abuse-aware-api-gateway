package rate_limit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background() // context allows you to manage deadlines and handle cancellations for requests.

type RateLimiter struct {
	client *redis.Client

	// token bucket config
	bucketSize int // maximum num of tokens in the bucket.
	refillRate int // The number of tokens to add to the bucket per second.
	// sliding window config
	windowSize  time.Duration // The duration of the sliding window
	maxRequests int
}

// NewRateLimiter creates a new RateLimiter configured instance
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
	// calculate how many tokens to refill based on the elapsed time and refill rate
	elapsed := time.Now().Unix() - lastRefill
	refilled := int(elapsed) * rl.refillRate
	if refilled > 0 {
		tokens = min(tokens+refilled, rl.bucketSize)
		rl.client.Set(ctx, tokensKey, tokens, 0)
		rl.client.Set(ctx, lastRefillKey, time.Now().Unix(), 0)
	}

	// check if there are enough tokens to allow the request
	if tokens > 0 {
		rl.client.Decr(ctx, tokensKey) // decrement token count
		return true, nil
	}
	return false, nil // not enough tokens, request is denied
}

// min returns the smaller of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ALlowSlidingWindow checks if a request is allowed based on the sliding windows
func (rl *RateLimiter) AllowSlidingWindow(ip string) (bool, error) {
	slidingKey := "sliding_window:" + ip
	now := time.Now().UnixMilli() // current time in miliseconds

	// Use a Redis transaction (MULTI/EXEC) for atomicity
	// This ensures that all operations are treated as a sinlge, atomic unit.
	pipe := rl.client.TxPipeline()

	// Remove timestamps older than the current window
	// ZREMRANGEBYSCORE key - inf (now - windowSize)
	pipe.ZRemRangeByScore(ctx, slidingKey, "-inf", fmt.Sprintf("%d", now-rl.windowSize.Milliseconds()))

	// Count BEFORE adding the new request
	countCmd := pipe.ZCard(ctx, slidingKey)

	// Execute to get the count
	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	// Check the limit before adding
	if countCmd.Val() >= int64(rl.maxRequests) {
		return false, nil
	}

	// Only add if allowed
	rl.client.ZAdd(ctx, slidingKey, redis.Z{Score: float64(now), Member: now})
	rl.client.Expire(ctx, slidingKey, rl.windowSize*2)

	return true, nil

}
