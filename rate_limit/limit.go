package rate_limit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// The entire token-bucket operation runs atomically inside Redis.
//
// KEYS[1] = token count
// KEYS[2] = last refill timestamp
//
// ARGV[1] = bucket size
// ARGV[2] = refill rate (tokens/second)
// ARGV[3] = current Unix timestamp
var tokenBucketScript = redis.NewScript(`
local tokens = redis.call("GET", KEYS[1])
local lastRefill = redis.call("GET", KEYS[2])

local bucketSize = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

-- First request: start with a full bucket.
if not tokens then
	tokens = bucketSize
	lastRefill = now
end

tokens = tonumber(tokens)
lastRefill = tonumber(lastRefill)

-- Refill only for time that has actually elapsed.
local elapsed = now - lastRefill

if elapsed > 0 then
	local refilled = elapsed * refillRate
	tokens = math.min(tokens + refilled, bucketSize)
	lastRefill = now
end

-- Consume one token if available.
if tokens > 0 then
	tokens = tokens - 1

	redis.call("SET", KEYS[1], tokens)
	redis.call("SET", KEYS[2], lastRefill)

	return 1
end

-- No tokens available.
redis.call("SET", KEYS[1], tokens)
redis.call("SET", KEYS[2], lastRefill)

return 0
`)

type RateLimiter struct {
	client *redis.Client

	// Token bucket configuration.
	bucketSize int
	refillRate int

	// Sliding window configuration.
	windowSize  time.Duration
	maxRequests int
}

// NewRateLimiter creates a new RateLimiter with the given configuration.
func NewRateLimiter(
	client *redis.Client,
	bucketSize int,
	refillRate int,
	windowSize time.Duration,
	maxRequests int,
) *RateLimiter {
	return &RateLimiter{
		client:      client,
		bucketSize:  bucketSize,
		refillRate:  refillRate,
		windowSize:  windowSize,
		maxRequests: maxRequests,
	}
}

// AllowTokenBucket checks whether a request is allowed by the token bucket.
//
// The entire operation is executed atomically inside Redis.
func (rl *RateLimiter) AllowTokenBucket(apiKey string) (bool, error) {
	tokensKey := "token_bucket:" + apiKey + ":tokens"
	lastRefillKey := "token_bucket:" + apiKey + ":last_refill"

	// Redis runs the script atomically, so concurrent requests
	// cannot read the same token count and both consume it.
	result, err := tokenBucketScript.Run(
		ctx,
		rl.client,
		[]string{tokensKey, lastRefillKey},
		rl.bucketSize,
		rl.refillRate,
		time.Now().Unix(),
	).Int()

	if err != nil {
		return false, err
	}

	return result == 1, nil
}

// min returns the smaller of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

// AllowSlidingWindow checks if a request is allowed based on the sliding window.
func (rl *RateLimiter) AllowSlidingWindow(ip string) (bool, error) {
	slidingKey := "sliding_window:" + ip
	now := time.Now().UnixMilli()

	// Use a Redis transaction to execute the cleanup and count together.
	pipe := rl.client.TxPipeline()

	// Remove timestamps older than the current window.
	pipe.ZRemRangeByScore(
		ctx,
		slidingKey,
		"-inf",
		fmt.Sprintf("%d", now-rl.windowSize.Milliseconds()),
	)

	// Count requests currently inside the window.
	countCmd := pipe.ZCard(ctx, slidingKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	// Check the limit before adding the new request.
	if countCmd.Val() >= int64(rl.maxRequests) {
		return false, nil
	}

	// Add the current request.
	rl.client.ZAdd(
		ctx,
		slidingKey,
		redis.Z{
			Score:  float64(now),
			Member: now,
		},
	)

	// Keep the Redis key around slightly longer than the window.
	rl.client.Expire(ctx, slidingKey, rl.windowSize*2)

	return true, nil
}
