package rate_limit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background() // context allows you to manage deadlines and handle cancellations for requests.

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

-- First request for this API key.
if not tokens then
	tokens = bucketSize
	lastRefill = now
end

tokens = tonumber(tokens)
lastRefill = tonumber(lastRefill)

-- Refill the bucket based on elapsed time.
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

	// token bucket config
	bucketSize int // maximum num of tokens in the bucket.
	refillRate int // The number of tokens to add to the bucket per second.
	// sliding window config
	windowSize  time.Duration // The duration of the sliding window
	maxRequests int
}

// NewRateLimiter creates a new RateLimiter configured instance
func (rl *RateLimiter) AllowTokenBucket(apiKey string) (bool, error) {
	tokensKey := "token_bucket:" + apiKey + ":tokens"
	lastRefillKey := "token_bucket:" + apiKey + ":last_refill"

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
