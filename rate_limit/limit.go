package rate_limit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// Lua script for sliding window rate limiting.
//
// KEYS[1] = Redis sorted-set key for this IP
//
// ARGV[1] = current timestamp in milliseconds
// ARGV[2] = window size in milliseconds
// ARGV[3] = maximum requests allowed in the window
// ARGV[4] = unique request ID
var slidingWindowScript = redis.NewScript(`
-- Remove requests older than the current window.
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1] - ARGV[2])

-- Count requests still inside the window.
local count = redis.call("ZCARD", KEYS[1])

-- Reject if the limit has already been reached.
if count >= tonumber(ARGV[3]) then
    return {0, count}
end

-- Add this request using its timestamp as the score
-- and a unique ID as the member.
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[4])

-- Keep the key around slightly longer than the window.
redis.call("PEXPIRE", KEYS[1], ARGV[2] * 2)

return {1, count + 1}
`)

func newRequestID() (string, error) {
	b := make([]byte, 8)

	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}

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

// AllowSlidingWindow checks if a request is allowed based on the sliding window.
func (rl *RateLimiter) AllowSlidingWindow(ip string) (bool, error) {
	slidingKey := "sliding_window:" + ip
	now := time.Now().UnixMilli()

	requestID, err := newRequestID()
	if err != nil {
		return false, err
	}

	result, err := slidingWindowScript.Run(
		ctx,
		rl.client,
		[]string{slidingKey},
		now,
		rl.windowSize.Milliseconds(),
		rl.maxRequests,
		requestID,
	).Result()

	if err != nil {
		return false, err
	}

	values := result.([]interface{})
	allowed := values[0].(int64) == 1

	return allowed, nil
}
