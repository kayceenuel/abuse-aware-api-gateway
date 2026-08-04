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
	windowSize time.Duration
	maxRequests int
}

func NewRateLimiter(client *redis.Client, bucketSize int, refillRate int, windowSize time.Duration, maxRequests int) *RateLimiter {
	return &RateLimiter{
		client: client,
		bucketSize: bucketSize,
		refillRate: refillRate, 
		windowSize: windowSize,
		maxRequests: maxRequests,
	}
}

