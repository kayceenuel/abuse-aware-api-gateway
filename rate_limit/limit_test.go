package rate_limit

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kayceenuel/abuse-aware-api-gateway/kafka"
	"github.com/redis/go-redis/v9"
)

// newTestLimiter starts an in-process Redis and returns a RateLimiter backed by it.
func newTestLimiter(t *testing.T, bucketSize, refillRate int, windowSize time.Duration, maxRequests int) (*RateLimiter, *redis.Client, *miniredis.Miniredis) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	return NewRateLimiter(client, bucketSize, refillRate, windowSize, maxRequests), client, mr
}

func TestAllowSlidingWindowEnforcesLimit(t *testing.T) {
	// refillRate is irrelevant to the sliding window; use 0 for determinism.
	rl, _, _ := newTestLimiter(t, 10, 0, time.Minute, 5)

	ip := "192.0.2.1"

	for i := 1; i <= 5; i++ {
		allowed, err := rl.AllowSlidingWindow(ip)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	allowed, err := rl.AllowSlidingWindow(ip)
	if err != nil {
		t.Fatalf("6th request: %v", err)
	}
	if allowed {
		t.Fatal("6th request should be rejected")
	}
}

func TestAllowTokenBucketEnforcesLimit(t *testing.T) {
	// refillRate 0 makes refill deterministic: the bucket starts full and never refills,
	// so exactly bucketSize requests pass before the bucket is exhausted.
	rl, _, _ := newTestLimiter(t, 5, 0, time.Minute, 100)

	apiKey := "test-key"

	for i := 1; i <= 5; i++ {
		allowed, err := rl.AllowTokenBucket(apiKey)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	allowed, err := rl.AllowTokenBucket(apiKey)
	if err != nil {
		t.Fatalf("6th request: %v", err)
	}
	if allowed {
		t.Fatal("6th request should be rejected")
	}
}

func TestIsRiskyReflectsLimitKey(t *testing.T) {
	rl, _, mr := newTestLimiter(t, 10, 0, time.Minute, 5)

	ip := "198.51.100.7"

	risky, err := rl.IsRisky(ip)
	if err != nil {
		t.Fatalf("IsRisky before flag: %v", err)
	}
	if risky {
		t.Fatal("IP with no limit key should not be risky")
	}

	// The risk scorer flags an IP by writing the "limit:<ip>" key.
	mr.Set("limit:"+ip, "1")

	risky, err = rl.IsRisky(ip)
	if err != nil {
		t.Fatalf("IsRisky after flag: %v", err)
	}
	if !risky {
		t.Fatal("IP with a limit key should be risky")
	}
}

// TestRiskScoreCausesIsRiskyToBlock ties the two packages together: once the risk
// scorer reaches its threshold, IsRisky must start blocking that IP.
func TestRiskScoreCausesIsRiskyToBlock(t *testing.T) {
	rl, client, _ := newTestLimiter(t, 10, 0, time.Minute, 5)

	scorer := kafka.NewRiskScorer(client)
	ip := "198.51.100.9"

	risky, err := rl.IsRisky(ip)
	if err != nil {
		t.Fatalf("IsRisky before scoring: %v", err)
	}
	if risky {
		t.Fatal("IP should not be risky before any risk is scored")
	}

	blocked := false
	for i := 1; i <= 10 && !blocked; i++ {
		event := kafka.RequestEvent{
			IPAddress: ip,
			Endpoint:  "/login",
			Username:  fmt.Sprintf("user%d", i),
		}
		if err := scorer.Score(event); err != nil {
			t.Fatalf("score distinct username %d: %v", i, err)
		}

		risky, err = rl.IsRisky(ip)
		if err != nil {
			t.Fatalf("IsRisky after scoring %d usernames: %v", i, err)
		}
		blocked = risky
	}

	if !blocked {
		t.Fatal("IP never became risky after repeated distinct-username logins")
	}
}
