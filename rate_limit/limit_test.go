package rate_limit

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestAllowSlidingWindow(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	rl := NewRateLimiter(
		client,
		10,          // token bucket size
		1,           // refill rate
		time.Minute, // sliding window
		5,           // maximum requests
	)

	ip := "test-ip"

	// Start with a clean Redis key.
	client.Del(ctx, "sliding_window:"+ip)

	for i := 1; i <= 5; i++ {
		allowed, err := rl.AllowSlidingWindow(ip)
		if err != nil {
			t.Fatal(err)
		}

		if !allowed {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	allowed, err := rl.AllowSlidingWindow(ip)
	if err != nil {
		t.Fatal(err)
	}

	if allowed {
		t.Fatal("6th request should be rejected")
	}
}
