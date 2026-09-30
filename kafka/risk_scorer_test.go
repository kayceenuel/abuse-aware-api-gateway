package kafka

import (
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRiskScorerCredentialStuffing(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	defer client.Close()

	scorer := NewRiskScorer(client)
	ip := "test-stuffing"

	// Start with clean Redis keys.
	client.Del(
		ctx,
		"stuffing:"+ip,
		"risk:"+ip,
		"limit:"+ip,
	)

	// Five unique usernames should add 2 points each.
	for i := 1; i <= 5; i++ {
		err := scorer.Score(RequestEvent{
			IPAddress: ip,
			Endpoint:  "/login",
			Username:  fmt.Sprintf("user%d", i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Five unique usernames × 2 points = 10.
	score, err := client.Get(ctx, "risk:"+ip).Int64()
	if err != nil {
		t.Fatal(err)
	}

	if score != 10 {
		t.Fatalf("expected risk score 10, got %d", score)
	}

	// A score of 10 should create the limit key.
	exists, err := client.Exists(ctx, "limit:"+ip).Result()
	if err != nil {
		t.Fatal(err)
	}

	if exists == 0 {
		t.Fatal("expected limit key to exist")
	}

	// Clean up Redis keys.
	client.Del(
		ctx,
		"stuffing:"+ip,
		"risk:"+ip,
		"limit:"+ip,
	)
}

func TestDuplicateUsername(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	defer client.Close()

	scorer := NewRiskScorer(client)
	ip := "test-duplicate"

	// Start with clean Redis keys.
	client.Del(
		ctx,
		"stuffing:"+ip,
		"risk:"+ip,
		"limit:"+ip,
	)

	// Send the same username twice.
	for i := 0; i < 2; i++ {
		err := scorer.Score(RequestEvent{
			IPAddress: ip,
			Endpoint:  "/login",
			Username:  "user1",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// The same username should only add 2 points once.
	score, err := client.Get(ctx, "risk:"+ip).Int64()
	if err != nil {
		t.Fatal(err)
	}

	if score != 2 {
		t.Fatalf("expected risk score 2, got %d", score)
	}

	// Clean up Redis keys.
	client.Del(
		ctx,
		"stuffing:"+ip,
		"risk:"+ip,
		"limit:"+ip,
	)
}

func TestScrapingPoints(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	defer client.Close()

	scorer := NewRiskScorer(client)
	ip := "test-scraping"

	// Start with clean Redis keys.
	client.Del(
		ctx,
		"stuffing:"+ip,
		"risk:"+ip,
		"limit:"+ip,
	)

	// Ten search requests should add 1 point each.
	for i := 0; i < 10; i++ {
		err := scorer.Score(RequestEvent{
			IPAddress: ip,
			Endpoint:  "/search",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Ten search requests × 1 point = 10.
	score, err := client.Get(ctx, "risk:"+ip).Int64()
	if err != nil {
		t.Fatal(err)
	}

	if score != 10 {
		t.Fatalf("expected risk score 10, got %d", score)
	}

	// A score of 10 should create the limit key.
	exists, err := client.Exists(ctx, "limit:"+ip).Result()
	if err != nil {
		t.Fatal(err)
	}

	if exists == 0 {
		t.Fatal("expected limit key to exist")
	}

	// Clean up Redis keys.
	client.Del(
		ctx,
		"stuffing:"+ip,
		"risk:"+ip,
		"limit:"+ip,
	)
}
