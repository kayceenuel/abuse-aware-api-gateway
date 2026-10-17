package kafka

import (
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestScorer starts an in-process Redis and returns a RiskScorer backed by it.
func newTestScorer(t *testing.T) (*RiskScorer, *redis.Client) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	return NewRiskScorer(client), client
}

// distinctUsernamesToThreshold is how many unique logins reach the risk threshold.
const distinctUsernamesToThreshold = riskThreshold / credentialStuffingPoints

// searchesToThreshold is how many scrape requests reach the risk threshold.
const searchesToThreshold = riskThreshold / scrapingPoints

func TestRiskScorerDistinctUsernamesRaiseScoreAndTriggerThreshold(t *testing.T) {
	scorer, client := newTestScorer(t)

	ip := "203.0.113.10"
	riskKey := "risk:" + ip
	limitKey := "limit:" + ip

	for i := 1; i <= distinctUsernamesToThreshold; i++ {
		event := RequestEvent{
			IPAddress: ip,
			Endpoint:  "/login",
			Username:  fmt.Sprintf("user%d", i),
		}
		if err := scorer.Score(event); err != nil {
			t.Fatalf("score distinct username %d: %v", i, err)
		}

		score, err := client.Get(ctx, riskKey).Int64()
		if err != nil {
			t.Fatalf("read risk score after %d usernames: %v", i, err)
		}

		want := int64(i * credentialStuffingPoints)
		if score != want {
			t.Fatalf("risk score after %d distinct usernames: got %d, want %d", i, score, want)
		}

		exists, err := client.Exists(ctx, limitKey).Result()
		if err != nil {
			t.Fatalf("check limit key after %d usernames: %v", i, err)
		}

		if i < distinctUsernamesToThreshold && exists != 0 {
			t.Fatalf("limit key set too early, after only %d usernames", i)
		}
		if i == distinctUsernamesToThreshold && exists == 0 {
			t.Fatal("limit key not set after reaching the risk threshold")
		}
	}
}

func TestRiskScorerRepeatedUsernameDoesNotRaiseScore(t *testing.T) {
	scorer, client := newTestScorer(t)

	ip := "203.0.113.11"
	riskKey := "risk:" + ip

	event := RequestEvent{IPAddress: ip, Endpoint: "/login", Username: "repeated"}
	if err := scorer.Score(event); err != nil {
		t.Fatalf("first score: %v", err)
	}
	if err := scorer.Score(event); err != nil {
		t.Fatalf("second score: %v", err)
	}

	score, err := client.Get(ctx, riskKey).Int64()
	if err != nil {
		t.Fatalf("read risk score: %v", err)
	}

	if want := int64(credentialStuffingPoints); score != want {
		t.Fatalf("repeated username changed the score: got %d, want %d", score, want)
	}
}

// TestRiskScorerScrapingAccumulatesScoreAndTriggersThreshold covers the scraping
// path: each /search event adds scrapingPoints until the threshold trips the limit.
func TestRiskScorerScrapingAccumulatesScoreAndTriggersThreshold(t *testing.T) {
	scorer, client := newTestScorer(t)

	ip := "203.0.113.12"
	riskKey := "risk:" + ip
	limitKey := "limit:" + ip

	for i := 1; i <= searchesToThreshold; i++ {
		if err := scorer.Score(RequestEvent{IPAddress: ip, Endpoint: "/search"}); err != nil {
			t.Fatalf("score search %d: %v", i, err)
		}

		score, err := client.Get(ctx, riskKey).Int64()
		if err != nil {
			t.Fatalf("read risk score after %d searches: %v", i, err)
		}

		want := int64(i * scrapingPoints)
		if score != want {
			t.Fatalf("risk score after %d searches: got %d, want %d", i, score, want)
		}

		exists, err := client.Exists(ctx, limitKey).Result()
		if err != nil {
			t.Fatalf("check limit key after %d searches: %v", i, err)
		}

		if i < searchesToThreshold && exists != 0 {
			t.Fatalf("limit key set too early, after only %d searches", i)
		}
		if i == searchesToThreshold && exists == 0 {
			t.Fatal("limit key not set after reaching the risk threshold")
		}
	}
}
