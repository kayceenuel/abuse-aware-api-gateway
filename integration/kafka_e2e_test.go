//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kayceenuel/abuse-aware-api-gateway/handlers"
	"github.com/kayceenuel/abuse-aware-api-gateway/kafka"
	"github.com/kayceenuel/abuse-aware-api-gateway/rate_limit"
	"github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	// riskThreshold(10) / credentialStuffingPoints(2).
	distinctUsernamesToThreshold = 5

	consumerWarmup = 3 * time.Second
	pollInterval   = 250 * time.Millisecond
	pollTimeout    = 30 * time.Second
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestKafkaToBlockingRoundTrip exercises the real asynchronous detection path
// end to end: request event -> Kafka -> consumer -> risk scorer -> Redis
// limit flag -> a subsequent gateway request from the same IP returns HTTP 429.
//
// It isolates its data with a unique topic, IP and API key, uses bounded
// polling for the asynchronous step, and skips (rather than fails) when Kafka
// or Redis is not reachable.
func TestKafkaToBlockingRoundTrip(t *testing.T) {
	broker := envOr("KAFKA_BROKER", "127.0.0.1:9092")
	redisAddr := envOr("REDIS_ADDR", "127.0.0.1:6379")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", redisAddr, err)
	}

	// Unique identifiers keep this run's topic, IP and counters isolated.
	run := time.Now().UnixNano()
	topic := fmt.Sprintf("gateway_events_it_%d", run)
	testIP := fmt.Sprintf("198.51.100.%d", (run%200)+1)
	apiKey := fmt.Sprintf("it-key-%d", run)

	// Create the topic explicitly instead of relying on broker auto-create.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dialCancel()
	conn, err := kafkago.DialContext(dialCtx, "tcp", broker)
	if err != nil {
		t.Skipf("kafka not reachable at %s: %v", broker, err)
	}
	if err := conn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		conn.Close()
		t.Fatalf("create topic %s: %v", topic, err)
	}
	conn.Close()

	keys := []string{
		"risk:" + testIP,
		"stuffing:" + testIP,
		"scraping:" + testIP,
		"limit:" + testIP,
		"sliding_window:" + testIP,
		"token_bucket:" + apiKey + ":tokens",
		"token_bucket:" + apiKey + ":last_refill",
		"token_bucket:" + apiKey + "-probe:tokens",
		"token_bucket:" + apiKey + "-probe:last_refill",
		"sliding_window:" + testIP + "-probe",
	}
	clean := func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rdb.Del(c, keys...)
	}
	clean()
	defer clean()

	// Real consumer + scorer against the real broker and Redis.
	scorer := kafka.NewRiskScorer(rdb)
	go kafka.Start(broker, topic, scorer)

	producer := kafka.NewProducer(broker, topic)
	defer producer.Close()

	rl := rate_limit.NewRateLimiter(rdb, 10, 1, time.Minute, 30)
	proxyStub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := handlers.LoginHandler(proxyStub, rl, producer)

	// Sanity-check the limiter against the real Redis so a Redis/Lua problem is
	// reported clearly instead of surfacing as an opaque 500.
	if _, err := rl.AllowTokenBucket(apiKey + "-probe"); err != nil {
		t.Fatalf("token bucket probe: %v", err)
	}
	if _, err := rl.AllowSlidingWindow(testIP + "-probe"); err != nil {
		t.Fatalf("sliding window probe: %v", err)
	}

	login := func(i int) *httptest.ResponseRecorder {
		body := fmt.Sprintf(`{"username":"user%d","password":"pw"}`, i)
		req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(body))
		req.Header.Set("X-API-Key", apiKey)
		req.RemoteAddr = testIP + ":40000"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// Let the consumer join its group before producing.
	time.Sleep(consumerWarmup)

	// Distinct usernames are allowed and published to Kafka.
	for i := 1; i <= distinctUsernamesToThreshold; i++ {
		if rec := login(i); rec.Code != http.StatusOK {
			t.Fatalf("login %d: got %d body=%q, want 200", i, rec.Code, rec.Body.String())
		}
	}

	// Wait (bounded) for the consumer to score the events and set the flag.
	limitKey := "limit:" + testIP
	deadline := time.Now().Add(pollTimeout)
	var score int64
	for {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		exists, err := rdb.Exists(c, limitKey).Result()
		cancel()
		if err != nil {
			t.Fatalf("redis EXISTS %s: %v", limitKey, err)
		}
		if exists == 1 {
			break
		}
		if time.Now().After(deadline) {
			score, _ = rdb.Get(context.Background(), "risk:"+testIP).Int64()
			t.Fatalf("timed out after %s waiting for %s (risk score=%d)", pollTimeout, limitKey, score)
		}
		time.Sleep(pollInterval)
	}

	// A subsequent request from the same IP must now be blocked.
	rec := login(999)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("post-flag login: got %d, want 429", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "detected abuse") {
		t.Fatalf("post-flag login body %q does not mention detected abuse", rec.Body.String())
	}
}
