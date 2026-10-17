package handlers

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kayceenuel/abuse-aware-api-gateway/rate_limit"
	"github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
)

// unreachableProducer points at a closed port and fails fast, so tests never
// block on Kafka. There is no broker running in the test environment.
func unreachableProducer() *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP("127.0.0.1:1"),
		Topic:        "gateway_events",
		MaxAttempts:  1,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		Transport:    &kafkago.Transport{DialTimeout: 500 * time.Millisecond},
	}
}

func newTestLimiter(t *testing.T) (*rate_limit.RateLimiter, *miniredis.Miniredis) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	return rate_limit.NewRateLimiter(client, 10, 1, time.Minute, 30), mr
}

func okProxy() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestLoginAllowsValidRequest(t *testing.T) {
	rl, _ := newTestLimiter(t)
	handler := LoginHandler(okProxy(), rl, unreachableProducer())

	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"user1","password":"pw"}`))
	req.Header.Set("X-API-Key", "testkey123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("valid login: got %d, want 200", rec.Code)
	}
}

func TestLoginBlocksFlaggedIP(t *testing.T) {
	rl, mr := newTestLimiter(t)
	handler := LoginHandler(okProxy(), rl, unreachableProducer())

	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"user1","password":"pw"}`))
	req.Header.Set("X-API-Key", "testkey123")

	ip := req.RemoteAddr[:strings.LastIndex(req.RemoteAddr, ":")]
	mr.Set("limit:"+ip, "1")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("flagged IP: got %d, want 429", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "detected abuse") {
		t.Fatalf("flagged IP: body %q does not mention detected abuse", rec.Body.String())
	}
}

func TestLoginRejectsBadInput(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		apiKey string
		want   int
	}{
		{name: "missing api key", body: `{"username":"user1"}`, apiKey: "", want: http.StatusUnauthorized},
		{name: "invalid json", body: `not-json`, apiKey: "testkey123", want: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rl, _ := newTestLimiter(t)
			handler := LoginHandler(okProxy(), rl, unreachableProducer())

			req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(tc.body))
			if tc.apiKey != "" {
				req.Header.Set("X-API-Key", tc.apiKey)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tc.want {
				t.Fatalf("got %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

// TestLoginLogsPublishFailure is the regression test for the silent-drop bug:
// when Kafka is unavailable the handler must surface the failure instead of
// swallowing it, so a down broker cannot silently disable risk detection.
func TestLoginLogsPublishFailure(t *testing.T) {
	rl, _ := newTestLimiter(t)

	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })

	handler := LoginHandler(okProxy(), rl, unreachableProducer())

	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"user1","password":"pw"}`))
	req.Header.Set("X-API-Key", "testkey123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("request should still succeed for the client: got %d, want 200", rec.Code)
	}
	if !strings.Contains(buf.String(), "failed to publish request event") {
		t.Fatalf("expected a logged publish failure, got log output: %q", buf.String())
	}
}
