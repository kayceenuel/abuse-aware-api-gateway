package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/kayceenuel/abuse-aware-api-gateway/kafka"
	"github.com/kayceenuel/abuse-aware-api-gateway/rate_limit"
	kafkago "github.com/segmentio/kafka-go"
)

type loginRequest struct {
	Username string `json:"username"`
}

func LoginHandler(proxy http.Handler, rl *rate_limit.RateLimiter, producer *kafkago.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		apiKey := r.Header.Get("X-API-Key")

		event := kafka.RequestEvent{
			IPAddress: ip,
			Endpoint:  r.URL.Path,
			Timestamp: time.Now(),
		}

		defer func() {
			kafka.Log(producer, event)
		}()

		if r.Method != http.MethodPost {
			event.Allowed = false
			event.Reason = "invalid request method"
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		if apiKey == "" {
			event.Allowed = false
			event.Reason = "missing API Key"
			http.Error(w, "Missing API Key", http.StatusUnauthorized)
			return
		}

		// Check if the IP is risky before applying rate limits.
		body, err := io.ReadAll(r.Body)
		if err != nil {
			event.Allowed = false
			event.Reason = "invalid request body"
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		var login loginRequest
		if err := json.Unmarshal(body, &login); err != nil {
			event.Allowed = false
			event.Reason = "invalid request body"
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		event.Username = login.Username
		r.Body = io.NopCloser(bytes.NewReader(body))

		allowed, err := rl.AllowTokenBucket(apiKey)
		if err != nil {
			event.Allowed = false
			event.Reason = "token bucket error"
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}

		if !allowed {
			event.Allowed = false
			event.Reason = "token bucket rate limit exceeded"
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		allowed, err = rl.AllowSlidingWindow(ip)
		if err != nil {
			event.Allowed = false
			event.Reason = "sliding window error"
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}

		if !allowed {
			event.Allowed = false
			event.Reason = "sliding window rate limit exceeded"
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		event.Allowed = true
		event.Reason = "request allowed"

		proxy.ServeHTTP(w, r)
	}
}

func SearchHandler(proxy http.Handler, rl *rate_limit.RateLimiter, producer *kafkago.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		apiKey := r.Header.Get("X-API-Key")

		event := kafka.RequestEvent{
			IPAddress: ip,
			Endpoint:  r.URL.Path,
			Timestamp: time.Now(),
		}

		defer func() {
			kafka.Log(producer, event)
		}()

		if r.Method != http.MethodGet {
			event.Allowed = false
			event.Reason = "invalid request method"
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		if apiKey == "" {
			event.Allowed = false
			event.Reason = "missing API Key"
			http.Error(w, "Missing API Key", http.StatusUnauthorized)
			return
		}

		// Check if the IP is risky before applying rate limits.
		risky, err := rl.IsRisky(ip)
		if err != nil {
			event.Allowed = false
			event.Reason = "risk check error"
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		if risky {
			event.Allowed = false
			event.Reason = "risk limit exceeded"
			http.Error(w, "Risk limit exceeded", http.StatusTooManyRequests)
			return
		}

		allowed, err := rl.AllowTokenBucket(apiKey)
		if err != nil {
			event.Allowed = false
			event.Reason = "token bucket error"
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}

		if !allowed {
			event.Allowed = false
			event.Reason = "token bucket rate limit exceeded"
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		allowed, err = rl.AllowSlidingWindow(ip)
		if err != nil {
			event.Allowed = false
			event.Reason = "sliding window error"
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}

		if !allowed {
			event.Allowed = false
			event.Reason = "sliding window rate limit exceeded"
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		event.Allowed = true
		event.Reason = "request allowed"

		proxy.ServeHTTP(w, r)
	}
}

func PurchaseHandler(proxy http.Handler, rl *rate_limit.RateLimiter, producer *kafkago.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		apiKey := r.Header.Get("X-API-Key")

		event := kafka.RequestEvent{
			IPAddress: ip,
			Endpoint:  r.URL.Path,
			Timestamp: time.Now(),
			Allowed:   false,
			Reason:    "request not processed",
		}

		defer func() {
			kafka.Log(producer, event)
		}()

		if r.Method != http.MethodPost {
			event.Allowed = false
			event.Reason = "invalid request method"
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		if apiKey == "" {
			event.Allowed = false
			event.Reason = "missing API Key"
			http.Error(w, "Missing API Key", http.StatusUnauthorized)
			return
		}
		// // Check if the IP is risky before applying rate limits.
		risky, err := rl.IsRisky(ip)
		if err != nil {
			event.Allowed = false
			event.Reason = "risk check error"
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		if risky {
			event.Allowed = false
			event.Reason = "risk limit exceeded"
			http.Error(w, "Risk limit exceeded", http.StatusTooManyRequests)
			return
		}

		allowed, err := rl.AllowTokenBucket(apiKey)
		if err != nil {
			event.Allowed = false
			event.Reason = "token bucket error"
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}

		if !allowed {
			event.Allowed = false
			event.Reason = "token bucket rate limit exceeded"
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		allowed, err = rl.AllowSlidingWindow(ip)
		if err != nil {
			event.Allowed = false
			event.Reason = "sliding window error"
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}

		if !allowed {
			event.Allowed = false
			event.Reason = "sliding window rate limit exceeded"
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		event.Allowed = true
		event.Reason = "request allowed"

		proxy.ServeHTTP(w, r)
	}
}
