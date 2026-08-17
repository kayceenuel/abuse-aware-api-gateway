package handlers

import (
	"net"
	"net/http"
	"time"

	"github.com/kayceenuel/abuse-aware-api-gateway/kafka"
	"github.com/kayceenuel/abuse-aware-api-gateway/rate_limit"
	kafkago "github.com/segmentio/kafka-go"
)

func LoginHandler(proxy http.Handler, rl *rate_limit.RateLimiter, producer *kafkago.Writer) http.HandlerFunc { // LoginHandler only accepts POST — login credentials must never appear in a URL.
	return func(w http.ResponseWriter, r *http.Request) { // check if the request method is POST, if not return an error.
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		//Extract the API key from the request header. If the API key is missing, return 401 Unauthorized.
		// The API key is used to identify the client and apply rate limiting.
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "Missing API key", http.StatusUnauthorized)
			return
		}
		// extract client IP
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)

		// check token bucket - if not allowed, return, 429 Too Many Requests.
		allowed, err := rl.AllowTokenBucket(apiKey)
		if err != nil {
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		// Check sliding_window - if not allowed, return 429 Too Many Requests.
		allowed, err = rl.AllowSlidingWindow(ip)
		if err != nil {
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		// log request event to Kafka before forwarding
		kafka.Log(producer, kafka.RequestEvent{
			IPAddress: ip,
			Endpoint:  r.URL.Path,
			APIKey:    apiKey,
			Timestamp: time.Now(),
			Allowed:   true,
		})

		// forward request to the product API via proxy
		proxy.ServeHTTP(w, r)
	}
}

func SearchHandler(proxy http.Handler, rl *rate_limit.RateLimiter, producer *kafkago.Writer) http.HandlerFunc { // SearchHandler only accepts GET - Search queries should be in the URL, not in the body.
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		//Extract the API key from the request header. If the API key is missing, return 401 Unauthorized.
		// The API key is used to identify the client and apply rate limiting.
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "Missing API key", http.StatusUnauthorized)
			return
		}
		// extract client IP
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)

		// check token bucket - if not allowed, return, 429 Too Many Requests.
		allowed, err := rl.AllowTokenBucket(apiKey)
		if err != nil {
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		// Check sliding_window - if not allowed, return 429 Too Many Requests.
		allowed, err = rl.AllowSlidingWindow(ip)
		if err != nil {
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		// log the request event to Kafka
		kafka.Log(producer, kafka.RequestEvent{
			IPAddress: ip,
			Endpoint:  r.URL.Path,
			APIKey:    apiKey,
			Timestamp: time.Now(),
			Allowed:   true,
		})

		// forward request to the product API via proxy
		proxy.ServeHTTP(w, r)
	}
}

func PurchaseHandler(proxy http.Handler, rl *rate_limit.RateLimiter, producer *kafkago.Writer) http.HandlerFunc { // PurchaseHandler only accepts POST - Purchase requests should be in the body, not in the URL.
	return func(w http.ResponseWriter, r *http.Request) { // check if the request method is POST, if not return an error.
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		//Extract the API key from the request header. If the API key is missing, return 401 Unauthorized.
		// The API key is used to identify the client and apply rate limiting.
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "Missing API key", http.StatusUnauthorized)
			return
		}
		// extract client IP
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)

		// check token bucket - if not allowed, return, 429 Too Many Requests.
		allowed, err := rl.AllowTokenBucket(apiKey)
		if err != nil {
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		// Check sliding_window - if not allowed, return 429 Too Many Requests.
		allowed, err = rl.AllowSlidingWindow(ip)
		if err != nil {
			http.Error(w, "Rate limiter error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		// log request event to Kafka before forwarding
		kafka.Log(producer, kafka.RequestEvent{
			IPAddress: ip,
			Endpoint:  r.URL.Path,
			APIKey:    apiKey,
			Timestamp: time.Now(),
			Allowed:   true,
		})
		// forward request to the product API via proxy
		proxy.ServeHTTP(w, r)
	}

}
