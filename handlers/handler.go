package handlers

import (
	"net/http"

	"github.com/kayceenuel/abuse-aware-api-gateway/rate_limit"
)

func LoginHandler(proxy http.Handler, rl *rate_limit.RateLimiter) http.HandlerFunc { // LoginHandler only accepts POST — login credentials must never appear in a URL.
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
		ip := r.RemoteAddr

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
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}
}

func SearchHandler(proxy http.Handler, rl *rate_limit.RateLimiter) http.HandlerFunc { // SearchHandler only accepts GET - Search queries should be in the URL, not in the body.
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
		ip := r.RemoteAddr

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
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}
}

func PurchaseHandler(proxy http.Handler, rl *rate_limit.RateLimiter) http.HandlerFunc { // PurchaseHandler only accepts POST - Purchase requests should be in the body, not in the URL.
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
		ip := r.RemoteAddr

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
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}

}
