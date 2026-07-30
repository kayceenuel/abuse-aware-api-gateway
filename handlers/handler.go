package handlers

import "net/http"

func LoginHandler(proxy http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}
}

func SearchHandler(proxy http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}
}

func PurchaseHandler(proxy http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}

}
