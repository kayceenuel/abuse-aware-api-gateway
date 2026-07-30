package handlers

import "net/http"

func LoginHandler(proxy http.Handler) http.HandlerFunc { // LoginHandler only accepts POST — login credentials must never appear in a URL.
	return func(w http.ResponseWriter, r *http.Request) { // check if the request method is POST, if not return an error.
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}
}

func SearchHandler(proxy http.Handler) http.HandlerFunc { // SearchHandler only accepts GET - Search queries should be in the URL, not in the body.
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}
}

func PurchaseHandler(proxy http.Handler) http.HandlerFunc { // PurchaseHandler only accepts POST - Purchase requests should be in the body, not in the URL.
	return func(w http.ResponseWriter, r *http.Request) { // check if the request method is POST, if not return an error.
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		// Call the proxy to forward the request if the method is correct.
		proxy.ServeHTTP(w, r)
	}

}
