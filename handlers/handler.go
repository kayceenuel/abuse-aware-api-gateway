package handlers

import "net/http"

func loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { // check if the request method is POST
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close() // close the request body when the function returns
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet { // check if the request method is GET
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
}

func purchaseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
}
