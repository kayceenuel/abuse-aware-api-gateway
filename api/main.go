package main // independent from the main.go file, this is a separate API server that handles the resp for the proxy.*/

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// Response represents a generic API response structure.
type loginResponse struct {
	Message string `json:"message"`
	Token   string `json:"token"`
}

type searchResponse struct {
	Message string   `json:"message"`
	Items   []string `json:"items"`
}

type purchaseResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
}

/* The Response handlers writes JSON Response to the proxy */
func LoginResponse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	// handles the login resp and returns a JSON resp with a msg and token
	response := loginResponse{
		Message: "Login successful",
		Token:   "abc123xyz",
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func SearchResponse(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := searchResponse{
		Message: "Search results",
		Items:   []string{"item1", "item2", "item3"},
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func PurchaseResponse(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := purchaseResponse{
		Message: "Purchase successful",
		Success: true,
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func main() {
	http.HandleFunc("/login", LoginResponse)
	http.HandleFunc("/search", SearchResponse)
	http.HandleFunc("/purchase", PurchaseResponse)

	// set up the HTTP server
	fmt.Println("Server is running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
