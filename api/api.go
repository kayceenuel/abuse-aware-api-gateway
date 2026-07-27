package api

import (
	"encoding/json"
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

func LoginResponse(w http.ResponseWriter, r *http.Request) {
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
