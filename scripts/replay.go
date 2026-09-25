package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// simulateCredentialStuffing sends 20 POST requests to /login with different
// usernames from the same API key, mimicking a credential stuffing attack.
func simulateCredentialStuffing(client *http.Client) {
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("user%d", i)

		// Send the username in the request so the gateway can track
		// which accounts are being targeted.
		loginBody := map[string]string{
			"username": username,
			"password": "testpassword",
		}

		body, err := json.Marshal(loginBody)
		if err != nil {
			fmt.Printf("request error: %v\n", err)
			return
		}

		req, err := http.NewRequest(
			http.MethodPost,
			"http://localhost:2121/login",
			bytes.NewReader(body),
		)
		if err != nil {
			fmt.Printf("request error: %v\n", err)
			return
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", "testkey123")

		res, err := client.Do(req)
		if err != nil {
			fmt.Printf("request failed: %v\n", err)
			return
		}

		fmt.Printf(
			"[CREDENTIAL STUFFING] attempt %d — username: %s → %s\n",
			i,
			username,
			res.Status,
		)

		res.Body.Close()

		// Small delay so gateway events are easier to follow during a live demo.
		time.Sleep(100 * time.Millisecond)
	}
}

// simulateScraping sends 30 GET requests to /search in rapid succession,
// mimicking a bot harvesting the product catalog.
func simulateScraping(client *http.Client) {
	for i := 1; i <= 30; i++ {
		req, err := http.NewRequest(
			http.MethodGet,
			"http://localhost:2121/search",
			nil,
		)
		if err != nil {
			fmt.Printf("request error: %v\n", err)
			return
		}

		req.Header.Set("X-API-Key", "testkey123")

		res, err := client.Do(req)
		if err != nil {
			fmt.Printf("request failed: %v\n", err)
			return
		}

		fmt.Printf("[SCRAPING] request %d → %s\n", i, res.Status)

		res.Body.Close()

		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}

	fmt.Println("=== Simulating credential stuffing ===")
	simulateCredentialStuffing(client)

	fmt.Println("\n=== Simulating scraping ===")
	simulateScraping(client)
}
