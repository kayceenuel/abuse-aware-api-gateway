package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

const gatewayURL = "http://localhost:2121"

// simulateCredentialStuffing sends different usernames from the same API key
// to simulate credential stuffing from one IP.
func simulateCredentialStuffing(client *http.Client) {
	var allowed, blocked int

	fmt.Println("=== Credential stuffing ===")

	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("user%d", i)

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
			gatewayURL+"/login",
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

		fmt.Printf("attempt %d — %s → %s\n", i, username, res.Status)

		if res.StatusCode >= 200 && res.StatusCode < 300 {
			allowed++
		} else {
			blocked++
		}

		res.Body.Close()
		// Small delay keeps the replay output readable during a live demo.
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("Summary: %d allowed, %d blocked\n\n", allowed, blocked)
}

// simulateScraping sends repeated search requests from the same IP
// to simulate automated scraping.
func simulateScraping(client *http.Client) {
	var allowed, blocked int

	fmt.Println("=== Scraping ===")

	for i := 1; i <= 30; i++ {
		req, err := http.NewRequest(
			http.MethodGet,
			gatewayURL+"/search",
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

		fmt.Printf("request %d → %s\n", i, res.Status)

		if res.StatusCode >= 200 && res.StatusCode < 300 {
			allowed++
		} else {
			blocked++
		}

		res.Body.Close()
		// Small delay keeps the replay output readable during a live demo.
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("Summary: %d allowed, %d blocked\n\n", allowed, blocked)
}

// Run the selected abuse scenario; default to both.
func main() {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	mode := "all"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	switch mode {
	case "stuffing":
		simulateCredentialStuffing(client)
	case "scraping":
		simulateScraping(client)
	case "all":
		simulateCredentialStuffing(client)
		simulateScraping(client)
	default:
		fmt.Println("Usage: go run ./scripts/replay.go [stuffing|scraping|all]")
	}
}
