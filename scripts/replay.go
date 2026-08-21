package main

import (
	"fmt"
	"net/http"
	"time"
)

func simulateCredentialStuffing(client *http.Client) {
	for i := 1; i <= 20; i++ {
		req, err := http.NewRequest("POST", "http://localhost:2121/login", nil)
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
		fmt.Printf("[CREDENTIAL STUFFING] attempt %d — username: user%d → %s\n", i, i, res.Status)
		res.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
}

func simulateScraping(client *http.Client) {
	for i := 1; i <= 30; i++ {
		req, err := http.NewRequest(http.MethodGet, "http://localhost:2121/search", nil)
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
