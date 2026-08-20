package main

import (
	"fmt"
	"net/http"
	"time"
)

func simulateCredentialStuffing(client *http.Client) {
	for i := 1; i <= 20; i++ {
		url := "http://localhost:2121/login"
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			fmt.Printf("%v\n", err)
			return
		}
		req.Header.Add("X-API-Key", "testKey123")

		res, err := client.Do(req)
		if err != nil {
			fmt.Printf("request failed:  %v\n", err)
			return
		}
		fmt.Printf("[CREDENTIAL STUFFING] attempt %d — username: user%d → %s\n", i, i, res.Status)
		res.Body.Close()
	}
}

func simulateScraping(client *http.Client) {
	for i := 1; i <= 30; i++ {
		url := "http://localhost:2121/search"
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			fmt.Printf("Error %v\n", err)
			return
		}
		req.Header.Add("X-API-Key", "testKey123")

		res, err := client.Do(req)
		if err != nil {
			fmt.Printf("request failed:  %v\n", err)
			return
		}
		fmt.Printf("[SCRAPING] request 1 → 200 OK")
		res.Body.Close()
	}
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}

	simulateCredentialStuffing(client)
	simulateScraping(client)

}
