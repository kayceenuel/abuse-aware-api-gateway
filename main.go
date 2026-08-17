package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/kayceenuel/abuse-aware-api-gateway/handlers"
	"github.com/kayceenuel/abuse-aware-api-gateway/kafka"
	"github.com/kayceenuel/abuse-aware-api-gateway/proxy"
	"github.com/kayceenuel/abuse-aware-api-gateway/rate_limit"
	"github.com/redis/go-redis/v9"
)

func main() {
	// create a Redis client
	// create a rate limiter using NewRateLimiter
	//Pass it to each handler func along proxyHandler
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Redis server address
	})
	defer redisClient.Close()

	// create a rate limiter using NewRateLimiter
	rateLimiter := rate_limit.NewRateLimiter(redisClient, 10, 1, time.Minute, 5)

	proxyHandler, err := proxy.NewHandler("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	// create risk scorer
	scorer := kafka.NewRiskScorer(redisClient, 10, 50)

	// create kafka producer
	producer := kafka.NewProducer("localhost:9092", "gateway_events")
	defer producer.Close()

	// start consumer in background
	go kafka.Start("localhost:9092", "gateway_events", scorer)
	// handle the routes: /login, /search, /purchase
	http.Handle("/login", handlers.LoginHandler(proxyHandler, rateLimiter, producer))
	http.Handle("/search", handlers.SearchHandler(proxyHandler, rateLimiter, producer))
	http.Handle("/purchase", handlers.PurchaseHandler(proxyHandler, rateLimiter, producer))

	// set up the HTTP server
	fmt.Println("Server is running on http://localhost:2121")
	log.Fatal(http.ListenAndServe(":2121", nil))
}
