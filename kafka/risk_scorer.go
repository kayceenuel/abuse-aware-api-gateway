package kafka

import "github.com/redis/go-redis/v9"

type RiskScorer struct {
	client            *redis.Client // to read & write counters
	stuffingThreshold int // how many unique username from one IP before it's supicious
	scrapingThreshold int // how many /search request before it's supicious
}
