# Design Document

## Overview

The Abuse-Aware API Gateway sits in front of a product API and protects it from abuse.
It does this in two paths:

- a **synchronous request path** — routing, risk-flag check, rate limiting, reverse proxy; and
- an **asynchronous detection path** — request events → Kafka → consumer → risk scorer → Redis risk flag.

Redis is the shared store for both paths: the rate limiters keep their counters there, and the risk scorer writes its flag there.

---

## Request lifecycle

1. The route handler parses the API key and client IP.
2. It checks the risk flag (`limit:<ip>`) in Redis.
3. It applies the token bucket (per API key) and the sliding window (per IP).
4. If every check passes, it reverse-proxies the request to the product API.
5. On the way out it publishes a `RequestEvent` to Kafka. Publishing is not on the client's critical path for *scoring* — the client never waits for risk analysis.

---

## Design Decisions

### Why Redis for rate limiting counters

In-memory maps would work for a single instance but fall apart the moment you run
more than one gateway. Each instance would have its own counters, so an attacker
could spread requests across instances and never hit the limit on any one of them.

Redis gives all gateway instances a single shared counter store. The rate limits
are enforced consistently regardless of how many instances are running. The same
Redis instance also holds the risk flags, so the detection path and the request
path share one source of truth.

### Why token bucket and sliding window together

Each algorithm catches a different kind of attacker.

The token bucket controls burst traffic per API key. Keys get tokens that refill
over time — short spikes are tolerated, but sustained hammering drains the bucket.
This catches an attacker using one API key aggressively.

The sliding window controls request rate per IP over a rolling time window. It
doesn't care which API key is used — it counts how many requests came from that IP
in the last N seconds. This catches an attacker rotating API keys but hitting from
the same machine.

Together they're harder to evade. Rotate your key and the sliding window still
catches you. Slow down your rate and the token bucket refills, but the sliding
window remembers your history.

### Why Kafka for request logging

Writing directly to a database inside the request handler would add latency to
every request. Under attack, that latency compounds fast.

Kafka decouples the logging from the request path. The handler publishes an event
and immediately moves on — the client never waits for risk analysis. The consumer
processes events in the background at its own pace, with no impact on gateway
throughput.

Kafka also gives fault tolerance. If the consumer crashes, the events stay in the
topic and get processed when it restarts. Nothing is lost.

### Sliding window is atomic (Redis Lua)

Both rate limiters run their read-modify-write sequence **inside a single Lua
script** executed atomically by Redis:

- `tokenBucketScript` reads the token count and last-refill time, refills, and
  decrements in one atomic step.
- `slidingWindowScript` trims expired entries (`ZREMRANGEBYSCORE`), counts the
  window (`ZCARD`), and records the request (`ZADD`) in one atomic step.

Because Redis runs each script to completion without interleaving, two concurrent
requests cannot both "pass the check before either is recorded". There is no
check-then-act race between the limit check and the recording step.

### Asynchronous detection affects subsequent requests

Risk scoring is deliberately off the request path. The consumer reads events from
Kafka, and the risk scorer updates `risk:<ip>` and, past the threshold, sets
`limit:<ip>`.

This means the request that *crosses* the threshold has already been served. The
flag only takes effect on the **next** request from that IP, when the route handler
checks `limit:<ip>`. This is a conscious tradeoff: scoring every request in real
time would make every request wait on the detector.

---

## Thresholds

| Setting | Value | Source |
| --- | --- | --- |
| Token bucket | 10 tokens, refill 1/sec, per API key | `main.go` |
| Sliding window | 30 requests / minute, per IP | `main.go` |
| Detection window | 10 minutes | `kafka/risk_scorer.go` |
| Credential-stuffing points | +2 per new username | `kafka/risk_scorer.go` |
| Scraping points | +1 per search | `kafka/risk_scorer.go` |
| Risk threshold | 10 | `kafka/risk_scorer.go` |
| Risk-flag TTL | 1 hour | `kafka/risk_scorer.go` |

The sliding-window cap is intentionally higher than both behavioral thresholds
(credential stuffing trips at 5 distinct logins, scraping at 10 searches), so the
risk scorer — not the coarse per-IP cap — is what flags abuse in the demo.

---

## Verified behaviour

The following are covered by automated tests (hermetic, no external services) and,
where noted, a manual smoke test against a live Redis:

- **Atomic rate limiting** — token bucket and sliding window limits, and
  `IsRisky` reflecting `limit:<ip>` (`rate_limit/limit_test.go`).
- **Risk scoring** — distinct usernames raise the score and trip the threshold;
  a repeated username does not; `/search` accumulates points and trips the
  threshold (`kafka/risk_scorer_test.go`).
- **Gateway decisions** — valid login is proxied (200); a flagged IP is blocked
  (429 `Request blocked due to detected abuse`); missing key (401) and invalid JSON
  (400) are rejected; a Kafka publish failure is logged rather than swallowed
  (`handlers/handler_test.go`).
- **Live smoke test** — with Redis running and Kafka down, valid logins were
  proxied and, after setting `limit:<ip>` manually, the next request was blocked
  with `Request blocked due to detected abuse`.

## Known limitations

- **The full Kafka round-trip is not covered by an automated test.** Kafka is an
  external dependency and is not available in every environment (this one
  included). The producer and consumer code are exercised indirectly, but the
  end-to-end "event → Kafka → scorer → flag → block" path is verified by design
  and by the scorer/limiter tests, not by an integration test.
- **Kafka is required for detection.** If the broker is down, events are not
  published and risk scoring stops. The handler now logs publish failures instead
  of ignoring them, but it does not block traffic while Kafka is unavailable.
- **No protection against distributed botnets.** The gateway detects abuse from a
  single IP. A botnet of thousands of IPs each sending one request would pass all
  checks. Detecting this needs cross-IP pattern analysis, which this project does
  not cover.
- **Detection lag** (see above) — the triggering request is not blocked.
- **The fake product API accepts any credentials** and exists only to exercise the
  proxy path.
