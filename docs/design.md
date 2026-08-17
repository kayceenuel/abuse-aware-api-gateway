# Design Document

## Overview

The Abuse-Aware API Gateway sits in front of a product API and protects it from abuse. 
It does this in three layers: routing and proxying (Ring 1), rate limiting (Ring 2), 
and behavioral analysis (Ring 3).

---

## Design Decisions

### Why Redis for rate limiting counters

In-memory maps would work for a single instance but fall apart the moment you run 
more than one gateway. Each instance would have its own counters, so an attacker 
could spread requests across instances and never hit the limit on any one of them.

Redis gives all gateway instances a single shared counter store. The rate limits 
are enforced consistently regardless of how many instances are running.

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

---

## Known Limitations

### Sliding window race condition

The limit check and the request recording happen in two separate steps. If two 
requests arrive at the same time, both could pass the check before either is 
recorded. A Lua script executed atomically in Redis would fix this in production.

### No protection against distributed botnets

The gateway detects abuse from a single IP. A sophisticated attacker using a 
botnet — thousands of IPs each sending one request — would pass all checks. 
Detecting this would require cross-IP pattern analysis, which this project doesn't cover.

### Detection lag

By the time the risk scorer flags an IP, the request that triggered it has already 
gone through. The tighter limits only apply to the next request. This is a conscious 
tradeoff — catching abuse in real time would mean making every request wait for a 
risk score, which slows everything down.