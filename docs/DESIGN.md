# Design — Live Trip Updates Fan-Out

Case Study 4: ordered, resumable, back-pressured live updates for cab trips (LOCATION, STATUS, SOS) over WebSocket, multi-node, multi-tenant.

## 1. Architecture

```
 cabs ──HTTP POST (cab JWT)──► nginx ──► gateway-N ─┐
                                                    │ publish.lua (atomic: dedup → seq → buffer → snapshot → PUBLISH)
                                                    ▼
                                                  Redis ──pub/sub──► every gateway ──► per-connection outbox ──► WebSocket
 clients ◄──WebSocket (user JWT, SUBSCRIBE/RESUME)── nginx ◄───────────────────────────────────────────────────────┘
                                          PostgreSQL: users/trips/cabs (auth lookups, cached) + async archive of events
```

- **Gateways are stateless with respect to history.** Any node can serve any client; Redis holds sequences, the replay buffer and snapshots.
- **Topics:** `trip:{id}`, `cab:{id}`, `office:{id}`. Each topic has its own sequence number, assigned by Redis inside one Lua script, so there is a single assigner per topic.
- **Publish path** (`publish.lua`, one atomic script per topic): dedup by `eventId` → stale-location guard → `INCR seq` → (STATUS/SOS only) `ZADD` into the critical replay buffer, trimmed to the last 100, remembering the highest evicted seq (`critFloor`) → update the latest-state snapshot → `PUBLISH`. Sequence order is publish order.
- **Two Redis channels per topic:** `sos:{topic}` for STATUS+SOS (own connection, own dedicated thread) and `evt:{topic}` for LOCATION (striped bounded dispatcher). A flood of locations cannot delay an SOS.
- **Routing across nodes:** every gateway subscribes to the channels of the topics its own clients watch (refcounted, batched). A published event reaches the node holding a subscriber through Redis pub/sub; the publishing node receives its own events the same way, so there is no special local path.
- **Per-connection outbox** (`ConnectionOutbox`): one *ordered critical lane* (STATUS+SOS, capacity 320, never silently dropped) and one *conflated location lane* (latest per cab, capacity 1024). Memory per connection is bounded by construction.
- **Per-connection writer** (`ConnectionWriter`): one virtual thread per connection drains its own outbox with a send timeout. A frozen client only blocks itself and is disconnected after the timeout.
- **Sequence numbers on the wire:** every EVENT frame carries `(topic, seq)`. The client keeps a sliding seen-window per topic (not a "highest seq" mark), because SOS can overtake lower-sequence events, and drops duplicates.

## 2. Resume after a disconnect or node crash

```
Client                  Gateway B (new node)                    Redis
  │ connect(JWT) ─────────►│                                      │
  │ RESUME{topic,lastSeq} ►│ authorize(role, tenant, trip…)       │
  │                        │ register in "buffering" state ──────►│ SUBSCRIBE sos:/evt: (batched)
  │                        │ whenSubscribed ◄─────────────────────│
  │ ◄── ACK{seq=baseline}  │ resume.lua(lastSeq) ────────────────►│ atomic read: seqs, critFloor, buffer, snapshot
  │                        │ ◄────────────── decision             │
  │                        │  NOTHING | REPLAY (missed STATUS/SOS in order) | SNAPSHOT (too far behind)
  │ ◄── replay / snapshot  │ then flush events parked meanwhile (seen-window removes replay/live overlap)
  │ ◄── live events …      │
```

- `lastSeq` is the highest **critical** (STATUS/SOS) sequence the client received, or the baseline from the first ACK if it subscribed to an empty topic. LOCATION is latest-only; the snapshot always carries the newest location.
- `ReplayDecision`: `lastSeq ≥ currentSeq` → NOTHING; `lastSeq ≥ critFloor` → REPLAY the missed critical events in order; otherwise → SNAPSHOT (the buffer no longer covers the gap; the frame carries the latest state of the topic).
- Live events that arrive while the replay is being read are parked per topic and flushed after it, so replay always precedes live.

## 3. Failure modes

| Failure | What the user sees | How the system recovers | Measured |
|---|---|---|---|
| Gateway crashes (no goodbye) | Connection drops; the client reconnects within about a second | nginx stops routing to the dead node (connect timeout 500 ms, `fail_timeout` 10 s); clients reconnect with jitter and RESUME on a survivor | 1,000 clients: all back in 2.3 s. 500 clients: 1.3 s. One STATUS/SOS lost in one of two 500-client runs (§6) |
| Gateway shut down gracefully | Connection closed with code 1013 | Client reconnects elsewhere and resumes | Covered by an automated test |
| Redis restarts | Cabs get `503` with `Retry-After`; SOS is retried 3× within 500 ms first | Pub/sub reconnects; per-topic reconcile detects gaps and repairs them from the buffer | 300 clients: 0 lost, 0 duplicates, 0 order violations |
| Pub/sub message lost | Nothing, if repaired in time | Per-topic `ReceiveTracker` sees a seq gap, waits a short grace period, repairs from the critical buffer; if the buffer cannot cover it, closes the connection so the client resumes | `pubsub_gap_repairs_total` > 0 in every load run |
| Slow or frozen client | Its own location updates are conflated; if its critical lane overflows it gets an explicit resync or disconnect (never a silent drop) | Send timeout disconnects a frozen socket; other clients are unaffected | Isolation test passes; `events_dropped{stage="outbox"}` = 0 on survivors in all runs |
| Replay buffer exhausted (offline for more than 100 critical events) | Receives a snapshot of the current state, then live events | Client resyncs from the latest state | Covered by the replay decision tests |
| PostgreSQL down | Live delivery continues; cached auth decisions keep working until their TTL ends | Archive writes are asynchronous and bounded; they never block publish (counted in `archive_dropped_total`) | Covered by an automated test |
| Duplicate, late or invalid input | Duplicate `eventId` → `202` with no new sequence; stale LOCATION is dropped and counted; illegal STATUS transition → `409` | Idempotent by `eventId` | Covered by ingest tests |

## 4. Security

- JWT user tokens (EMPLOYEE / ADMIN / SECURITY_OPERATOR) authenticate the WebSocket and read paths. CAB_DEVICE tokens, minted by an admin or operator, authenticate ingest. Ingest takes the office from the trip, not from the request; `eventId` is mandatory.
- Subscription rules are re-checked on **every** SUBSCRIBE and RESUME, including the tenant check for every role. Employees see only their own trips, admins see their office's trips, cabs and office topic, and operators see anything in their tenant. Unknown, malformed and foreign topics all get the same 403, so existence cannot be enumerated.
- Connections are closed at token expiry. Logs and error bodies carry no PII or tokens (masking, plus generic errors with an `errorId`).

## 5. Cost and complexity

- **Memory per connection (bounded):** outbox ≤ 320 critical + ≤ 1,024 location envelopes; a typical connection holds a few KB. Per subscribed topic: one seen-window (1,024-entry window) and one receive tracker. Redis: per topic ≤ 100 buffered critical events plus one snapshot.
- **Time:** publish is O(log N) in Redis (N ≤ 100). Fan-out per event is O(subscribers on the node), with O(1) work per subscriber (`offer` into a bounded outbox). Resume is O(missed events), capped at 100.
- **Redis load:** two pub/sub connections per node regardless of client count; one SUBSCRIBE per *topic with at least one local subscriber* (refcounted, batched), not per client.

## 6. Measured results

Environment: Docker VM with 8 logical cores and 3.7 GiB RAM on one Windows laptop; nginx → 3 gateways (512 MB heap each) → Redis 7 (AOF) and PostgreSQL; the load simulator runs on the same machine. Full run log in [`VALIDATION_REPORT.md`](VALIDATION_REPORT.md).

| Scenario | Result |
|---|---|
| 500 connections, 150 cabs, one gateway killed at +15 s (run B) | 0 of 7,287 critical deliveries missing; 0 order violations; SOS p99 819 ms; all clients back in 1.3 s |
| Same scenario, repeated (run A) | 1 of 7,275 critical deliveries missing in one consumer; 0 order violations; SOS p99 505 ms. Investigation open (see §7) |
| 1,000 connections, 300 cabs, one gateway killed at +15 s (run C) | 0 of 24,408 missing; SOS p99 971 ms; all clients back in 2.3 s. This was the only 1,000-connection run that passed; later runs on the final build did not (§7) |
| Redis restarted mid-run, 300 connections | 0 lost; 0 duplicates; 0 order violations; publishers retried through the outage |

## 7. Scale limits and known limitations

- **Verified scale: 500 connections on one laptop**, with the node-kill and Redis-restart behaviour above. The case study's 10,000 connections and 2,000 publishing cabs were **not run**; this machine cannot host them. The design scales by adding gateways (no per-node state beyond each node's own connections) and by sharding topics across Redis instances if one Redis becomes the limit.
- **1,000 connections is not a stable result on this laptop.** One run passed; later runs saturated the machine, with ingest and SOS latency in the seconds and, in two runs, a few thousand critical events missing. The CPU load was concentrated on the gateway that took the reconnects.
- **Known correctness gap:** one STATUS/SOS event was missing in one consumer in one of two 500-connection kill runs. It has not been traced to a cause yet. The leading hypothesis is the window between the node dying and the client's RESUME. Zero loss is therefore not claimed.
- **Archive:** under load, some batched archive inserts hit the `(trip_id, sequence_number)` unique constraint and were dropped. Publish is not affected. Making the archive insert idempotent is the planned fix.
- 200k connections is out of reach for this stack on one box. The cost is dominated by socket memory and fan-out CPU, which means horizontal gateways.
- A finding kept from development: virtual threads blocked inside `synchronized` or `ConcurrentHashMap.compute` can starve the carrier pool under connection storms. It was found with `-Djdk.tracePinnedThreads` and fixed in `TtlCache` and the session send path.

## 8. Trade-offs and deliberate omissions

- **Per-topic ordering only**, with no global order. STATUS and SOS are never dropped; LOCATION is latest-wins (conflated) by design.
- **At-least-once to the wire, exactly-once to the application** through `(topic, seq)` and the client's seen-window. There is no exactly-once transport.
- **Redis is a single point of failure.** There is no Sentinel or Cluster. AOF is on, so a restart is tolerated; losing the Redis volume loses the replay buffer, and clients then get a SNAPSHOT. Not built: Redis HA, Kafka, multi-region.
- The replay buffer holds only STATUS and SOS (last 100 per topic). Anything older is served as a snapshot, not as history. The PostgreSQL archive is write-only here, for audit, and is not a replay source.
- Latency is measured on the local clock, because all nodes run on one host.
