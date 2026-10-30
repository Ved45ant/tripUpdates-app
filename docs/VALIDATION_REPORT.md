# Validation report — Live Trip Updates (Case Study 4)

Architecture and failure modes: [`DESIGN.md`](DESIGN.md). How to reproduce: [`../loadtest/README.md`](../loadtest/README.md).

## 1. Test environment

| Item | Value |
|---|---|
| Host | Windows 11, Docker Desktop (WSL2 backend) |
| Docker VM | 8 logical cores, 3.7 GiB RAM |
| Stack | nginx (`least_conn`) → 3 gateways (JDK 21, Tomcat with virtual threads, 512 MB heap each) → Redis 7 (AOF on, `volatile-ttl`) and PostgreSQL |
| Load generator | `loadtest/LoadSimulator.java`, JDK 21, in a container on the stack's Docker network, 768 MB heap |
| Topology | Everything on one machine. The simulator competes with the gateways for CPU, so these numbers are a lower bound for real hardware. |

## 2. Automated tests

`./mvnw test -Dtest='!TripUpdatesApplicationTests'` → **182 tests, 0 failures, 0 errors.** Redis and PostgreSQL run in Testcontainers.

| Area | What is verified |
|---|---|
| Outbox and back-pressure | SOS is never dropped; STATUS/SOS keep their order in the critical lane; locations are conflated to the latest per cab; overflow produces an explicit resync, not a silent drop; memory is bounded |
| Concurrency | 16-thread offer/drain with no lost STATUS/SOS; 32 threads × 200 publishes to one topic give contiguous sequences; resume racing with live publishing gives no gap and no duplicate |
| Dedup and replay | Sliding seen-window; replay decision table (NOTHING / REPLAY / SNAPSHOT); the "lift for 40 seconds" scenario gives exactly the three missed STATUS events and the latest location |
| Cross-node | An event published on one gateway reaches a subscriber on another; the SOS channel is unaffected by a saturated location channel |
| Authorization | Role × (own / other office / other tenant / missing / malformed) matrix for trips, cabs and offices; identical 403 for missing and foreign topics; a subscribe to another client's trip is refused |
| Ingest | Duplicate `eventId` is idempotent; stale locations are dropped and counted; illegal status transitions are refused; Redis-down gives 503; archive failure does not block publish |
| Failure | In-process node crash: two nodes on one Redis, the first killed without a goodbye, clients reconnect to the second and resume; every critical event arrives exactly once and in order; survivor SOS latency < 2 s. Graceful shutdown closes clients with code 1013 |
| Observability | Logs and error bodies contain no email, phone or token; metrics are registered and incremented |

## 3. Load and failure runs (compose stack)

**How latency is measured:** from the start of the successful ingest POST (for SOS, the attempt that succeeded) to the moment a subscribed consumer parsed the frame. Publisher and consumers share one host clock.

**How a run works:** the simulator logs in, mints one cab token per cab, opens consumers through nginx (security desks on the office topic, the rest watching one trip each), publishes LOCATION, STATUS and SOS events, and then `docker kill`s one gateway at a fixed time. Afterwards it checks, per consumer, that every acknowledged STATUS/SOS arrived exactly once and in order.

| Run | Connections | Cabs | Gateway killed | Ingest POST p50 / p90 / p99 | SOS delivery p50 / p90 / p99 / max | Critical events missing | Order violations | Clients back after kill | Result |
|---|---|---|---|---|---|---|---|---|---|
| B | 500 | 150 | gateway-2 at +15 s | 7 / 37 / 611 ms | 7 / 22 / 819 / 820 ms | 0 of 7,287 | 0 | 1.3 s | PASS |
| A | 500 | 150 | gateway-2 at +15 s | 7 / 23 / 256 ms | 6 / 33 / 505 / 505 ms | 1 of 7,275 (1 consumer) | 0 | 1.3 s | FAIL: one event lost |
| C | 1,000 | 300 | gateway-2 at +15 s | p50 12 ms / p99 1.1 s | p50 9 ms / p99 971 ms / max 2.4 s | 0 of 24,408 | 0 | 2.3 s | PASS (earlier build; did not reproduce on the final build) |
| D | 1,000 | 300 | gateway-2 at +15 s | 4,867 / 10,000 / 10,000 ms | 4,104 / 11,808 / 19,767 / 21,275 ms | 2,385 of 24,283 | 0 | 9.0 s | FAIL: saturated |
| E | 1,000 | 300 | gateway-2 at +15 s, fresh database | 4,683 / 9,037 / 10,000 ms | 2,958 / 11,377 / 20,439 / 22,657 ms | 2,102 of 24,385 | 2 | 10.9 s | FAIL: saturated |

Notes on the runs:
- **p90 was not recorded for runs C.** The simulator started printing p90 after that run.
- **Runs A and B** have the same configuration. Run B is clean; run A lost one critical event. Both are kept as recorded.
- **Run C** passed on the build before the final frame-serialization change. Later runs on the final build did not reproduce it; runs D and E failed in the same configuration.
- **Runs D and E:** slow POSTs from the first seconds of publishing. Postgres was not exhausted when sampled (login took 0.35–0.48 s, Hikari pending 0). Gateway-2 stayed at about 220% CPU while the other two gateways were idle. The root cause has not been established.
- **Redis restart** (300 connections, Redis restarted during publishing): 0 lost, 0 duplicates, 0 order violations, 0 permanently failed POSTs. Publishers received `503` with retry during the outage. SOS p99 was about 3 s for events raised during the outage.

### Server-side metrics (run A, surviving gateways, after the kill)

| Metric | gateway-1 | gateway-3 |
|---|---|---|
| `events_dropped_total{stage="outbox",type="STATUS"}` | 0 | 0 |
| `events_dropped_total{stage="outbox",type="SOS"}` | 0 | 0 |
| `outbox_forced_resync_total` (resync-required / sos-overflow) | 0 / 0 | 0 / 0 |
| `events_conflated_total` (locations superseded) | 12,107 | 10,341 |
| `pubsub_gap_repairs_total` | 7 | 5 |
| `pubsub_resyncs_total` | 0 | 0 |
| `pubsub_messages_dropped_total` | 0 | 0 |
| `redis_errors_total{op="publish"}` | — | 11 |
| `ws_disconnects_total{reason="send-timeout"}` | — | 3 |
| `ws_connections` (after kill) | 248 | 252 |

Other exported series: `sos_delivery_latency_seconds` (histogram), `pubsub_lag_seconds`, `outbox_depth`, `ingest_events_total`, `resume_total{action}`, `auth_rejections_total`, `archive_dropped_total`. Each node exposes them at its own `/actuator/prometheus` (ports 8091–8093).

## 4. Requirements against results

| Requirement | Result |
|---|---|
| Per-trip ordered delivery | Met: 0 order violations in every run except E (2) |
| Resume after disconnect, no missed STATUS/SOS | Met in all automated tests, and in load runs B and C. **Not met in run A (one event lost). Runs D and E lost thousands of events under saturation.** |
| Client dedup | Met: duplicates from resume overlap are dropped; the client sees each event once |
| SOS p99 < 2 s | Met in runs A, B and C (505 ms to 971 ms). Not met in the saturated runs D and E |
| Slow consumer does not affect others | Met in the automated isolation test. Not measured separately under load |
| Node crash, survivors take over | Met: 2.3 s recovery at 1,000 clients, 1.3 s at 500 |
| Redis restart | Met for acknowledged events |
| 10,000 connections / 2,000 cabs | **Not run.** The test machine cannot host it (see DESIGN §7) |

## 5. Known limitations

These are the findings from the load runs. They are stated here so the results are read correctly.

1. **Lost event (run A).** One STATUS/SOS event was not delivered to one consumer across a gateway kill. The likely window is between the node dying and the client's RESUME, which the resume protocol should cover. It has not been traced to a cause, so zero loss is not claimed.
2. **Saturation (runs D and E).** At 1,000 connections the machine is CPU-saturated. Ingest and SOS latency rise to seconds, and in these runs thousands of critical events were not delivered. The load concentrated on one gateway. Whether a larger machine removes this is untested.
3. **Archive inserts.** Under load, some archive batches fail on the `(trip_id, sequence_number)` unique constraint and are dropped. Publish is not affected. The planned fix is an idempotent insert.
4. **Scale.** No run exceeded 1,000 connections, and 1,000 is not a stable result on this machine.
