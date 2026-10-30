# Demo — how to run and what to show (≈ 7 minutes)

Use `ENTRY_PORT=8088` if 8080 is taken; `BASE` must match it.

## Before you hit record
```bash
export ENTRY_PORT=8088 SEED_LOAD_CABS=2000        # SEED only applies to a fresh database
docker compose down -v && docker compose up -d --wait   # clean start, ~1–2 min
docker compose ps                                  # 3 gateways, nginx, redis, postgres: all healthy
export BASE=http://localhost:8088
source demo/demo.sh                                # login, cab-token, send helpers
```
Three terminals side by side: **A** = watcher (WsClient), **B** = publisher (helpers), **C** = load test / docker.

## Step 1 — Live fan-out (1 min)
A: `OP=$(login $TENANT_OP); java demo/WsClient.java ws://localhost:8088 $OP trip:$TRIP 0`
B: `location l1 12.97 77.59` · `status s1 "pickup"` · `sos o1 "help"`
Show: each frame has a `seq`; the first frame is `ACK` with the baseline seq.

## Step 2 — Tenant isolation (45 s)
```bash
EMP=$(login $EMPLOYEE); java demo/WsClient.java ws://localhost:8088 $EMP trip:12345678-1234-1234-1234-999999999999 0
```
Show `ERROR 403` (same answer for a foreign or missing trip). Then ingest with no token → `401`.
(Postman: open `../postman/TripUpdates.postman_collection.json`, run requests 1–7 in order.)

## Step 3 — Resume: the "lift for 40 seconds" (1.5 min)
1. A: Ctrl-C. Note the last STATUS/SOS `seq` printed (e.g. 5).
2. B: `status s2 "en route"` · `status s3 "near office"` · `status s4 "arrived"` · `location l2 12.98 77.60`
3. A: `java demo/WsClient.java ws://localhost:8088 $OP trip:$TRIP 5`
Show: exactly the 3 missed STATUS events in order, then the latest location. No duplicates.

## Step 4 — Node crash under live traffic (1.5 min)
C: `./loadtest/run.sh` with `CONNECTIONS=500 DESKS=10 CABS=150 DURATION=40 KILL_AT=15` (the script kills gateway-2 at +15 s).
Show during the run: `node loss detected: connected dropped 500 -> ~333`, then `all back after ~1.3 s`.
Restart the killed node afterwards: `docker start trip-gateway-2`.

## Step 5 — Results (1.5 min)
Open the result block at the end of the load run and point at:
- `critical delivery check: … missing … order violations` (target: 0 and 0)
- `SOS delivery latency … p50 / p90 / p99` (target p99 < 2 s)
- `events_dropped_total{stage="outbox"}` = 0 (server side)
Then open `../docs/VALIDATION_REPORT.md` (tables) and `docs/DESIGN.md` (architecture + failure modes).

**Do not claim zero loss on the video until the open item in VALIDATION_REPORT §5 is fixed.** Run the kill test once more right before recording; if it shows a missing event, say so and show the report instead.

## Video script (narration)

**0:00 — Problem.** "Cabs send location, status and SOS events. Employees, admins and security desks watch them live. Three rules: every status and SOS arrives, in order, even if the client drops or a server dies; a slow client must not slow anyone else; and a tenant must never see another tenant's trips."

**0:30 — Architecture.** Show `../docs/DESIGN.md` §1 diagram. "Three stateless gateways behind nginx. Redis assigns the per-trip sequence atomically in a Lua script, keeps the last 100 STATUS and SOS events for resume, and fans out over pub/sub. Each WebSocket connection has its own bounded outbox, so a frozen client only affects itself."

**1:15 — Start.** Run the start commands. "Three gateways, nginx, Redis, PostgreSQL, all healthy."

**1:45 — Live fan-out.** Run Step 1. "Every event gets a sequence number. The client's ACK tells it where the topic is."

**2:30 — Isolation.** Run Step 2. "The employee asks for a trip that is not theirs: 403. The same answer as for a trip that does not exist, so nothing can be enumerated. Ingest needs a cab-device token."

**3:15 — Resume.** Run Step 3. "The watcher drops off for a moment while three status changes happen. Reconnecting with the last sequence it saw replays exactly those three, in order, then the latest location. This is the case study's 'lift' example."

**4:45 — Node crash.** Run Step 4. "Now the real test: 500 clients, and I kill one of the three gateways in the middle. Those clients reconnect to the survivors within about a second and resume from their last sequence."

**5:45 — Numbers.** Run Step 5 and read the result block. "[Read the numbers you got.] We ran 500 connections, 150 cabs, and one node killed. I'm stating the scale honestly: this is one laptop running everything. The case study's 10,000 connections needs more hardware; the design scales by adding gateways."

**6:45 — Close.** "Architecture, failure modes, test results and the scale limits are in the repository: README, DESIGN, and VALIDATION_REPORT. Thank you."

## Quick reference: what each piece proves
| Demo step | Case-study requirement |
|---|---|
| 1 | ordered per-trip sequence, live push |
| 2 | tenant isolation, uniform 403, ingest auth |
| 3 | resume after disconnect with no missed STATUS/SOS |
| 4 | node failure: survivors take over, zero loss target |
| 5 | SOS latency p99, dropped-event counters, scale honesty |
