# Load and failure simulator

`LoadSimulator.java` is a single JDK-only file (no build step). It drives the 3-node stack from `docker-compose.yml`
with the case study's scale targets and checks the guarantees end to end.

## What it does
1. Logs in as the demo admin and security operator and mints one cab-device token per cab.
2. Opens N WebSocket consumers through nginx: a few **security desks** on the office topic (they see every event of
   every trip) and the rest **trip watchers** (one trip each), using the reference client protocol:
   dedup by `(topic, seq)`, resume with the highest critical sequence (or the baseline from the first ACK),
   reconnect with jittered exponential backoff.
3. Publishes: a LOCATION per cab on an interval, a STATUS per cab every ~15 s, and a steady stream of SOS from random cabs.
4. Optionally kills a gateway container mid-run (`run.sh`, or `--kill-node=<container>` when run on the host).
5. After a settle period verifies, **per consumer**, that every acknowledged STATUS/SOS arrived (each STATUS/SOS carries a
   per-trip counter `n`), exactly once and in sequence order, and reports SOS latency (p50/p99/max, all receivers and
   desks only), ingest POST latency, reconnect behaviour and recovery time after the node loss, plus selected
   server-side Prometheus metrics of the surviving nodes.

Exit code 0 means: nothing lost, in order, SOS p99 < 2 s, no permanently failed POSTs.

## Run
```bash
# 1. start the stack once, seeding 2,000 load cabs/trips (ENTRY_PORT if 8080 is taken)
SEED_LOAD_CABS=2000 docker compose up -d --build

# 2a. recommended: simulator inside the compose network, kills gateway-2 mid-run
./loadtest/run.sh

# 2b. or on the host (goes through Docker Desktop port forwarding; fine for small runs)
java loadtest/LoadSimulator.java --base=http://localhost:8080 --connections=1000 --cabs=300 --duration=30 --kill-node=trip-gateway-2 --kill-at=15
```

Options: `--base --connections --desks --cabs --duration --kill-node --kill-at --loc-interval-ms --sos-interval-ms
--status-interval-sec --settle --metrics=<url>,<url>`.

## Notes for reading the numbers
- SOS latency is measured from the start of the successful POST attempt to the moment a consumer parsed the frame
  (same host clock). Failed attempts that were retried are not counted in the latency, but are counted as retries.
- Consumers on the killed node reconnect through nginx to the survivors; the "recovery" figure is the time until
  99.9 % of the pre-kill connections are back.
- The simulator and the whole stack share one machine; report the machine specs together with the numbers.
- Trip watchers all use the operator token (tenant-wide), which is enough to exercise fan-out, resume and ordering;
  per-role authorization is covered by the automated tests.

## Status
Validated at laptop scale. Results and known limitations: [`../docs/VALIDATION_REPORT.md`](../docs/VALIDATION_REPORT.md). Not run at 10k connections.
