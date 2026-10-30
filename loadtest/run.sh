#!/usr/bin/env bash
# Runs the load simulator INSIDE the compose network (so Docker Desktop's host port forwarding is not part of the
# measurement) and kills one gateway node in the middle of the run.
#
#   docker compose up -d --build            # with SEED_LOAD_CABS=2000 on first start, see ../docker-compose.yml
#   ./loadtest/run.sh                       # defaults: 10,000 connections, 2,000 cabs, 90 s, kill gateway-2 at +40 s
#
# Overrides (environment): CONNECTIONS DESKS CABS DURATION KILL_AT KILL_NODE SIM_HEAP NETWORK
set -euo pipefail

CONNECTIONS="${CONNECTIONS:-10000}"
DESKS="${DESKS:-50}"
CABS="${CABS:-2000}"
DURATION="${DURATION:-90}"
KILL_AT="${KILL_AT:-40}"
KILL_NODE="${KILL_NODE:-trip-gateway-2}"
SIM_HEAP="${SIM_HEAP:-1g}"
NETWORK="${NETWORK:-tripupdates_default}"

HERE="$(cd "$(dirname "$0")" && pwd)"
# Git Bash on Windows needs a Windows-style path for the volume mount
MOUNT="$(cd "$HERE" && (pwd -W 2>/dev/null || pwd))"
LOG="$(mktemp)"

export MSYS_NO_PATHCONV=1
echo "starting simulator (log: $LOG)"
docker run --rm --name trip-loadsim --network "$NETWORK" -v "$MOUNT:/lt" eclipse-temurin:21-jdk-alpine \
  java -Xmx"$SIM_HEAP" /lt/LoadSimulator.java \
    --base=http://nginx --metrics=http://gateway-1:8081,http://gateway-2:8081,http://gateway-3:8081 \
    --connections="$CONNECTIONS" --desks="$DESKS" --cabs="$CABS" --duration="$DURATION" > "$LOG" 2>&1 &
SIM_PID=$!

# wait for the publishing phase to begin, then schedule the node kill relative to it
until grep -q "PUBLISHING_STARTED" "$LOG" 2>/dev/null; do
  kill -0 "$SIM_PID" 2>/dev/null || { echo "simulator exited early:"; cat "$LOG"; exit 1; }
  sleep 1
done
echo "publishing started; killing $KILL_NODE in ${KILL_AT}s"
sleep "$KILL_AT"
docker kill "$KILL_NODE" > /dev/null && echo "killed $KILL_NODE"

wait "$SIM_PID" && STATUS=0 || STATUS=$?
cat "$LOG"
exit "$STATUS"
