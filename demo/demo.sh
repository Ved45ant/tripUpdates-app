#!/usr/bin/env bash
# Helper commands for the demo. Usage: source demo/demo.sh   (then call the functions)
#   BASE   entry point (default http://localhost:8080)
BASE="${BASE:-http://localhost:8080}"
TENANT_OP=12345678-1234-1234-1234-123456789017   # security operator
EMPLOYEE=12345678-1234-1234-1234-123456789015    # passenger of the demo trip
CAB=12345678-1234-1234-1234-123456789014
TRIP=12345678-1234-1234-1234-123456789018
OFFICE=12345678-1234-1234-1234-123456789013

_json() { python -c "import sys,json;print(json.load(sys.stdin)['$1'])"; }
login()    { curl -s -H 'Content-Type: application/json' -d "{\"username\":\"$1\",\"password\":\"password123\"}" "$BASE/api/v1/auth/login" | _json token; }
cab_token(){ curl -s -H "Authorization: Bearer $(login $TENANT_OP)" -H 'Content-Type: application/json' -d "{\"cabId\":\"$CAB\"}" "$BASE/api/v1/auth/cab-token" | _json token; }

# send <status|sos|location> <eventId> '<payload json>'
send() {
  local kind="$1" id="$2" payload="$3" type; type=$(echo "$kind" | tr a-z A-Z)
  curl -s -o /dev/null -w "$kind $id -> HTTP %{http_code}\n" -H "Authorization: Bearer ${CABTOKEN:=$(cab_token)}" -H 'Content-Type: application/json' \
    -d "{\"eventType\":\"$type\",\"eventId\":\"$id\",\"cabId\":\"$CAB\",\"tripId\":\"$TRIP\",\"timestamp\":$(date +%s%3N),\"payload\":$payload}" "$BASE/api/v1/events/$kind"
}
status()   { send status "$1" "{\"status\":\"ACTIVE\",\"reason\":\"$2\"}"; }
sos()      { send sos "$1" "{\"level\":\"HIGH\",\"message\":\"$2\",\"lat\":12.9,\"lng\":77.5}"; }
location() { send location "$1" "{\"lat\":$2,\"lng\":$3}"; }
