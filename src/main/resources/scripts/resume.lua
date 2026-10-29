-- Consistent read for a resuming client: current seq, critical floor, the buffered STATUS/SOS
-- events newer than lastSeq (in seq order) and the latest-state snapshot, in one atomic step.
--
-- KEYS[1] seq:{topic}  KEYS[2] buf:{topic}:crit  KEYS[3] floor:{topic}  KEYS[4] snap:{topic}
-- ARGV[1] lastSeq
--
-- Returns {currentSeq, critFloor, {eventJson...}, {field, json, field, json, ...}}

local seq = tonumber(redis.call('GET', KEYS[1]) or '0')
local floor = tonumber(redis.call('GET', KEYS[3]) or '0')
local events = redis.call('ZRANGEBYSCORE', KEYS[2], '(' .. ARGV[1], '+inf')
local snap = redis.call('HGETALL', KEYS[4])
return {seq, floor, events, snap}
