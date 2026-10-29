-- Atomic publish of one event to one topic.
-- Sequencing, replay buffer, snapshot and PUBLISH happen in a single script, so for a topic
-- "sequence order == publish order" even with many concurrent publishers.
--
-- KEYS[1] sequence counter        (seq:{topic})
-- KEYS[2] critical replay buffer  (buf:{topic}:crit, ZSET scored by seq, STATUS/SOS only)
-- KEYS[3] critical floor          (floor:{topic}, highest STATUS/SOS seq evicted from the buffer)
-- KEYS[4] idempotency key         (dedup:{topic}:{eventId})
-- KEYS[5] latest-state snapshot   (snap:{topic}, HASH)
-- KEYS[6] latest accepted location time of the cab (lastloc:{cabId}), only used when ARGV[9] == '1'
-- KEYS[6] latest accepted location time of the cab (lastloc:{cabId}), only used when ARGV[9] == '1'
--
-- ARGV[1] event type (LOCATION | STATUS | SOS)
-- ARGV[2] pub/sub channel
-- ARGV[3] max critical events kept in the buffer
-- ARGV[4] idempotency window, seconds
-- ARGV[5] key TTL, seconds (refreshed on every publish)
-- ARGV[6] message JSON without the closing brace; the script appends ,"seq":N}
-- ARGV[7] snapshot hash field for this event
-- ARGV[8] cab-supplied event timestamp (ms)
-- ARGV[9] '1' to drop a LOCATION that is older than the latest accepted one for the cab
--
-- Returns {seq, flag}: flag 0 = published, 1 = duplicate (eventId already published to this topic;
-- the original seq is returned, nothing is published again), 2 = stale location (dropped, seq 0).

local existing = redis.call('GET', KEYS[4])
if existing then
  return {tonumber(existing), 1}
end

local ttl = tonumber(ARGV[5])

if ARGV[9] == '1' and ARGV[1] == 'LOCATION' then
  local prev = redis.call('GET', KEYS[6])
  if prev and tonumber(prev) > tonumber(ARGV[8]) then
    return {0, 2}
  end
  redis.call('SET', KEYS[6], ARGV[8], 'EX', ttl)
end

local seq = redis.call('INCR', KEYS[1])
local msg = ARGV[6] .. ',"seq":' .. seq .. '}'

redis.call('SET', KEYS[4], seq, 'EX', tonumber(ARGV[4]))

if ARGV[1] ~= 'LOCATION' then
  redis.call('ZADD', KEYS[2], seq, msg)
  local over = redis.call('ZCARD', KEYS[2]) - tonumber(ARGV[3])
  if over > 0 then
    local evicted = redis.call('ZRANGE', KEYS[2], 0, over - 1, 'WITHSCORES')
    local top = tonumber(evicted[#evicted])
    local floor = tonumber(redis.call('GET', KEYS[3]) or '0')
    if top > floor then
      redis.call('SET', KEYS[3], top)
    end
    redis.call('ZREMRANGEBYRANK', KEYS[2], 0, over - 1)
  end
  redis.call('EXPIRE', KEYS[2], ttl)
  redis.call('EXPIRE', KEYS[3], ttl)
end

redis.call('HSET', KEYS[5], ARGV[7], msg)
redis.call('EXPIRE', KEYS[5], ttl)
redis.call('EXPIRE', KEYS[1], ttl)

redis.call('PUBLISH', ARGV[2], msg)
return {seq, 0}
