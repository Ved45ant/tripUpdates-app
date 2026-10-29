package com.example.tripUpdates.repository.redis;

import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.service.business.TopicPublisher;
import com.example.tripUpdates.service.core.EnvelopeJson;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Publishes one event to one topic through {@code scripts/publish.lua}: idempotency check, per-topic
 * sequence (Redis is the single sequence assigner), critical replay buffer, latest-state snapshot
 * and pub/sub PUBLISH, all atomically. One round trip, O(log N) for the buffer insert.
 *
 * STATUS and SOS (critical events) go out on a dedicated {@code sos:} channel, so they are consumed on their
 * own connection and thread, in sequence order, and never queue behind location traffic.
 */
@Repository
public class RedisEventPublisher implements TopicPublisher {

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<List> script;
    private final int maxCriticalEvents;
    private final long dedupWindowSeconds;
    private final long keyTtlSeconds;

    public RedisEventPublisher(StringRedisTemplate redis,
                               @Value("${replay.buffer-size:100}") int maxCriticalEvents,
                               @Value("${replay.dedup-window-seconds:300}") long dedupWindowSeconds,
                               @Value("${replay.key-ttl-seconds:604800}") long keyTtlSeconds) {
        this.redis = redis;
        this.maxCriticalEvents = maxCriticalEvents;
        this.dedupWindowSeconds = dedupWindowSeconds;
        this.keyTtlSeconds = keyTtlSeconds;
        this.script = new DefaultRedisScript<>();
        this.script.setLocation(new ClassPathResource("scripts/publish.lua"));
        this.script.setResultType(List.class);
    }

    /**
     * @param topic       topic to publish to
     * @param eventId     idempotency key (a retry with the same id returns the original seq); random if null
     * @param cabId       emitting cab (conflation key for locations)
     * @param tripId      used to name the snapshot field of STATUS/SOS events
     * @param payloadJson event body as JSON
     * @throws org.springframework.data.redis.RedisSystemException (or subclass) if Redis is unavailable
     */
    public PublishResult publish(String topic, EventType type, String eventId, String cabId, String tripId,
                                 long ingestTs, String payloadJson) {
        return publish(topic, type, eventId, cabId, tripId, ingestTs, ingestTs, payloadJson, false);
    }

    @Override
    public PublishResult publish(String topic, EventType type, String eventId, String cabId, String tripId,
                                 long ingestTs, long eventTimestamp, String payloadJson, boolean guardStale) {
        String id = eventId != null ? eventId : UUID.randomUUID().toString();
        String prefix;
        try {
            prefix = EnvelopeJson.prefix(topic, type, cabId, ingestTs, payloadJson);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("payload is not valid JSON", e);
        }
        String channel = type == EventType.LOCATION ? RedisKeys.eventChannel(topic) : RedisKeys.sosChannel(topic);

        List<?> result = redis.execute(script,
                List.of(RedisKeys.seq(topic), RedisKeys.criticalBuffer(topic), RedisKeys.criticalFloor(topic),
                        RedisKeys.dedup(topic, id), RedisKeys.snapshot(topic), RedisKeys.lastLocation(String.valueOf(cabId))),
                type.name(), channel, String.valueOf(maxCriticalEvents), String.valueOf(dedupWindowSeconds),
                String.valueOf(keyTtlSeconds), prefix, snapshotField(type, cabId, tripId),
                String.valueOf(eventTimestamp), guardStale ? "1" : "0");

        long flag = ((Number) result.get(1)).longValue();
        return new PublishResult(((Number) result.get(0)).longValue(), flag == 1L, flag == 2L);
    }

    private static String snapshotField(EventType type, String cabId, String tripId) {
        return switch (type) {
            case LOCATION -> "loc:" + cabId;
            case STATUS -> "status:" + tripId;
            case SOS -> "sos:" + tripId;
        };
    }
}
