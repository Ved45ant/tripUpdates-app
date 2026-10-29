package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.enums.EventType;

import java.util.Objects;

/**
 * Immutable, framework-agnostic unit of delivery handed to a connection outbox.
 *
 * @param topic    topic the sequence belongs to (e.g. "trip:{id}"); sequences are per topic
 * @param seq      per-topic sequence number assigned by Redis (monotonic, starts at 1)
 * @param type     LOCATION, STATUS or SOS
 * @param cabId    emitting cab; the conflation key for LOCATION events (may be null otherwise)
 * @param ingestTs epoch millis when the event entered the system (SOS latency origin)
 * @param payload  pre-serialised JSON payload, opaque to the core engine
 */
public record EventEnvelope(String topic, long seq, EventType type, String cabId, long ingestTs, String payload) {

    public EventEnvelope {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(type, "type");
        if (seq <= 0) {
            throw new IllegalArgumentException("seq must be positive");
        }
    }

    /** Conflation key: the cab for LOCATION events, falling back to the topic. */
    public String conflationKey() {
        return cabId != null ? cabId : topic;
    }
}
