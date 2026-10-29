package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.enums.EventType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded per-connection outbox with two lanes. Plain Java, no framework dependencies.
 *
 * <pre>
 *   CRITICAL  bounded FIFO holding STATUS and SOS together, never conflated, drained first
 *   LOCATION  latest event per cab (conflation), drained last
 * </pre>
 *
 * Why one critical lane: critical events of a topic reach a connection in sequence order (one Redis channel, one
 * processing thread, one FIFO), so a client can resume from "the highest critical sequence I have" without ever
 * skipping an event that was still in flight. SOS still jumps ahead of the traffic that matters for latency, all
 * LOCATION updates, both inside the outbox (critical is drained before locations) and between socket writes
 * ({@link #drainCritical()}).
 *
 * Guarantees:
 * <ul>
 *   <li>SOS and STATUS events are never silently dropped. If the lane is full the event is rejected with an
 *       explicit result and the outbox is flagged, so the caller closes the connection and the client recovers
 *       from the replay buffer.</li>
 *   <li>LOCATION events are conflated to the newest per cab.</li>
 *   <li>Memory is bounded: at most {@code criticalCapacity + locationCapacity} events per connection.</li>
 * </ul>
 *
 * All methods are thread-safe (one intrinsic lock; critical sections are O(1) per offer).
 */
public class ConnectionOutbox {

    public static final int DEFAULT_CRITICAL_CAPACITY = 320;
    public static final int DEFAULT_LOCATION_CAPACITY = 1024;

    public enum OfferResult {
        /** Stored (or, for LOCATION, stored as a new cab entry). */
        ACCEPTED,
        /** LOCATION replaced an older pending update for the same cab, or was stale and ignored. */
        CONFLATED,
        /** Critical lane full while offering a STATUS; the event was NOT stored. Connection must resync. */
        RESYNC_REQUIRED,
        /** Critical lane full while offering an SOS; the event was NOT stored. Connection must be closed and resumed. */
        CONNECTION_OVERFLOW
    }

    private final int criticalCapacity;
    private final int locationCapacity;

    private final ArrayDeque<EventEnvelope> criticalLane;
    private final LinkedHashMap<String, EventEnvelope> locationLane = new LinkedHashMap<>();

    private boolean resyncRequired;
    private long conflatedCount;
    private long evictedLocationCount;

    public ConnectionOutbox() {
        this(DEFAULT_CRITICAL_CAPACITY, DEFAULT_LOCATION_CAPACITY);
    }

    public ConnectionOutbox(int criticalCapacity, int locationCapacity) {
        if (criticalCapacity < 1 || locationCapacity < 1) {
            throw new IllegalArgumentException("lane capacities must be >= 1");
        }
        this.criticalCapacity = criticalCapacity;
        this.locationCapacity = locationCapacity;
        this.criticalLane = new ArrayDeque<>(Math.min(criticalCapacity, 16));
    }

    public synchronized OfferResult offer(EventEnvelope event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        EventType type = event.type();
        switch (type) {
            case SOS:
            case STATUS:
                if (criticalLane.size() >= criticalCapacity) {
                    resyncRequired = true;
                    return type == EventType.SOS ? OfferResult.CONNECTION_OVERFLOW : OfferResult.RESYNC_REQUIRED;
                }
                criticalLane.addLast(event);
                return OfferResult.ACCEPTED;

            case LOCATION:
                return offerLocation(event);

            default:
                throw new IllegalStateException("Unhandled event type " + type);
        }
    }

    private OfferResult offerLocation(EventEnvelope event) {
        String key = event.conflationKey();
        EventEnvelope existing = locationLane.get(key);
        if (existing != null) {
            conflatedCount++;
            if (existing.seq() >= event.seq()) {
                return OfferResult.CONFLATED; // out-of-order stale update: keep the newer one
            }
            locationLane.remove(key); // re-insert so insertion order tracks recency
            locationLane.put(key, event);
            return OfferResult.CONFLATED;
        }
        if (locationLane.size() >= locationCapacity) {
            Iterator<Map.Entry<String, EventEnvelope>> eldest = locationLane.entrySet().iterator();
            eldest.next();
            eldest.remove(); // bound memory: drop the least recently updated cab
            evictedLocationCount++;
        }
        locationLane.put(key, event);
        return OfferResult.ACCEPTED;
    }

    /**
     * Removes and returns everything pending in delivery order: all critical events (arrival order), then the
     * latest location per cab.
     */
    public synchronized List<EventEnvelope> drain() {
        int total = criticalLane.size() + locationLane.size();
        if (total == 0) {
            return List.of();
        }
        List<EventEnvelope> out = new ArrayList<>(total);
        out.addAll(criticalLane);
        out.addAll(locationLane.values());
        criticalLane.clear();
        locationLane.clear();
        return out;
    }

    /**
     * Removes and returns only the pending critical events. The writer calls this between sends so a freshly
     * arrived SOS or STATUS never waits behind a location backlog that was already drained.
     */
    public synchronized List<EventEnvelope> drainCritical() {
        if (criticalLane.isEmpty()) {
            return List.of();
        }
        List<EventEnvelope> out = new ArrayList<>(criticalLane);
        criticalLane.clear();
        return out;
    }

    /** Number of events currently retained (memory footprint in events). */
    public synchronized int size() {
        return criticalLane.size() + locationLane.size();
    }

    public synchronized boolean isEmpty() {
        return size() == 0;
    }

    public synchronized boolean hasCriticalPending() {
        return !criticalLane.isEmpty();
    }

    /** True once a critical event had to be rejected; cleared by {@link #clearResyncRequired()}. */
    public synchronized boolean isResyncRequired() {
        return resyncRequired;
    }

    public synchronized void clearResyncRequired() {
        resyncRequired = false;
    }

    public synchronized long conflatedCount() {
        return conflatedCount;
    }

    public synchronized long evictedLocationCount() {
        return evictedLocationCount;
    }

    /** Upper bound on retained events regardless of load. */
    public int maxRetainedEvents() {
        return criticalCapacity + locationCapacity;
    }

    /** Drops everything pending (used when a connection is closed). */
    public synchronized void clear() {
        criticalLane.clear();
        locationLane.clear();
        resyncRequired = false;
    }
}
