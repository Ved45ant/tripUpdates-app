package com.example.tripUpdates.service.core;

/** Telemetry hooks for the delivery path; the core engine stays independent of any metrics library. */
public interface DeliveryObserver {

    /** An SOS frame was written to a socket; {@code latencyMillis} is measured from ingest time. */
    default void sosDelivered(long latencyMillis) {
    }

    /** A pending LOCATION update was replaced/ignored in favour of a newer one for the same cab. */
    default void conflated() {
    }

    /** A connection was force-closed (reason e.g. "send-timeout", "sos-overflow", "resync-required"). */
    default void disconnected(String reason) {
    }

    /** A writer drained {@code depth} pending events from one connection's outbox. */
    default void outboxDepth(int depth) {
    }

    DeliveryObserver NOOP = new DeliveryObserver() {
    };
}
