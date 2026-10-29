package com.example.tripUpdates.service.core;

import com.example.tripUpdates.service.core.ConnectionOutbox.OfferResult;

/**
 * Node-local fan-out: topic -> subscribed sessions -> outbox -> wake the writer.
 *
 * Never blocks and never touches a socket, so the caller (a Redis listener thread) cannot be
 * slowed by any client. Cost is O(S) in the number of local subscribers of the topic.
 *
 * Outbox rejections are not silent: a full SOS or STATUS lane closes the connection so the client
 * reconnects and recovers the missed critical events from the replay buffer.
 */
public class EventFanOut {

    private final ConnectionRegistry registry;
    private final ConnectionWriter writer;
    private final DeliveryObserver observer;

    public EventFanOut(ConnectionRegistry registry, ConnectionWriter writer, DeliveryObserver observer) {
        this.registry = registry;
        this.writer = writer;
        this.observer = observer == null ? DeliveryObserver.NOOP : observer;
    }

    /** @return number of local subscribers the event was offered to. */
    public int fanOut(EventEnvelope event) {
        int offered = 0;
        for (ClientConnectionSession session : registry.getSubscribers(event.topic())) {
            if (deliverTo(session, event)) {
                offered++;
            }
        }
        return offered;
    }

    /**
     * Closes every local subscriber of the topic so they reconnect and resume (which gives them a
     * snapshot). Used when critical events were lost beyond what the replay buffer can repair.
     *
     * @return number of connections closed
     */
    public int resyncAll(String topic) {
        int closed = 0;
        for (ClientConnectionSession session : registry.getSubscribers(topic)) {
            if (session.isOpen()) {
                writer.disconnect(session, "resync-required");
                closed++;
            }
        }
        return closed;
    }

    /** Queues one event for one session (also used for resume replay). */
    public boolean deliverTo(ClientConnectionSession session, EventEnvelope event) {
        if (!session.isOpen()) {
            return false;
        }
        if (session.parkIfBuffering(event)) {
            return true; // a replay is in progress for this topic: live events wait behind it
        }
        return offer(session, event);
    }

    /** Queues a replayed / parked event, bypassing the live-event buffering of the session. */
    public boolean deliverReplay(ClientConnectionSession session, EventEnvelope event) {
        return session.isOpen() && offer(session, event);
    }

    private boolean offer(ClientConnectionSession session, EventEnvelope event) {
        OfferResult result = session.getOutbox().offer(event);
        switch (result) {
            case ACCEPTED:
                writer.wake(session);
                return true;
            case CONFLATED:
                observer.conflated();
                writer.wake(session);
                return true;
            case RESYNC_REQUIRED:
                writer.disconnect(session, "resync-required");
                return false;
            case CONNECTION_OVERFLOW:
                writer.disconnect(session, "sos-overflow");
                return false;
            default:
                throw new IllegalStateException("Unhandled offer result " + result);
        }
    }
}
