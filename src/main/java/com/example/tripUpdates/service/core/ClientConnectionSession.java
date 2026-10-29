package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.ws.WsOutboundFrame;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Server-side state of one client connection: the socket (via {@link FrameSink}), its bounded
 * {@link ConnectionOutbox}, its topic subscriptions and a per-topic {@link SeenWindow} that
 * removes duplicates (e.g. replay/live overlap) before anything is written.
 *
 * Writes are serialised by {@link #send(String)}; event delivery is driven by
 * {@link ConnectionWriter}, control frames (ACK/ERROR/SNAPSHOT) go through
 * {@link #sendMessage(WsOutboundFrame)}.
 */
public class ClientConnectionSession {

    private static final Logger logger = LoggerFactory.getLogger(ClientConnectionSession.class);

    private final String sessionId;
    private final FrameSink sink;
    private final ObjectMapper objectMapper;
    private final ConnectionOutbox outbox;
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, SeenWindow> seenByTopic = new ConcurrentHashMap<>();
    /** A ReentrantLock, not synchronized: the blocking socket write inside must not pin a virtual-thread carrier. */
    private final ReentrantLock sendLock = new ReentrantLock();
    private final long createdAtMillis = System.currentTimeMillis();

    /** Live events parked while a SUBSCRIBE/RESUME is replaying, so replay always precedes live. */
    private static final class Parked {
        final List<EventEnvelope> events = new ArrayList<>();
        boolean overflowed;
        boolean finished;
    }

    public static final int MAX_PARKED_EVENTS = 2000;

    private final ConcurrentHashMap<String, Parked> buffering = new ConcurrentHashMap<>();

    private volatile boolean closed;
    private volatile long expiresAtMillis; // 0 = no expiry
    private volatile long lastActivityMillis = createdAtMillis;

    // Used by ConnectionWriter only
    final AtomicBoolean writerScheduled = new AtomicBoolean(false);
    volatile long sendStartedNanos;
    volatile Thread sendingThread;

    public ClientConnectionSession(String sessionId, FrameSink sink, ObjectMapper objectMapper) {
        this(sessionId, sink, objectMapper, new ConnectionOutbox());
    }

    public ClientConnectionSession(String sessionId, FrameSink sink, ObjectMapper objectMapper,
                                   ConnectionOutbox outbox) {
        this.sessionId = sessionId;
        this.sink = sink;
        this.objectMapper = objectMapper;
        this.outbox = outbox;
    }

    /** Writes one text frame, serialised against other writers of this connection. */
    public boolean send(String text) {
        sendLock.lock();
        try {
            if (closed) {
                return false;
            }
            try {
                sink.send(text);
                lastActivityMillis = System.currentTimeMillis();
                return true;
            } catch (IOException | RuntimeException e) {
                logger.debug("Send failed for session {}: {}", sessionId, e.getClass().getSimpleName());
                return false;
            }
        } finally {
            sendLock.unlock();
        }
    }

    /** Serialises and writes a control frame (ACK, ERROR, SNAPSHOT). */
    public boolean sendMessage(WsOutboundFrame frame) {
        try {
            return send(objectMapper.writeValueAsString(frame));
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize frame for session {}", sessionId);
            return false;
        }
    }

    /** True the first time the topic is added. */
    boolean addSubscription(String topic) {
        boolean added = subscribedTopics.add(topic);
        if (added) {
            seenByTopic.put(topic, new SeenWindow());
        }
        lastActivityMillis = System.currentTimeMillis();
        return added;
    }

    /** True if the topic had been subscribed. */
    boolean removeSubscription(String topic) {
        boolean removed = subscribedTopics.remove(topic);
        seenByTopic.remove(topic);
        lastActivityMillis = System.currentTimeMillis();
        return removed;
    }

    /** Begins buffering live events of the topic; call before registering the subscription. */
    public void startBuffering(String topic) {
        buffering.put(topic, new Parked());
    }

    /** @return true if the event was parked because a replay for its topic is in progress */
    boolean parkIfBuffering(EventEnvelope event) {
        Parked parked = buffering.get(event.topic());
        if (parked == null) {
            return false;
        }
        synchronized (parked) {
            if (parked.finished) {
                return false;
            }
            if (parked.events.size() >= MAX_PARKED_EVENTS) {
                parked.overflowed = true;
            } else {
                parked.events.add(event);
            }
            return true;
        }
    }

    /**
     * Ends buffering and hands the parked live events to {@code deliver} (null to discard them).
     * Runs under the buffer's lock and keeps the entry in place until done, so any live event arriving
     * meanwhile waits and is delivered after the parked ones: order is replay, parked, then new live.
     *
     * @return true if too many events were parked (the connection should be closed to resync)
     */
    public boolean finishBuffering(String topic, Consumer<EventEnvelope> deliver) {
        Parked parked = buffering.get(topic);
        if (parked == null) {
            return false;
        }
        synchronized (parked) {
            if (deliver != null && !parked.overflowed) {
                parked.events.forEach(deliver);
            }
            parked.finished = true;
            buffering.remove(topic, parked);
            return parked.overflowed;
        }
    }

    public void setExpiresAtMillis(long expiresAtMillis) {
        this.expiresAtMillis = expiresAtMillis;
    }

    public boolean isExpired(long nowMillis) {
        return expiresAtMillis > 0 && nowMillis >= expiresAtMillis;
    }

    /** Closes the connection because this node is shutting down; the client reconnects elsewhere and resumes. */
    public void closeForShutdown() {
        if (markClosed()) {
            sink.close("server-shutdown");
        }
    }

    /** Closes the connection because its token expired (client must log in again). */
    public void closeBecauseExpired() {
        if (markClosed()) {
            sink.closeUnauthorized("token-expired");
        }
    }

    /** Dedup gate applied by the writer immediately before a send. */
    boolean acceptForDelivery(EventEnvelope event) {
        SeenWindow window = seenByTopic.get(event.topic());
        return window != null && window.markIfNew(event.seq());
    }

    /** Marks the connection closed (idempotent). @return true if this call closed it. */
    boolean markClosed() {
        if (closed) {
            return false;
        }
        closed = true;
        outbox.clear();
        return true;
    }

    FrameSink sink() {
        return sink;
    }

    public Set<String> getSubscribedTopics() {
        return Set.copyOf(subscribedTopics);
    }

    public boolean isSubscribedTo(String topic) {
        return subscribedTopics.contains(topic);
    }

    public int getSubscriptionCount() {
        return subscribedTopics.size();
    }

    public ConnectionOutbox getOutbox() {
        return outbox;
    }

    public String getSessionId() {
        return sessionId;
    }

    public boolean isOpen() {
        return !closed && sink.isOpen();
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public long getLastActivityMillis() {
        return lastActivityMillis;
    }

    public int getQueueSize() {
        return outbox.size();
    }

    @Override
    public String toString() {
        return "ClientConnectionSession{sessionId='" + sessionId + "', subscriptions="
                + subscribedTopics.size() + ", queueSize=" + outbox.size() + ", open=" + isOpen() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return sessionId.equals(((ClientConnectionSession) o).sessionId);
    }

    @Override
    public int hashCode() {
        return sessionId.hashCode();
    }
}
