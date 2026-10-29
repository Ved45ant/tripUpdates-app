package com.example.tripUpdates.service.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routing table of the local gateway node: sessions by id and topic -> subscribed sessions.
 *
 * Fan-out lookup is O(1) plus iteration over the subscribers, and allocates nothing: it returns a
 * live, weakly consistent view of the topic's subscriber set.
 *
 * A {@link TopicListener} is told when a topic gets its first / loses its last local subscriber,
 * so the Redis layer can subscribe to a channel only while this node has someone interested.
 * Callbacks run under the per-topic lock (so first/last notifications can never be reordered) and
 * must therefore be quick and non-blocking.
 *
 * Thread-safe. Plain Java (wired as a bean in DeliveryConfig).
 */
public class ConnectionRegistry {

    public interface TopicListener {
        void onFirstSubscriber(String topic);

        void onLastUnsubscriber(String topic);
    }

    private static final Logger logger = LoggerFactory.getLogger(ConnectionRegistry.class);

    private final ConcurrentHashMap<String, ClientConnectionSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<ClientConnectionSession>> topics = new ConcurrentHashMap<>();
    private volatile TopicListener topicListener = new TopicListener() {
        @Override
        public void onFirstSubscriber(String topic) {
        }

        @Override
        public void onLastUnsubscriber(String topic) {
        }
    };

    public void setTopicListener(TopicListener listener) {
        this.topicListener = listener == null ? this.topicListener : listener;
    }

    public void register(ClientConnectionSession session) {
        if (session == null) {
            return;
        }
        ClientConnectionSession previous = sessions.put(session.getSessionId(), session);
        if (previous != null && previous != session) {
            logger.warn("Session {} re-registered, replacing previous instance", session.getSessionId());
            drop(previous); // never orphan the old instance's topic entries
        }
    }

    private void drop(ClientConnectionSession session) {
        session.markClosed();
        for (String topic : session.getSubscribedTopics()) {
            removeFromTopic(session, topic);
            session.removeSubscription(topic);
        }
    }

    /**
     * Removes the session, drops all its subscriptions and marks it closed.
     *
     * @return the removed session or null if unknown
     */
    public ClientConnectionSession unregister(String sessionId) {
        ClientConnectionSession session = sessions.remove(sessionId);
        if (session == null) {
            return null;
        }
        drop(session);
        return session;
    }

    /** Idempotent. @return false if the session is unknown/closed. */
    public boolean subscribe(String sessionId, String topic) {
        ClientConnectionSession session = sessions.get(sessionId);
        if (session == null || !session.isOpen()) {
            return false;
        }
        if (!session.addSubscription(topic)) {
            return true; // already subscribed
        }
        topics.compute(topic, (t, set) -> {
            if (set == null) {
                set = ConcurrentHashMap.newKeySet();
                set.add(session);
                topicListener.onFirstSubscriber(t);
            } else {
                set.add(session);
            }
            return set;
        });
        if (sessions.get(sessionId) != session) {
            // unregistered concurrently: undo so nothing leaks
            removeFromTopic(session, topic);
            session.removeSubscription(topic);
            return false;
        }
        return true;
    }

    /** @return true if the session was subscribed to the topic. */
    public boolean unsubscribe(String sessionId, String topic) {
        ClientConnectionSession session = sessions.get(sessionId);
        if (session == null || !session.removeSubscription(topic)) {
            return false;
        }
        removeFromTopic(session, topic);
        return true;
    }

    private void removeFromTopic(ClientConnectionSession session, String topic) {
        topics.computeIfPresent(topic, (t, set) -> {
            set.remove(session);
            if (set.isEmpty()) {
                topicListener.onLastUnsubscriber(t);
                return null;
            }
            return set;
        });
    }

    /** Live view of the subscribers of a topic (empty if none). Do not modify. */
    public Collection<ClientConnectionSession> getSubscribers(String topic) {
        Set<ClientConnectionSession> set = topics.get(topic);
        return set == null ? Collections.emptySet() : Collections.unmodifiableSet(set);
    }

    public void forEachSession(java.util.function.Consumer<ClientConnectionSession> action) {
        sessions.values().forEach(action);
    }

    public ClientConnectionSession getSession(String sessionId) {
        return sessions.get(sessionId);
    }

    public int getSessionCount() {
        return sessions.size();
    }

    public int getTopicCount() {
        return topics.size();
    }

    public int getSubscriberCount(String topic) {
        Set<ClientConnectionSession> set = topics.get(topic);
        return set == null ? 0 : set.size();
    }
}
