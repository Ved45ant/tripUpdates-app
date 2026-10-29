package com.example.tripUpdates.service.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionRegistryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private ClientConnectionSession session(String id) {
        return new ClientConnectionSession(id, TestSink.normal(), mapper);
    }

    @Test
    void subscribeRoutesAndUnsubscribeCleansUp() {
        ConnectionRegistry registry = new ConnectionRegistry();
        ClientConnectionSession a = session("a");
        ClientConnectionSession b = session("b");
        registry.register(a);
        registry.register(b);

        assertThat(registry.subscribe("a", "trip:1")).isTrue();
        assertThat(registry.subscribe("b", "trip:1")).isTrue();
        assertThat(registry.subscribe("b", "trip:1")).as("idempotent").isTrue();
        assertThat(registry.getSubscribers("trip:1")).containsExactlyInAnyOrder(a, b);
        assertThat(registry.getSubscriberCount("trip:1")).isEqualTo(2);

        assertThat(registry.unsubscribe("a", "trip:1")).isTrue();
        assertThat(registry.unsubscribe("a", "trip:1")).as("not subscribed any more").isFalse();
        assertThat(registry.getSubscribers("trip:1")).containsExactly(b);

        registry.unsubscribe("b", "trip:1");
        assertThat(registry.getTopicCount()).isZero();
        assertThat(registry.getSubscribers("trip:1")).isEmpty();
    }

    @Test
    void unregisterRemovesEveryTopicEntryAndClosesSession() {
        ConnectionRegistry registry = new ConnectionRegistry();
        ClientConnectionSession a = session("a");
        registry.register(a);
        for (int i = 0; i < 50; i++) {
            registry.subscribe("a", "trip:" + i);
        }
        assertThat(registry.getTopicCount()).isEqualTo(50);

        assertThat(registry.unregister("a")).isSameAs(a);
        assertThat(registry.getTopicCount()).isZero();
        assertThat(registry.getSessionCount()).isZero();
        assertThat(a.isOpen()).isFalse();
        assertThat(registry.subscribe("a", "trip:1")).as("unknown session").isFalse();
        assertThat(registry.unregister("a")).isNull();
    }

    @Test
    void topicListenerFiresOnFirstAndLastSubscriberOnly() {
        ConnectionRegistry registry = new ConnectionRegistry();
        List<String> calls = new ArrayList<>();
        registry.setTopicListener(new ConnectionRegistry.TopicListener() {
            public void onFirstSubscriber(String t) {
                calls.add("first:" + t);
            }

            public void onLastUnsubscriber(String t) {
                calls.add("last:" + t);
            }
        });
        registry.register(session("a"));
        registry.register(session("b"));

        registry.subscribe("a", "cab:9");
        registry.subscribe("b", "cab:9");
        registry.unsubscribe("a", "cab:9");
        assertThat(calls).containsExactly("first:cab:9");
        registry.unregister("b");
        assertThat(calls).containsExactly("first:cab:9", "last:cab:9");
    }

    @Test
    void concurrentSubscribeUnsubscribeUnregisterNeverLeaksAndListenerStaysBalanced() throws Exception {
        final int sessionsCount = 64;
        final int topicCount = 8;
        final int threads = 16;

        ConnectionRegistry registry = new ConnectionRegistry();
        Map<String, AtomicInteger> active = new ConcurrentHashMap<>(); // +1 first, -1 last
        AtomicInteger violations = new AtomicInteger();
        registry.setTopicListener(new ConnectionRegistry.TopicListener() {
            public void onFirstSubscriber(String t) {
                if (active.computeIfAbsent(t, k -> new AtomicInteger()).incrementAndGet() != 1) {
                    violations.incrementAndGet();
                }
            }

            public void onLastUnsubscriber(String t) {
                if (active.computeIfAbsent(t, k -> new AtomicInteger()).decrementAndGet() != 0) {
                    violations.incrementAndGet();
                }
            }
        });

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    for (int i = 0; i < 20_000; i++) {
                        String id = "s" + rnd.nextInt(sessionsCount);
                        String topic = "trip:" + rnd.nextInt(topicCount);
                        switch (rnd.nextInt(5)) {
                            case 0 -> registry.register(new ClientConnectionSession(id, TestSink.normal(), mapper));
                            case 1, 2 -> registry.subscribe(id, topic);
                            case 3 -> registry.unsubscribe(id, topic);
                            default -> registry.unregister(id);
                        }
                        for (ClientConnectionSession s : registry.getSubscribers(topic)) {
                            s.getSessionId(); // concurrent fan-out style iteration must not throw
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        for (int s = 0; s < sessionsCount; s++) {
            registry.unregister("s" + s);
        }
        assertThat(violations.get()).as("first/last notifications strictly alternate per topic").isZero();
        assertThat(registry.getTopicCount()).as("no leaked topic entries").isZero();
        assertThat(registry.getSessionCount()).isZero();
        active.values().forEach(v -> assertThat(v.get()).isZero());
    }
}
