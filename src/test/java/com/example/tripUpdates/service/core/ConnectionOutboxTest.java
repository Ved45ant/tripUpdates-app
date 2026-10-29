package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.service.core.ConnectionOutbox.OfferResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionOutboxTest {

    private static EventEnvelope loc(String cab, long seq) {
        return new EventEnvelope("trip:t1", seq, EventType.LOCATION, cab, 0L, "{}");
    }

    private static EventEnvelope status(long seq) {
        return new EventEnvelope("trip:t1", seq, EventType.STATUS, null, 0L, "{}");
    }

    private static EventEnvelope sos(long seq) {
        return new EventEnvelope("trip:t1", seq, EventType.SOS, "cab1", 0L, "{}");
    }

    @Test
    void sosSurvivesLocationFloodAndMultipleSosAreAllKept() {
        ConnectionOutbox box = new ConnectionOutbox();
        for (int i = 1; i <= 10_000; i++) {
            box.offer(loc("cab" + (i % 50), i));
        }
        assertThat(box.offer(sos(10_001))).isEqualTo(OfferResult.ACCEPTED);
        assertThat(box.offer(sos(10_002))).isEqualTo(OfferResult.ACCEPTED);
        assertThat(box.offer(sos(10_003))).isEqualTo(OfferResult.ACCEPTED);

        List<EventEnvelope> out = box.drain();
        assertThat(out).filteredOn(e -> e.type() == EventType.SOS)
                .extracting(EventEnvelope::seq).containsExactly(10_001L, 10_002L, 10_003L);
    }

    @Test
    void criticalEventsKeepArrivalOrderAndAllPrecedeLocations() {
        ConnectionOutbox box = new ConnectionOutbox();
        box.offer(loc("cab1", 1));
        box.offer(status(2));
        box.offer(sos(3));
        box.offer(status(4));
        box.offer(loc("cab2", 5));

        List<EventEnvelope> out = box.drain();
        assertThat(out).extracting(EventEnvelope::type).containsExactly(
                EventType.STATUS, EventType.SOS, EventType.STATUS, EventType.LOCATION, EventType.LOCATION);
        assertThat(out).filteredOn(e -> e.type() != EventType.LOCATION)
                .extracting(EventEnvelope::seq).as("critical events stay in sequence order").containsExactly(2L, 3L, 4L);
        assertThat(box.isEmpty()).isTrue();
    }

    @Test
    void drainCriticalReturnsOnlyCriticalEventsAndLeavesLocations() {
        ConnectionOutbox box = new ConnectionOutbox();
        box.offer(loc("cab1", 1));
        box.offer(status(2));
        box.offer(sos(3));
        assertThat(box.drainCritical()).extracting(EventEnvelope::seq).containsExactly(2L, 3L);
        assertThat(box.hasCriticalPending()).isFalse();
        assertThat(box.size()).isEqualTo(1);
    }

    @Test
    void locationsAreConflatedToLatestPerCab() {
        ConnectionOutbox box = new ConnectionOutbox();
        for (int i = 1; i <= 1000; i++) {
            box.offer(loc("cab" + (i % 5), i));
        }
        List<EventEnvelope> out = box.drain();
        assertThat(out).hasSize(5);
        Map<String, Long> latest = new HashMap<>();
        out.forEach(e -> latest.put(e.cabId(), e.seq()));
        // last seq for cab k within 1..1000 where i % 5 == k
        assertThat(latest).containsEntry("cab0", 1000L).containsEntry("cab1", 996L)
                .containsEntry("cab2", 997L).containsEntry("cab3", 998L).containsEntry("cab4", 999L);
        assertThat(box.conflatedCount()).isEqualTo(995);
    }

    @Test
    void staleOutOfOrderLocationDoesNotReplaceNewer() {
        ConnectionOutbox box = new ConnectionOutbox();
        box.offer(loc("cab1", 10));
        assertThat(box.offer(loc("cab1", 7))).isEqualTo(OfferResult.CONFLATED);
        assertThat(box.drain()).extracting(EventEnvelope::seq).containsExactly(10L);
    }

    @Test
    void statusOverflowIsRejectedExplicitlyNeverSilentlyDropsStoredEvents() {
        ConnectionOutbox box = new ConnectionOutbox(3, 4);
        assertThat(box.offer(status(1))).isEqualTo(OfferResult.ACCEPTED);
        assertThat(box.offer(status(2))).isEqualTo(OfferResult.ACCEPTED);
        assertThat(box.offer(status(3))).isEqualTo(OfferResult.ACCEPTED);
        assertThat(box.offer(status(4))).isEqualTo(OfferResult.RESYNC_REQUIRED);
        assertThat(box.isResyncRequired()).isTrue();
        // previously accepted events are all still there, in order
        assertThat(box.drain()).extracting(EventEnvelope::seq).containsExactly(1L, 2L, 3L);
        box.clearResyncRequired();
        assertThat(box.isResyncRequired()).isFalse();
    }

    @Test
    void sosOverflowIsRejectedExplicitly() {
        ConnectionOutbox box = new ConnectionOutbox(2, 4);
        assertThat(box.offer(sos(1))).isEqualTo(OfferResult.ACCEPTED);
        assertThat(box.offer(sos(2))).isEqualTo(OfferResult.ACCEPTED);
        assertThat(box.offer(sos(3))).isEqualTo(OfferResult.CONNECTION_OVERFLOW);
        assertThat(box.isResyncRequired()).isTrue();
        assertThat(box.drain()).extracting(EventEnvelope::seq).containsExactly(1L, 2L);
    }

    @Test
    void memoryIsBoundedUnderAnyLoad() {
        ConnectionOutbox box = new ConnectionOutbox(24, 32);
        for (int i = 1; i <= 100_000; i++) {
            box.offer(loc("cab" + i, i));      // 100k distinct cabs
            box.offer(status(i));
            box.offer(sos(i));
            assertThat(box.size()).isLessThanOrEqualTo(box.maxRetainedEvents());
        }
        assertThat(box.maxRetainedEvents()).isEqualTo(24 + 32);
        assertThat(box.evictedLocationCount()).isEqualTo(100_000 - 32);
    }

    @Test
    void concurrentProducersAndDrainerLoseNoAcceptedCriticalEventAndKeepPerProducerOrder() throws Exception {
        final int producers = 8;
        final int perProducer = 20_000;
        ConnectionOutbox box = new ConnectionOutbox(320, 1024);

        AtomicInteger acceptedCritical = new AtomicInteger();
        // per-producer list of accepted critical seqs, in producer order
        List<List<Long>> accepted = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            accepted.add(new ArrayList<>());
        }
        List<EventEnvelope> drained = new ArrayList<>();
        AtomicBoolean producersDone = new AtomicBoolean(false);

        ExecutorService pool = Executors.newFixedThreadPool(producers + 1);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(producers);

        Thread drainer = new Thread(() -> {
            try {
                start.await();
                while (!producersDone.get()) {
                    drained.addAll(box.drain());
                    Thread.onSpinWait();
                }
                drained.addAll(box.drain());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        drainer.start();

        for (int p = 0; p < producers; p++) {
            final int producer = p;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 1; i <= perProducer; i++) {
                        long seq = i;
                        String topic = "trip:p" + producer;
                        EventType type = i % 3 == 0 ? EventType.SOS : i % 3 == 1 ? EventType.STATUS : EventType.LOCATION;
                        EventEnvelope e = new EventEnvelope(topic, seq, type, "cab" + producer, 0L, "{}");
                        OfferResult r = box.offer(e);
                        if (type != EventType.LOCATION && r == OfferResult.ACCEPTED) {
                            acceptedCritical.incrementAndGet();
                            synchronized (accepted.get(producer)) {
                                accepted.get(producer).add(seq);
                            }
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).as("producers finished (no deadlock)").isTrue();
        producersDone.set(true);
        drainer.join(10_000);
        pool.shutdownNow();

        // every accepted SOS/STATUS was delivered exactly once
        long drainedCritical = drained.stream().filter(e -> e.type() != EventType.LOCATION).count();
        assertThat(drainedCritical).isEqualTo(acceptedCritical.get());

        // per producer, SOS-first drain order may interleave lanes within one drain, but each lane keeps FIFO order
        for (int p = 0; p < producers; p++) {
            final String topic = "trip:p" + p;
            for (EventType lane : List.of(EventType.SOS, EventType.STATUS)) {
                List<Long> seqs = drained.stream()
                        .filter(e -> e.topic().equals(topic) && e.type() == lane)
                        .map(EventEnvelope::seq).toList();
                assertThat(seqs).isSorted();
                assertThat(seqs).doesNotHaveDuplicates();
            }
        }
        assertThat(box.size()).isZero();
    }
}
