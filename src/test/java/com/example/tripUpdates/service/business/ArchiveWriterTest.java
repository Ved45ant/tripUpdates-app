package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.enums.EventType;
import com.fasterxml.jackson.databind.node.NullNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ArchiveWriterTest {

    private ArchiveWriter writer;

    @AfterEach
    void tearDown() {
        if (writer != null) {
            writer.close();
        }
    }

    private static ArchiveWriter.Record rec(long seq) {
        return new ArchiveWriter.Record("trip-1", EventType.STATUS, seq, NullNode.getInstance());
    }

    @Test
    void recordsAreWrittenInBatchesAndInOrder() {
        List<List<Long>> batches = Collections.synchronizedList(new ArrayList<>());
        writer = new ArchiveWriter(batch -> batches.add(batch.stream().map(ArchiveWriter.Record::seq).toList()),
                1_000, 50, 20, 3, 10);
        for (long i = 1; i <= 500; i++) {
            assertThat(writer.offer(rec(i))).isTrue();
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> writer.written() == 500);

        List<Long> flat = batches.stream().flatMap(List::stream).toList();
        assertThat(flat).hasSize(500).isSorted();
        assertThat(batches).allSatisfy(b -> assertThat(b.size()).isLessThanOrEqualTo(50));
        assertThat(batches.size()).as("batched, not one write per event").isLessThan(500);
    }

    @Test
    void fullQueueRejectsInsteadOfBlockingThePublisher() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        writer = new ArchiveWriter(batch -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 5, 1, 10, 1, 1);

        int rejected = 0;
        long start = System.nanoTime();
        for (long i = 1; i <= 200; i++) {
            if (!writer.offer(rec(i))) {
                rejected++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).as("offer never blocks").isLessThan(500);
        assertThat(rejected).isGreaterThan(150);
        assertThat(writer.dropped()).isEqualTo(rejected);
        release.countDown();
    }

    @Test
    void transientDatabaseFailuresAreRetriedAndNothingIsLost() {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger saved = new AtomicInteger();
        writer = new ArchiveWriter(batch -> {
            if (calls.incrementAndGet() <= 2) {
                throw new IllegalStateException("db down");
            }
            saved.addAndGet(batch.size());
        }, 100, 100, 10, 5, 5);

        for (long i = 1; i <= 10; i++) {
            writer.offer(rec(i));
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> writer.written() == 10);
        assertThat(saved.get()).isEqualTo(10);
        assertThat(writer.failedAttempts()).isEqualTo(2);
        assertThat(writer.dropped()).isZero();
    }

    @Test
    void aPersistentlyFailingBatchIsDroppedAndTheWriterRecoversForLaterBatches() throws Exception {
        boolean[] databaseUp = {false};
        List<Long> saved = Collections.synchronizedList(new ArrayList<>());
        writer = new ArchiveWriter(batch -> {
            if (!databaseUp[0]) {
                throw new IllegalStateException("db down");
            }
            batch.forEach(r -> saved.add(r.seq()));
        }, 100, 100, 10, 3, 5);

        writer.offer(rec(1));
        await().atMost(Duration.ofSeconds(5)).until(() -> writer.dropped() == 1);
        assertThat(writer.failedAttempts()).isEqualTo(3);

        databaseUp[0] = true;
        writer.offer(rec(2));
        await().atMost(Duration.ofSeconds(5)).until(() -> saved.contains(2L));
        assertThat(saved).containsExactly(2L);
    }

    @Test
    void closeFlushesWhatIsQueued() {
        List<Long> saved = Collections.synchronizedList(new ArrayList<>());
        writer = new ArchiveWriter(batch -> batch.forEach(r -> saved.add(r.seq())), 1_000, 10, 500, 3, 5);
        for (long i = 1; i <= 100; i++) {
            writer.offer(rec(i));
        }
        writer.close();
        assertThat(saved).hasSize(100);
    }
}
