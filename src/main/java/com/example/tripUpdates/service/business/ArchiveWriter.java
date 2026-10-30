package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.enums.EventType;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous, batched writer of the durable audit trail (STATUS and SOS only).
 *
 * Publishing never waits for it and never fails because of it: {@link #offer} is non-blocking and
 * rejects when the bounded queue is full (counted as dropped). A failing database is retried with
 * backoff for a bounded number of attempts per batch, after which the batch is dropped and counted.
 * Dropping is acceptable because every archived event also lives in the Redis replay buffer; the
 * archive is for audit, not for delivery.
 */
public class ArchiveWriter implements AutoCloseable {

    public record Record(String tripId, EventType type, long seq, JsonNode payload) {
    }

    public interface Sink {
        /** Must be idempotent per (tripId, seq): a retried event may be offered twice. */
        void saveBatch(List<Record> batch) throws Exception;
    }

    private static final Logger logger = LoggerFactory.getLogger(ArchiveWriter.class);

    private final Sink sink;
    private final BlockingQueue<Record> queue;
    private final int maxBatch;
    private final long flushMillis;
    private final int maxAttempts;
    private final long baseBackoffMillis;
    private final Thread worker;
    private volatile boolean running = true;

    private final AtomicLong written = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong failedAttempts = new AtomicLong();

    public ArchiveWriter(Sink sink, int capacity, int maxBatch, long flushMillis, int maxAttempts, long baseBackoffMillis) {
        this.sink = sink;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.maxBatch = maxBatch;
        this.flushMillis = flushMillis;
        this.maxAttempts = maxAttempts;
        this.baseBackoffMillis = baseBackoffMillis;
        this.worker = new Thread(this::run, "event-archiver");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    /** @return false (and counts a drop) if the queue is full */
    public boolean offer(Record record) {
        if (queue.offer(record)) {
            return true;
        }
        dropped.incrementAndGet();
        return false;
    }

    private void run() {
        List<Record> batch = new ArrayList<>(maxBatch);
        while (running || !queue.isEmpty()) {
            try {
                Record first = queue.poll(flushMillis, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.clear();
                batch.add(first);
                queue.drainTo(batch, maxBatch - 1);
                write(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void write(List<Record> batch) throws InterruptedException {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                sink.saveBatch(new ArrayList<>(batch));
                written.addAndGet(batch.size());
                return;
            } catch (Exception e) {
                failedAttempts.incrementAndGet();
                logger.warn("Archive batch of {} failed (attempt {}/{}): {}", batch.size(), attempt, maxAttempts,
                        e.getClass().getSimpleName());
                if (attempt < maxAttempts) {
                    Thread.sleep(baseBackoffMillis * attempt);
                }
            }
        }
        dropped.addAndGet(batch.size());
        logger.error("Archive batch of {} dropped after {} attempts", batch.size(), maxAttempts);
    }

    public long written() {
        return written.get();
    }

    public long dropped() {
        return dropped.get();
    }

    public long failedAttempts() {
        return failedAttempts.get();
    }

    public int queued() {
        return queue.size();
    }

    /** Stops accepting work implicitly and flushes what is queued (bounded wait). */
    @Override
    public void close() {
        running = false;
        try {
            worker.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        worker.interrupt();
    }
}
