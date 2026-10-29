package com.example.tripUpdates.service.core;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Fake client socket: records frames, can be slow, or frozen until closed. */
class TestSink implements FrameSink {

    final List<String> frames = new CopyOnWriteArrayList<>();
    final List<Long> frameTimesNanos = new CopyOnWriteArrayList<>();
    final AtomicBoolean open = new AtomicBoolean(true);
    final CountDownLatch closedLatch = new CountDownLatch(1);
    volatile String closeReason;
    private final long delayMillis;
    private final boolean frozen;

    private TestSink(long delayMillis, boolean frozen) {
        this.delayMillis = delayMillis;
        this.frozen = frozen;
    }

    static TestSink normal() {
        return new TestSink(0, false);
    }

    static TestSink slow(long delayMillis) {
        return new TestSink(delayMillis, false);
    }

    /** Never completes a write until the connection is closed (client stopped reading). */
    static TestSink frozen() {
        return new TestSink(0, true);
    }

    @Override
    public void send(String text) throws IOException {
        if (frozen) {
            try {
                closedLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IOException("closed while blocked");
        }
        if (delayMillis > 0) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted");
            }
        }
        if (!open.get()) {
            throw new IOException("closed");
        }
        frames.add(text);
        frameTimesNanos.add(System.nanoTime());
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void close(String reason) {
        closeReason = reason;
        open.set(false);
        closedLatch.countDown();
    }

    boolean awaitClosed(long millis) throws InterruptedException {
        return closedLatch.await(millis, TimeUnit.MILLISECONDS);
    }

    /** nanoTime at which the first frame containing {@code needle} was written, or -1. */
    long timeOfFirst(String needle) {
        for (int i = 0; i < frames.size() && i < frameTimesNanos.size(); i++) {
            if (frames.get(i).contains(needle)) {
                return frameTimesNanos.get(i);
            }
        }
        return -1;
    }

    long countFramesContaining(String needle) {
        return frames.stream().filter(f -> f.contains(needle)).count();
    }
}
