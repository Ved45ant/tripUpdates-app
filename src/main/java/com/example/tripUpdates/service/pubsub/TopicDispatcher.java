package com.example.tripUpdates.service.pubsub;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Runs per-topic work on a fixed set of single-threaded, bounded stripes: all messages of one topic
 * go to the same thread (so they are processed in arrival order) while different topics proceed in
 * parallel. A full stripe rejects work instead of blocking the caller (the Redis I/O thread);
 * rejected messages are repaired later by gap detection.
 */
public class TopicDispatcher implements AutoCloseable {

    private final ThreadPoolExecutor[] stripes;

    public TopicDispatcher(int stripeCount, int queueCapacityPerStripe, String threadNamePrefix) {
        this.stripes = new ThreadPoolExecutor[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            final int id = i;
            stripes[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(queueCapacityPerStripe),
                    r -> {
                        Thread t = new Thread(r, threadNamePrefix + "-" + id);
                        t.setDaemon(true);
                        return t;
                    },
                    new ThreadPoolExecutor.AbortPolicy());
        }
    }

    /** @return false if the stripe's queue is full and the task was not accepted */
    public boolean execute(String topic, Runnable task) {
        try {
            stripes[Math.floorMod(topic.hashCode(), stripes.length)].execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    public int queuedTasks() {
        int total = 0;
        for (ThreadPoolExecutor s : stripes) {
            total += s.getQueue().size();
        }
        return total;
    }

    @Override
    public void close() {
        for (ThreadPoolExecutor s : stripes) {
            s.shutdownNow();
        }
    }
}
