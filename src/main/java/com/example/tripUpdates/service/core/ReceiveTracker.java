package com.example.tripUpdates.service.core;

import java.util.Arrays;

/**
 * Tracks which sequence numbers of one topic this gateway node has received over pub/sub, to detect
 * lost messages. Redis pub/sub is at-most-once, so loss is possible (slow subscriber connection,
 * reconnect, overloaded dispatcher).
 *
 * Messages for one topic arrive over two connections (SOS channel and event channel) and may be
 * reordered relative to each other, so a missing sequence is only a <em>suspected</em> gap: the
 * caller waits a short grace period and re-checks before repairing from the replay buffer.
 *
 * The first sequence seen is the baseline (history before it is the resume flow's job).
 * Space O(window) per tracked topic; all operations O(1) amortised. Thread-safe.
 */
public class ReceiveTracker {

    public static final int DEFAULT_WINDOW = 1024;

    private final int size;
    private final boolean[] seen;
    private long highest;
    private long contiguous;

    public ReceiveTracker() {
        this(DEFAULT_WINDOW);
    }

    public ReceiveTracker(int window) {
        if (window < 2) {
            throw new IllegalArgumentException("window must be >= 2");
        }
        this.size = window;
        this.seen = new boolean[window];
    }

    public synchronized void record(long seq) {
        if (seq <= 0) {
            return;
        }
        if (highest == 0) {
            highest = seq;
            contiguous = seq;
            seen[index(seq)] = true;
            return;
        }
        if (seq > highest) {
            long advance = seq - highest;
            if (advance >= size) {
                Arrays.fill(seen, false);
            } else {
                for (long s = highest + 1; s < seq; s++) {
                    seen[index(s)] = false;
                }
            }
            seen[index(seq)] = true;
            highest = seq;
        } else if (seq > highest - size) {
            seen[index(seq)] = true;
        } else {
            return; // older than the window: nothing to learn
        }
        if (contiguous < highest - size) {
            contiguous = highest - size; // a gap older than the window is no longer repairable
        }
        while (contiguous < highest && seen[index(contiguous + 1)]) {
            contiguous++;
        }
    }

    /** True if some sequence below the highest seen has not been received. */
    public synchronized boolean hasGap() {
        return highest > contiguous;
    }

    /** Highest sequence such that everything from the baseline up to it was received (or repaired). */
    public synchronized long contiguous() {
        return contiguous;
    }

    public synchronized long highest() {
        return highest;
    }

    /** Declares everything up to the highest seen as handled (after a repair). */
    public synchronized void markRepaired() {
        contiguous = highest;
    }

    /** Moves the baseline forward (e.g. to the topic's current sequence at subscribe/reconnect). Never regresses. */
    public synchronized void advanceTo(long seq) {
        if (seq <= highest) {
            return;
        }
        Arrays.fill(seen, false);
        highest = seq;
        contiguous = seq;
    }

    private int index(long seq) {
        return (int) (seq % size);
    }
}
