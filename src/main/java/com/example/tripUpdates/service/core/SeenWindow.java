package com.example.tripUpdates.service.core;

/**
 * Sliding-window duplicate filter over per-topic sequence numbers.
 *
 * A plain "highest seq seen" check is not enough: SOS events may overtake lower-sequence
 * STATUS/LOCATION events, so a legitimate lower seq can arrive after a higher one. This window
 * remembers exactly which of the last {@code size} sequences were seen.
 *
 * <ul>
 *   <li>seq already seen            -> duplicate (false)</li>
 *   <li>seq newer than anything seen -> new, window slides forward (true)</li>
 *   <li>seq inside window, unseen    -> late/out-of-order but new (true)</li>
 *   <li>seq older than the window    -> cannot be verified, treated as duplicate (false)</li>
 * </ul>
 *
 * Space: O(size) bits. Time: O(1) amortised per call (O(size) on a jump larger than the window).
 * Thread-safe. This is also the reference dedup algorithm for clients.
 */
public class SeenWindow {

    public static final int DEFAULT_SIZE = 1024;

    private final int size;
    private final boolean[] seen;
    private long highest;

    public SeenWindow() {
        this(DEFAULT_SIZE);
    }

    public SeenWindow(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("size must be >= 1");
        }
        this.size = size;
        this.seen = new boolean[size];
        this.highest = 0;
    }

    /** @return true if {@code seq} had not been seen (and is now recorded), false if duplicate/too old. */
    public synchronized boolean markIfNew(long seq) {
        if (seq <= 0) {
            return false;
        }
        if (seq > highest) {
            long advance = seq - highest;
            if (advance >= size) {
                java.util.Arrays.fill(seen, false);
            } else {
                for (long s = highest + 1; s < seq; s++) {
                    seen[index(s)] = false; // slots for skipped seqs become "unseen" and reusable
                }
            }
            seen[index(seq)] = true;
            highest = seq;
            return true;
        }
        if (seq <= highest - size) {
            return false;
        }
        int i = index(seq);
        if (seen[i]) {
            return false;
        }
        seen[i] = true;
        return true;
    }

    /** Highest sequence recorded so far (0 if none). Useful as the client's {@code lastSeq}. */
    public synchronized long highest() {
        return highest;
    }

    private int index(long seq) {
        return (int) (seq % size);
    }
}
