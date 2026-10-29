package com.example.tripUpdates.service.core;

/**
 * Pure decision logic for resuming a subscription.
 *
 * Inputs (all per topic):
 * <ul>
 *   <li>{@code lastSeq}    last sequence the client confirms having (0 = brand-new subscription)</li>
 *   <li>{@code currentSeq} latest sequence assigned for the topic (0 = no events yet)</li>
 *   <li>{@code critFloor}  highest STATUS/SOS sequence already evicted from the replay buffer
 *                          (0 = nothing evicted)</li>
 * </ul>
 *
 * Only STATUS/SOS events live in the replay buffer; locations are served from the latest-location
 * snapshot, so a gap is only "unrecoverable by replay" when a critical event was evicted after
 * the client's {@code lastSeq}.
 */
public final class ReplayDecision {

    public enum Action {
        /** Client is up to date: just go live. */
        NOTHING,
        /** Replay buffered STATUS/SOS with seq > lastSeq (complete), then latest location, then live. */
        REPLAY,
        /** Send a fresh snapshot, then live. */
        SNAPSHOT
    }

    private ReplayDecision() {
    }

    public static Action decide(long lastSeq, long currentSeq, long critFloor) {
        if (lastSeq < 0 || currentSeq < 0 || critFloor < 0) {
            return Action.SNAPSHOT;
        }
        if (currentSeq == 0) {
            return Action.NOTHING; // topic has no events yet
        }
        if (lastSeq == 0) {
            return Action.SNAPSHOT; // new subscriber: start from the latest state
        }
        if (lastSeq > currentSeq) {
            return Action.SNAPSHOT; // client is "ahead": topic state was reset, cannot trust seqs
        }
        if (lastSeq == currentSeq) {
            return Action.NOTHING;
        }
        // lastSeq < currentSeq: replay is complete only if no critical event > lastSeq was evicted
        return lastSeq >= critFloor ? Action.REPLAY : Action.SNAPSHOT;
    }
}
