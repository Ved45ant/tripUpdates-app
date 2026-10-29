package com.example.tripUpdates.service.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReceiveTrackerTest {

    @Test
    void firstSeqIsBaselineNotAGap() {
        ReceiveTracker t = new ReceiveTracker(16);
        t.record(500);
        assertThat(t.hasGap()).isFalse();
        assertThat(t.contiguous()).isEqualTo(500);
    }

    @Test
    void inOrderStreamNeverHasGap() {
        ReceiveTracker t = new ReceiveTracker(16);
        for (long s = 1; s <= 100; s++) {
            t.record(s);
            assertThat(t.hasGap()).isFalse();
        }
        assertThat(t.contiguous()).isEqualTo(100);
    }

    @Test
    void missingSeqIsAGapUntilItArrivesLate() {
        ReceiveTracker t = new ReceiveTracker(16);
        t.record(1);
        t.record(2);
        t.record(4);                       // 3 missing (e.g. an SOS still in flight on the other connection)
        assertThat(t.hasGap()).isTrue();
        assertThat(t.contiguous()).isEqualTo(2);
        t.record(3);
        assertThat(t.hasGap()).isFalse();
        assertThat(t.contiguous()).isEqualTo(4);
    }

    @Test
    void markRepairedClosesTheGap() {
        ReceiveTracker t = new ReceiveTracker(16);
        t.record(1);
        t.record(9);
        assertThat(t.hasGap()).isTrue();
        t.markRepaired();
        assertThat(t.hasGap()).isFalse();
        t.record(10);
        assertThat(t.hasGap()).isFalse();
    }

    @Test
    void duplicatesAndOldMessagesAreHarmless() {
        ReceiveTracker t = new ReceiveTracker(8);
        for (long s = 1; s <= 30; s++) {
            t.record(s);
        }
        t.record(30);
        t.record(5);                       // older than the window
        assertThat(t.hasGap()).isFalse();
        assertThat(t.highest()).isEqualTo(30);
    }

    @Test
    void gapOlderThanWindowStopsBeingRepairableButNewTrafficIsTracked() {
        ReceiveTracker t = new ReceiveTracker(8);
        t.record(1);
        t.record(3);                       // 2 missing
        for (long s = 4; s <= 40; s++) {
            t.record(s);
        }
        assertThat(t.hasGap()).as("old gap fell out of the window").isFalse();
        t.record(42);                      // 41 missing now
        assertThat(t.hasGap()).isTrue();
        assertThat(t.contiguous()).isEqualTo(40);
    }

    @Test
    void advanceToMovesBaselineForwardOnly() {
        ReceiveTracker t = new ReceiveTracker(16);
        t.advanceTo(50);
        assertThat(t.highest()).isEqualTo(50);
        t.record(51);
        assertThat(t.hasGap()).isFalse();
        t.advanceTo(10);                   // never regresses
        assertThat(t.highest()).isEqualTo(51);
        t.record(60);
        assertThat(t.hasGap()).isTrue();
        t.advanceTo(60);                   // not beyond what was seen: must not hide the hole below
        assertThat(t.hasGap()).isTrue();
        t.advanceTo(61);                   // moving the baseline past it declares the range handled
        assertThat(t.hasGap()).isFalse();
    }
}
