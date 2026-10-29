package com.example.tripUpdates.service.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SeenWindowTest {

    @Test
    void firstSeenIsNewDuplicateIsNot() {
        SeenWindow w = new SeenWindow(16);
        assertThat(w.markIfNew(1)).isTrue();
        assertThat(w.markIfNew(1)).isFalse();
        assertThat(w.markIfNew(2)).isTrue();
        assertThat(w.highest()).isEqualTo(2);
    }

    @Test
    void lowerSeqArrivingAfterSosOvertookItIsStillAccepted() {
        SeenWindow w = new SeenWindow(16);
        assertThat(w.markIfNew(5)).as("SOS seq 5 overtakes").isTrue();
        assertThat(w.markIfNew(3)).as("late STATUS seq 3").isTrue();
        assertThat(w.markIfNew(4)).as("late LOCATION seq 4").isTrue();
        assertThat(w.markIfNew(3)).as("replayed duplicate").isFalse();
    }

    @Test
    void ignoresNonPositiveSeq() {
        SeenWindow w = new SeenWindow(8);
        assertThat(w.markIfNew(0)).isFalse();
        assertThat(w.markIfNew(-4)).isFalse();
    }

    @Test
    void tooOldSeqIsTreatedAsDuplicate() {
        SeenWindow w = new SeenWindow(8);
        assertThat(w.markIfNew(100)).isTrue();
        assertThat(w.markIfNew(92)).as("exactly size behind").isFalse();
        assertThat(w.markIfNew(93)).as("inside window, unseen").isTrue();
    }

    @Test
    void windowSlidesAndSlotsAreReused() {
        SeenWindow w = new SeenWindow(4);
        for (long s = 1; s <= 50; s++) {
            assertThat(w.markIfNew(s)).as("seq %d", s).isTrue();
            assertThat(w.markIfNew(s)).as("dup %d", s).isFalse();
        }
    }

    @Test
    void bigJumpClearsWindow() {
        SeenWindow w = new SeenWindow(8);
        w.markIfNew(1);
        w.markIfNew(2);
        assertThat(w.markIfNew(1_000)).isTrue();
        assertThat(w.markIfNew(999)).isTrue();
        assertThat(w.markIfNew(2)).as("far behind").isFalse();
    }

    @Test
    void skippedSeqsInsideJumpRemainUnseen() {
        SeenWindow w = new SeenWindow(8);
        w.markIfNew(1);
        w.markIfNew(6);           // skips 2..5
        assertThat(w.markIfNew(4)).isTrue();
        assertThat(w.markIfNew(4)).isFalse();
    }
}
