package com.example.tripUpdates.service.core;

import com.example.tripUpdates.service.core.ReplayDecision.Action;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class ReplayDecisionTest {

    @ParameterizedTest(name = "last={0} current={1} floor={2} -> {3}")
    @CsvSource({
            // topic has no events yet
            "0,   0,  0,  NOTHING",
            "5,   0,  0,  NOTHING",
            // brand-new subscriber on an active topic
            "0,   50, 0,  SNAPSHOT",
            // caught up
            "50,  50, 0,  NOTHING",
            // missed events, nothing critical evicted since
            "47,  50, 0,  REPLAY",
            "47,  50, 30, REPLAY",
            "30,  50, 30, REPLAY",
            // a critical event after lastSeq was evicted
            "29,  50, 30, SNAPSHOT",
            "1,   500, 400, SNAPSHOT",
            // client ahead of server (topic state reset)
            "60,  50, 0,  SNAPSHOT",
            // invalid inputs fall back to the safe option
            "-1,  50, 0,  SNAPSHOT"
    })
    void decide(long lastSeq, long currentSeq, long critFloor, Action expected) {
        assertThat(ReplayDecision.decide(lastSeq, currentSeq, critFloor)).isEqualTo(expected);
    }
}
