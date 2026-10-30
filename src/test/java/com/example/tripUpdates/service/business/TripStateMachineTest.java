package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.enums.TripStatus;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class TripStateMachineTest {

    @ParameterizedTest(name = "{0} -> {1} allowed={2}")
    @CsvSource({
            "SCHEDULED, SCHEDULED, true",
            "SCHEDULED, ACTIVE,    true",
            "SCHEDULED, CANCELLED, true",
            "SCHEDULED, COMPLETED, false",
            "ACTIVE,    ACTIVE,    true",
            "ACTIVE,    COMPLETED, true",
            "ACTIVE,    CANCELLED, true",
            "ACTIVE,    SCHEDULED, false",
            "COMPLETED, COMPLETED, true",
            "COMPLETED, ACTIVE,    false",
            "COMPLETED, CANCELLED, false",
            "COMPLETED, SCHEDULED, false",
            "CANCELLED, CANCELLED, true",
            "CANCELLED, ACTIVE,    false",
            "CANCELLED, COMPLETED, false",
    })
    void transitions(TripStatus from, TripStatus to, boolean allowed) {
        assertThat(TripStateMachine.isAllowed(from, to)).isEqualTo(allowed);
    }

    @org.junit.jupiter.api.Test
    void nullsAreNeverAllowed() {
        assertThat(TripStateMachine.isAllowed(null, TripStatus.ACTIVE)).isFalse();
        assertThat(TripStateMachine.isAllowed(TripStatus.ACTIVE, null)).isFalse();
    }
}
