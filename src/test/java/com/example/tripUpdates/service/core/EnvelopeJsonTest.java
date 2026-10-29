package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.enums.EventType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvelopeJsonTest {

    @Test
    void prefixCompletedWithSeqParsesBackToTheSameEnvelope() throws Exception {
        String prefix = EnvelopeJson.prefix("trip:t1", EventType.STATUS, "cab-9", 1715421000000L,
                "{\"status\":\"ACTIVE\",\"note\":\"he said \\\"hi\\\"\"}");
        EventEnvelope e = EnvelopeJson.parse(prefix + ",\"seq\":42}");

        assertThat(e.topic()).isEqualTo("trip:t1");
        assertThat(e.seq()).isEqualTo(42);
        assertThat(e.type()).isEqualTo(EventType.STATUS);
        assertThat(e.cabId()).isEqualTo("cab-9");
        assertThat(e.ingestTs()).isEqualTo(1715421000000L);
        assertThat(e.payload()).isEqualTo("{\"status\":\"ACTIVE\",\"note\":\"he said \\\"hi\\\"\"}");
    }

    @Test
    void nullCabAndRoundTripThroughToJson() throws Exception {
        EventEnvelope original = new EventEnvelope("office:o1", 7, EventType.SOS, null, 5L, "{\"level\":\"HIGH\"}");
        EventEnvelope back = EnvelopeJson.parse(EnvelopeJson.toJson(original));
        assertThat(back).isEqualTo(original);
    }

    @Test
    void rejectsInvalidPayloadJson() {
        assertThatThrownBy(() -> EnvelopeJson.prefix("trip:t1", EventType.LOCATION, "c", 1L, "{not json"))
                .isInstanceOf(com.fasterxml.jackson.core.JsonProcessingException.class);
    }
}
