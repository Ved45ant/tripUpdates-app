package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.dto.BaseTripEvent;
import com.example.tripUpdates.model.dto.LocationUpdateEvent;
import com.example.tripUpdates.model.dto.SosAlertEvent;
import com.example.tripUpdates.model.dto.TripStatusEvent;
import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.model.enums.TripStatus;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class IngestValidatorTest {

    static final long NOW = 1_715_421_000_000L;

    static LocationUpdateEvent location() {
        LocationUpdateEvent e = new LocationUpdateEvent("cab-1", "trip-1", "office-1", NOW,
                new LocationUpdateEvent.LocationPayload(12.97, 77.59, 40));
        e.setEventId("evt-1");
        return e;
    }

    static TripStatusEvent status() {
        TripStatusEvent e = new TripStatusEvent("cab-1", "trip-1", "office-1", NOW,
                new TripStatusEvent.TripStatusPayload(TripStatus.ACTIVE, "started"));
        e.setEventId("evt-2");
        return e;
    }

    static SosAlertEvent sos() {
        SosAlertEvent e = new SosAlertEvent("cab-1", "trip-1", "office-1", NOW,
                new SosAlertEvent.SosAlertPayload("CRITICAL", "help", 12.97, 77.59));
        e.setEventId("evt-3");
        return e;
    }

    private String check(LocationUpdateEvent e, Consumer<LocationUpdateEvent> mutate) {
        mutate.accept(e);
        return IngestValidator.validate(e, EventType.LOCATION, NOW);
    }

    @Test
    void validEventsPass() {
        assertThat(IngestValidator.validate(location(), EventType.LOCATION, NOW)).isNull();
        assertThat(IngestValidator.validate(status(), EventType.STATUS, NOW)).isNull();
        assertThat(IngestValidator.validate(sos(), EventType.SOS, NOW)).isNull();
    }

    @Test
    void eventIdIsMandatoryAndConstrained() {
        assertThat(check(location(), e -> e.setEventId(null))).isEqualTo("event-id");
        assertThat(check(location(), e -> e.setEventId(""))).isEqualTo("event-id");
        assertThat(check(location(), e -> e.setEventId("has space"))).isEqualTo("event-id");
        assertThat(check(location(), e -> e.setEventId("x".repeat(65)))).isEqualTo("event-id");
        assertThat(check(location(), e -> e.setEventId("a-b_c.d:e"))).isNull();
    }

    @Test
    void timestampMustBePositiveAndNotInTheFuture() {
        assertThat(check(location(), e -> e.setTimestamp(0))).isEqualTo("timestamp");
        assertThat(check(location(), e -> e.setTimestamp(-5))).isEqualTo("timestamp");
        // a far-future timestamp would make every later location of the cab look stale
        assertThat(check(location(), e -> e.setTimestamp(NOW + 3_600_000))).isEqualTo("timestamp");
        assertThat(check(location(), e -> e.setTimestamp(NOW + IngestValidator.MAX_CLOCK_SKEW_MILLIS))).isNull();
    }

    @Test
    void locationRanges() {
        assertThat(check(location(), e -> e.getPayload().setLatitude(90.1))).isEqualTo("location-range");
        assertThat(check(location(), e -> e.getPayload().setLongitude(-180.1))).isEqualTo("location-range");
        assertThat(check(location(), e -> e.getPayload().setLatitude(Double.NaN))).isEqualTo("location-range");
        assertThat(check(location(), e -> e.getPayload().setSpeed(-1))).isEqualTo("location-range");
        assertThat(check(location(), e -> e.getPayload().setSpeed(401))).isEqualTo("location-range");
        assertThat(check(location(), e -> e.setPayload(null))).isEqualTo("payload");
    }

    @Test
    void idsAndEventTypeMustMatchTheEndpoint() {
        assertThat(check(location(), e -> e.setCabId(null))).isEqualTo("ids");
        assertThat(check(location(), e -> e.setTripId(null))).isEqualTo("ids");
        assertThat(IngestValidator.validate(status(), EventType.LOCATION, NOW)).isEqualTo("event-type");
        assertThat(IngestValidator.validate(null, EventType.LOCATION, NOW)).isEqualTo("empty");
    }

    @Test
    void statusAndSosPayloads() {
        TripStatusEvent noStatus = status();
        noStatus.getPayload().setStatus(null);
        assertThat(IngestValidator.validate(noStatus, EventType.STATUS, NOW)).isEqualTo("payload");

        TripStatusEvent longReason = status();
        longReason.getPayload().setReason("r".repeat(201));
        assertThat(IngestValidator.validate(longReason, EventType.STATUS, NOW)).isEqualTo("reason-length");

        SosAlertEvent noLevel = sos();
        noLevel.getPayload().setLevel(" ");
        assertThat(IngestValidator.validate(noLevel, EventType.SOS, NOW)).isEqualTo("payload");

        SosAlertEvent longMessage = sos();
        longMessage.getPayload().setMessage("m".repeat(501));
        assertThat(IngestValidator.validate(longMessage, EventType.SOS, NOW)).isEqualTo("message-length");

        SosAlertEvent badCoords = sos();
        badCoords.getPayload().setLatitude(200);
        assertThat(IngestValidator.validate(badCoords, EventType.SOS, NOW)).isEqualTo("location-range");
    }

    @Test
    void baseTypeIsUsedOnlyThroughKnownSubclasses() {
        BaseTripEvent unknown = new BaseTripEvent("cab-1", "trip-1", "office-1", EventType.LOCATION, NOW) {
        };
        unknown.setEventId("evt-9");
        assertThat(IngestValidator.validate(unknown, EventType.LOCATION, NOW)).isEqualTo("type");
    }
}
