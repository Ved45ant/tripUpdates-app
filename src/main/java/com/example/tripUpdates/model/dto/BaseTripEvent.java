package com.example.tripUpdates.model.dto;

import com.example.tripUpdates.model.enums.EventType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base abstract class for all trip events in the system.
 * Uses JSON polymorphism to support Location, Status, and SOS variants.
 * Generated sequence numbers by Redis guarantee strict ordering.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "eventType",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = LocationUpdateEvent.class, name = "LOCATION"),
        @JsonSubTypes.Type(value = TripStatusEvent.class, name = "STATUS"),
        @JsonSubTypes.Type(value = SosAlertEvent.class, name = "SOS")
})
public abstract class BaseTripEvent {

    /** Client-supplied idempotency key; a retry carrying the same id never gets a second sequence. */
    @JsonProperty("eventId")
    private String eventId;

    @JsonProperty("cabId")
    private String cabId;

    @JsonProperty("tripId")
    private String tripId;

    @JsonProperty("officeId")
    private String officeId;

    @JsonProperty("eventType")
    private EventType eventType;

    @JsonProperty("timestamp")
    private long timestamp;

    // Set by backend after sequence generation
    private long sequenceNumber;

    public BaseTripEvent() {}

    public BaseTripEvent(String cabId, String tripId, String officeId, EventType eventType, long timestamp) {
        this.cabId = cabId;
        this.tripId = tripId;
        this.officeId = officeId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCabId() {
        return cabId;
    }

    public void setCabId(String cabId) {
        this.cabId = cabId;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getOfficeId() {
        return officeId;
    }

    public void setOfficeId(String officeId) {
        this.officeId = officeId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
}
