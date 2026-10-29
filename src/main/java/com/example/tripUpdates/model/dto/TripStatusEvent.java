package com.example.tripUpdates.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.model.enums.TripStatus;

/**
 * Represents a trip status change event (e.g., SCHEDULED -> ACTIVE -> COMPLETED).
 * Status events are archived in the database for audit trails.
 */
public class TripStatusEvent extends BaseTripEvent {

    @JsonProperty("payload")
    private TripStatusPayload payload;

    public TripStatusEvent() {
        super();
    }

    public TripStatusEvent(String cabId, String tripId, String officeId, long timestamp, TripStatusPayload payload) {
        super(cabId, tripId, officeId, EventType.STATUS, timestamp);
        this.payload = payload;
    }

    public TripStatusPayload getPayload() {
        return payload;
    }

    public void setPayload(TripStatusPayload payload) {
        this.payload = payload;
    }

    public static class TripStatusPayload {
        @JsonProperty("status")
        private TripStatus status;

        @JsonProperty("reason")
        private String reason;

        public TripStatusPayload() {}

        public TripStatusPayload(TripStatus status, String reason) {
            this.status = status;
            this.reason = reason;
        }

        public TripStatus getStatus() {
            return status;
        }

        public void setStatus(TripStatus status) {
            this.status = status;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }
    }
}
