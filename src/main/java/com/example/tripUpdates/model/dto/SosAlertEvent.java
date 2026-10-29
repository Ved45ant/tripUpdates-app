package com.example.tripUpdates.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.example.tripUpdates.model.enums.EventType;

/**
 * Represents an SOS (emergency) alert event from a cab.
 * SOS events bypass normal queuing and have guaranteed < 2 second delivery.
 * SOS events are archived in the database and routed to Security Operators immediately.
 */
public class SosAlertEvent extends BaseTripEvent {

    @JsonProperty("payload")
    private SosAlertPayload payload;

    public SosAlertEvent() {
        super();
    }

    public SosAlertEvent(String cabId, String tripId, String officeId, long timestamp, SosAlertPayload payload) {
        super(cabId, tripId, officeId, EventType.SOS, timestamp);
        this.payload = payload;
    }

    public SosAlertPayload getPayload() {
        return payload;
    }

    public void setPayload(SosAlertPayload payload) {
        this.payload = payload;
    }

    public static class SosAlertPayload {
        @JsonProperty("level")
        private String level; // e.g., "CRITICAL", "WARNING"

        @JsonProperty("message")
        private String message;

        @JsonProperty("lat")
        private double latitude;

        @JsonProperty("lng")
        private double longitude;

        public SosAlertPayload() {}

        public SosAlertPayload(String level, String message, double latitude, double longitude) {
            this.level = level;
            this.message = message;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public double getLatitude() {
            return latitude;
        }

        public void setLatitude(double latitude) {
            this.latitude = latitude;
        }

        public double getLongitude() {
            return longitude;
        }

        public void setLongitude(double longitude) {
            this.longitude = longitude;
        }
    }
}
