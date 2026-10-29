package com.example.tripUpdates.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.example.tripUpdates.model.enums.EventType;

/**
 * Represents a location update event from a cab.
 * Location updates are high-frequency and are conflated at the connection level.
 * They are NOT archived in the database to save storage.
 */
public class LocationUpdateEvent extends BaseTripEvent {

    @JsonProperty("payload")
    private LocationPayload payload;

    public LocationUpdateEvent() {
        super();
    }

    public LocationUpdateEvent(String cabId, String tripId, String officeId, long timestamp, LocationPayload payload) {
        super(cabId, tripId, officeId, EventType.LOCATION, timestamp);
        this.payload = payload;
    }

    public LocationPayload getPayload() {
        return payload;
    }

    public void setPayload(LocationPayload payload) {
        this.payload = payload;
    }

    public static class LocationPayload {
        @JsonProperty("lat")
        private double latitude;

        @JsonProperty("lng")
        private double longitude;

        @JsonProperty("speed")
        private int speed;

        public LocationPayload() {}

        public LocationPayload(double latitude, double longitude, int speed) {
            this.latitude = latitude;
            this.longitude = longitude;
            this.speed = speed;
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

        public int getSpeed() {
            return speed;
        }

        public void setSpeed(int speed) {
            this.speed = speed;
        }
    }
}
