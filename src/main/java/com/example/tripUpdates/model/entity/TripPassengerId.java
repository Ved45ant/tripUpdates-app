package com.example.tripUpdates.model.entity;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Composite primary key for TripPassenger entity.
 * Ensures O(1) membership lookups for (trip_id, user_id) pairs.
 */
public class TripPassengerId implements Serializable {

    private static final long serialVersionUID = 1L;

    private UUID tripId;
    private UUID userId;

    public TripPassengerId() {}

    public TripPassengerId(String tripId, String userId) {
        this.tripId = UUID.fromString(tripId);
        this.userId = UUID.fromString(userId);
    }

    public TripPassengerId(UUID tripId, UUID userId) {
        this.tripId = tripId;
        this.userId = userId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TripPassengerId that = (TripPassengerId) o;
        return Objects.equals(tripId, that.tripId) &&
               Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tripId, userId);
    }

    public UUID getTripId() {
        return tripId;
    }

    public void setTripId(UUID tripId) {
        this.tripId = tripId;
    }

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }
}
