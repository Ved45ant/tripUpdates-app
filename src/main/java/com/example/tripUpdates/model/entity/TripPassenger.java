package com.example.tripUpdates.model.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.util.UUID;

/**
 * Junction table allowing multiple employees to share a single trip.
 * Provides O(1) membership lookups for authorization checks.
 * Primary key is composite: (trip_id, user_id)
 */
@Entity
@Table(name = "trip_passengers", indexes = {
        @Index(name = "idx_trip_passenger_trip_id", columnList = "trip_id"),
        @Index(name = "idx_trip_passenger_user_id", columnList = "user_id")
})
@IdClass(TripPassengerId.class)
public class TripPassenger {

    @Id
    @Column(name = "trip_id", columnDefinition = "UUID")
    @JdbcTypeCode(SqlTypes.UUID)
    private UUID tripId;

    @Id
    @Column(name = "user_id", columnDefinition = "UUID")
    @JdbcTypeCode(SqlTypes.UUID)
    private UUID userId;

    public TripPassenger() {}

    public TripPassenger(String tripId, String userId) {
        this.tripId = UUID.fromString(tripId);
        this.userId = UUID.fromString(userId);
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
