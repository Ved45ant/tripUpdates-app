package com.example.tripUpdates.repository;

import com.example.tripUpdates.model.entity.TripPassenger;
import com.example.tripUpdates.model.entity.TripPassengerId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JPA Repository for TripPassenger entity (composite key: trip_id, user_id).
 * Provides fast O(1) membership lookups for authorization checks.
 * Used to verify if a user is a passenger on a specific trip.
 */
@Repository
public interface TripPassengerRepository extends JpaRepository<TripPassenger, TripPassengerId> {

    Optional<TripPassenger> findByTripIdAndUserId(UUID tripId, UUID userId);

    boolean existsByTripIdAndUserId(UUID tripId, UUID userId);

    List<TripPassenger> findByTripId(UUID tripId);

    List<TripPassenger> findByUserId(UUID userId);
}
