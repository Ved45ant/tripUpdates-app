package com.example.tripUpdates.repository;

import com.example.tripUpdates.model.entity.Trip;
import com.example.tripUpdates.model.enums.TripStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JPA Repository for Trip entity.
 * Provides queries for trip lookups with tenant and status filtering.
 * Used during authorization checks to verify user access and trip state.
 */
@Repository
public interface TripRepository extends JpaRepository<Trip, UUID> {

    Optional<Trip> findById(UUID id);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select t from Trip t where t.id = :id")
    Optional<Trip> findByIdForUpdate(@Param("id") UUID id);

    Optional<Trip> findByIdAndTenantId(UUID id, UUID tenantId);

    List<Trip> findByTenantId(UUID tenantId);

    List<Trip> findByOfficeId(UUID officeId);

    List<Trip> findByOfficeIdAndStatus(UUID officeId, TripStatus status);

    List<Trip> findByCabId(UUID cabId);

    List<Trip> findByTenantIdAndCabId(UUID tenantId, UUID cabId);
}
