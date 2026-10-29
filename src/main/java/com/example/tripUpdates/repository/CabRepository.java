package com.example.tripUpdates.repository;

import com.example.tripUpdates.model.entity.Cab;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JPA Repository for Cab entity.
 * Provides queries for cab lookups within a tenant context.
 */
@Repository
public interface CabRepository extends JpaRepository<Cab, UUID> {

    Optional<Cab> findById(UUID id);

    Optional<Cab> findByIdAndTenantId(UUID id, UUID tenantId);

    List<Cab> findByTenantId(UUID tenantId);
}
