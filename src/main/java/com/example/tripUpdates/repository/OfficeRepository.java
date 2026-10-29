package com.example.tripUpdates.repository;

import com.example.tripUpdates.model.entity.Office;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JPA Repository for Office entity.
 * Provides queries for office lookups within a tenant context.
 */
@Repository
public interface OfficeRepository extends JpaRepository<Office, UUID> {

    Optional<Office> findById(UUID id);

    List<Office> findByTenantId(UUID tenantId);

    Optional<Office> findByIdAndTenantId(UUID id, UUID tenantId);
}
