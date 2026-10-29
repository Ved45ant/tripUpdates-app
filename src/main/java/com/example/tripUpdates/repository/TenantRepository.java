package com.example.tripUpdates.repository;

import com.example.tripUpdates.model.entity.Tenant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * JPA Repository for Tenant entity.
 * Provides basic CRUD operations and query methods for tenant isolation.
 */
@Repository
public interface TenantRepository extends JpaRepository<Tenant, UUID> {

    Optional<Tenant> findById(UUID id);
}
