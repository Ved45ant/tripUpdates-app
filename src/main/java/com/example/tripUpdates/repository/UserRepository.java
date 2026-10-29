package com.example.tripUpdates.repository;

import com.example.tripUpdates.model.entity.User;
import com.example.tripUpdates.model.enums.UserRole;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JPA Repository for User entity.
 * Provides queries for user lookups and role-based filtering.
 */
@Repository
public interface UserRepository extends JpaRepository<User, UUID> {

    Optional<User> findById(UUID id);

    Optional<User> findByIdAndTenantId(UUID id, UUID tenantId);

    List<User> findByTenantId(UUID tenantId);

    List<User> findByTenantIdAndRole(UUID tenantId, UserRole role);

    List<User> findByOfficeId(UUID officeId);

    List<User> findByOfficeIdAndRole(UUID officeId, UserRole role);
}
