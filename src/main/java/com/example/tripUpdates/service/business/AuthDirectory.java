package com.example.tripUpdates.service.business;

import java.util.Optional;
import java.util.UUID;

/**
 * Read-only view of the ownership data that authorization decisions are based on.
 * Lookups deliberately do NOT filter by tenant: the caller compares tenants explicitly, so a
 * cross-tenant attempt is a decision ("deny") rather than an accident of a query.
 */
public interface AuthDirectory {

    record TripInfo(UUID id, UUID tenantId, UUID officeId, UUID cabId) {
    }

    record CabInfo(UUID id, UUID tenantId, UUID officeId) {
    }

    record OfficeInfo(UUID id, UUID tenantId) {
    }

    Optional<TripInfo> findTrip(UUID tripId);

    Optional<CabInfo> findCab(UUID cabId);

    Optional<OfficeInfo> findOffice(UUID officeId);

    boolean isPassenger(UUID tripId, UUID userId);

    boolean isUserActive(UUID userId);
}
