package com.example.tripUpdates.repository;

import com.example.tripUpdates.model.entity.EventArchive;
import com.example.tripUpdates.model.enums.EventType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JPA Repository for EventArchive entity.
 * Provides queries for archived critical events (STATUS_CHANGE, SOS).
 * Location updates are not archived to conserve storage.
 * Used for audit trails and replay-buffer recovery.
 */
@Repository
public interface EventArchiveRepository extends JpaRepository<EventArchive, UUID> {

    Optional<EventArchive> findById(UUID id);

    List<EventArchive> findByTripId(UUID tripId);

    List<EventArchive> findByTripIdAndEventType(UUID tripId, EventType eventType);

    List<EventArchive> findByEventType(EventType eventType);

    List<EventArchive> findByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);

    List<EventArchive> findByTripIdAndCreatedAtBetween(UUID tripId, LocalDateTime startTime, LocalDateTime endTime);
}
