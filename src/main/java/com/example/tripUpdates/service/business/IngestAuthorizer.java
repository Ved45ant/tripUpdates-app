package com.example.tripUpdates.service.business;

import com.example.tripUpdates.security.AuthPrincipal;
import com.example.tripUpdates.service.business.AuthDirectory.TripInfo;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Authorizes an ingested event: a cab device may only publish for itself and only for a trip that
 * is assigned to that cab within its own tenant. The returned {@link TripInfo} is the source of
 * truth for the event's office; the client-supplied {@code officeId} is never trusted, so a cab can
 * not push events into another office's topic.
 */
@Service
public class IngestAuthorizer {

    private static final Pattern UUID_PATTERN =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private final CachedDirectory directory;

    public IngestAuthorizer(CachedDirectory directory) {
        this.directory = directory;
    }

    /** @return the trip if allowed, empty for every kind of denial (unknown, malformed, foreign, mismatched) */
    public Optional<TripInfo> authorize(AuthPrincipal principal, String eventCabId, String eventTripId) {
        if (principal == null || !principal.isCabDevice() || principal.cabId() == null || principal.tenantId() == null) {
            return Optional.empty();
        }
        if (eventCabId == null || !eventCabId.equalsIgnoreCase(principal.cabId())) {
            return Optional.empty();
        }
        UUID tripId = parse(eventTripId);
        UUID tenantId = parse(principal.tenantId());
        UUID cabId = parse(principal.cabId());
        if (tripId == null || tenantId == null || cabId == null) {
            return Optional.empty();
        }
        try {
            return directory.trip(tripId)
                    .filter(t -> t.tenantId().equals(tenantId) && t.cabId().equals(cabId));
        } catch (RuntimeException e) {
            return Optional.empty(); // fail closed
        }
    }

    private static UUID parse(String value) {
        return value != null && UUID_PATTERN.matcher(value).matches() ? UUID.fromString(value) : null;
    }
}
