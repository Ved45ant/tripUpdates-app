package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.dto.BaseTripEvent;
import com.example.tripUpdates.model.dto.LocationUpdateEvent;
import com.example.tripUpdates.model.dto.SosAlertEvent;
import com.example.tripUpdates.model.dto.TripStatusEvent;
import com.example.tripUpdates.model.enums.EventType;

import java.util.regex.Pattern;

/**
 * Structural validation of ingested events (the "400" rules). Ownership is checked separately
 * ({@link IngestAuthorizer}). Bounds protect the pipeline: a future timestamp would poison the stale
 * location guard for a cab, and unbounded free text would bloat Redis and the archive.
 */
public final class IngestValidator {

    public static final long MAX_CLOCK_SKEW_MILLIS = 60_000;
    private static final Pattern EVENT_ID = Pattern.compile("^[A-Za-z0-9._:-]{1,64}$");
    private static final int MAX_REASON = 200;
    private static final int MAX_MESSAGE = 500;

    private IngestValidator() {
    }

    /** @return null if valid, otherwise a short machine-readable reason (never echoed to the client) */
    public static String validate(BaseTripEvent event, EventType expected, long nowMillis) {
        if (event == null) {
            return "empty";
        }
        if (event.getEventType() != expected) {
            return "event-type";
        }
        if (event.getEventId() == null || !EVENT_ID.matcher(event.getEventId()).matches()) {
            return "event-id";
        }
        if (event.getCabId() == null || event.getTripId() == null) {
            return "ids";
        }
        if (event.getTimestamp() <= 0 || event.getTimestamp() > nowMillis + MAX_CLOCK_SKEW_MILLIS) {
            return "timestamp";
        }
        if (event instanceof LocationUpdateEvent) {
            return location((LocationUpdateEvent) event);
        }
        if (event instanceof TripStatusEvent) {
            return status((TripStatusEvent) event);
        }
        if (event instanceof SosAlertEvent) {
            return sos((SosAlertEvent) event);
        }
        return "type";
    }

    private static String location(LocationUpdateEvent e) {
        LocationUpdateEvent.LocationPayload p = e.getPayload();
        if (p == null) {
            return "payload";
        }
        if (!validCoordinates(p.getLatitude(), p.getLongitude()) || p.getSpeed() < 0 || p.getSpeed() > 400) {
            return "location-range";
        }
        return null;
    }

    private static String status(TripStatusEvent e) {
        TripStatusEvent.TripStatusPayload p = e.getPayload();
        if (p == null || p.getStatus() == null) {
            return "payload";
        }
        if (p.getReason() != null && p.getReason().length() > MAX_REASON) {
            return "reason-length";
        }
        return null;
    }

    private static String sos(SosAlertEvent e) {
        SosAlertEvent.SosAlertPayload p = e.getPayload();
        if (p == null || p.getLevel() == null || p.getLevel().isBlank() || p.getLevel().length() > 32) {
            return "payload";
        }
        if (p.getMessage() != null && p.getMessage().length() > MAX_MESSAGE) {
            return "message-length";
        }
        if (!validCoordinates(p.getLatitude(), p.getLongitude())) {
            return "location-range";
        }
        return null;
    }

    private static boolean validCoordinates(double lat, double lng) {
        return !Double.isNaN(lat) && !Double.isNaN(lng) && lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180;
    }
}
