package com.example.tripUpdates.controller;

import com.example.tripUpdates.exception.InvalidTripTransitionException;
import com.example.tripUpdates.exception.PublishUnavailableException;
import com.example.tripUpdates.model.dto.BaseTripEvent;
import com.example.tripUpdates.model.dto.LocationUpdateEvent;
import com.example.tripUpdates.model.dto.SosAlertEvent;
import com.example.tripUpdates.model.dto.TripStatusEvent;
import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.security.AuthPrincipal;
import com.example.tripUpdates.service.business.AuthDirectory.TripInfo;
import com.example.tripUpdates.service.business.EventPublishingService;
import com.example.tripUpdates.service.business.IngestAuthorizer;
import com.example.tripUpdates.service.business.IngestValidator;
import com.example.tripUpdates.telemetry.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Event ingestion for cab devices. Every endpoint runs the same pipeline:
 * <pre>
 *   authenticated cab device (401/403)
 *   -> structural validation incl. mandatory eventId (400)
 *   -> ownership: own cab, own trip, own tenant; office taken from the trip (403)
 *   -> publish: duplicate -> 202, stale location -> 202 (dropped), bad status transition -> 409,
 *               event store unavailable -> 503 + Retry-After (client retries with the same eventId)
 * </pre>
 * 202 for SOS means Redis acknowledged the event. Bodies are empty: nothing about the request is echoed.
 */
@RestController
@RequestMapping("/api/v1/events")
public class CabIngestController {

    private static final Logger logger = LoggerFactory.getLogger(CabIngestController.class);

    private final EventPublishingService eventPublishingService;
    private final IngestAuthorizer ingestAuthorizer;
    private final MetricsService metrics;

    public CabIngestController(EventPublishingService eventPublishingService, IngestAuthorizer ingestAuthorizer,
                               MetricsService metrics) {
        this.eventPublishingService = eventPublishingService;
        this.ingestAuthorizer = ingestAuthorizer;
        this.metrics = metrics;
    }

    @PostMapping("/location")
    public ResponseEntity<Void> ingestLocation(@RequestBody LocationUpdateEvent event,
                                               @AuthenticationPrincipal AuthPrincipal principal) {
        return handle(EventType.LOCATION, event, principal, () -> eventPublishingService.publishLocation(event));
    }

    @PostMapping("/status")
    public ResponseEntity<Void> ingestStatus(@RequestBody TripStatusEvent event,
                                             @AuthenticationPrincipal AuthPrincipal principal) {
        return handle(EventType.STATUS, event, principal, () -> eventPublishingService.publishStatus(event));
    }

    @PostMapping("/sos")
    public ResponseEntity<Void> ingestSos(@RequestBody SosAlertEvent event,
                                          @AuthenticationPrincipal AuthPrincipal principal) {
        return handle(EventType.SOS, event, principal, () -> eventPublishingService.publishSos(event));
    }

    private ResponseEntity<Void> handle(EventType type, BaseTripEvent event, AuthPrincipal principal, Runnable publish) {
        String problem = IngestValidator.validate(event, type, System.currentTimeMillis());
        if (problem != null) {
            metrics.count("ingest.events", "type", type.name(), "outcome", "invalid");
            logger.debug("Rejected {} event: {}", type, problem);
            return ResponseEntity.badRequest().build();
        }
        TripInfo trip = ingestAuthorizer.authorize(principal, event.getCabId(), event.getTripId()).orElse(null);
        if (trip == null) {
            metrics.count("ingest.events", "type", type.name(), "outcome", "forbidden");
            metrics.recordAuthRejection("ingest");
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        event.setOfficeId(trip.officeId().toString());
        try {
            publish.run();
            return ResponseEntity.accepted().build();
        } catch (InvalidTripTransitionException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        } catch (PublishUnavailableException e) {
            logger.error("Event store unavailable while publishing {}", type);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header("Retry-After", "1").build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }
}
