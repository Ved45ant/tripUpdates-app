package com.example.tripUpdates.service.business;

import com.example.tripUpdates.exception.InvalidTripTransitionException;
import com.example.tripUpdates.exception.PublishUnavailableException;
import com.example.tripUpdates.model.dto.BaseTripEvent;
import com.example.tripUpdates.model.dto.LocationUpdateEvent;
import com.example.tripUpdates.model.dto.SosAlertEvent;
import com.example.tripUpdates.model.dto.TripStatusEvent;
import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.service.business.TopicPublisher.PublishResult;
import com.example.tripUpdates.service.business.TripStatusStore.TransitionResult;
import com.example.tripUpdates.telemetry.MetricsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Ingest pipeline after validation and authorization: state check, publish, archive.
 *
 * <ul>
 *   <li><b>Topics.</b> Every event goes to {@code trip:{tripId}} and, when known, {@code cab:{cabId}} and
 *       {@code office:{officeId}}, each with its own sequence. The publishing node delivers nothing itself.</li>
 *   <li><b>Duplicates.</b> {@code eventId} makes each per-topic publish idempotent, so a client retry after a
 *       timeout or partial failure completes the remaining topics without duplicating the finished ones.</li>
 *   <li><b>Out-of-order.</b> A LOCATION older than the latest accepted one of the cab is dropped. A STATUS that
 *       would move the trip backwards (or out of a terminal state) is rejected as a conflict.</li>
 *   <li><b>Redis failures.</b> SOS is retried up to {@link RetryPolicy#attempts()} times within the time budget;
 *       success is reported only after Redis acknowledged it. Otherwise {@link PublishUnavailableException}
 *       (HTTP 503) makes the caller retry. Other event types fail fast.</li>
 *   <li><b>Archive.</b> STATUS/SOS are handed to the {@link ArchiveWriter}; it can never fail or delay a publish.</li>
 * </ul>
 */
@Service
public class EventPublishingService {

    /** SOS retry policy: attempts, backoff base (attempt n waits n * base), total budget. */
    public record RetryPolicy(int attempts, long baseBackoffMillis, long budgetMillis) {
        public static RetryPolicy defaults() {
            return new RetryPolicy(3, 50, 500);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(EventPublishingService.class);

    private final TopicPublisher publisher;
    private final TripStatusStore tripStatusStore;
    private final ArchiveWriter archiveWriter;
    private final ObjectMapper objectMapper;
    private final MetricsService metrics;
    private final RetryPolicy sosRetry;

    @Autowired
    public EventPublishingService(TopicPublisher publisher, TripStatusStore tripStatusStore, ArchiveWriter archiveWriter,
                                  ObjectMapper objectMapper, MetricsService metrics,
                                  @Value("${ingest.sos.attempts:3}") int sosAttempts,
                                  @Value("${ingest.sos.backoff-ms:50}") long sosBackoffMillis,
                                  @Value("${ingest.sos.budget-ms:500}") long sosBudgetMillis) {
        this(publisher, tripStatusStore, archiveWriter, objectMapper, metrics,
                new RetryPolicy(sosAttempts, sosBackoffMillis, sosBudgetMillis));
    }

    public EventPublishingService(TopicPublisher publisher, TripStatusStore tripStatusStore, ArchiveWriter archiveWriter,
                                  ObjectMapper objectMapper, MetricsService metrics, RetryPolicy sosRetry) {
        this.publisher = publisher;
        this.tripStatusStore = tripStatusStore;
        this.archiveWriter = archiveWriter;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.sosRetry = sosRetry;
    }

    public void publishLocation(LocationUpdateEvent event) {
        publish(event, true);
    }

    /** @throws InvalidTripTransitionException if the trip cannot move to the requested status */
    public void publishStatus(TripStatusEvent event) {
        TransitionResult result = tripStatusStore.transition(UUID.fromString(event.getTripId()), event.getPayload().getStatus());
        if (result == TransitionResult.NOT_FOUND) {
            throw new IllegalArgumentException("unknown trip");
        }
        if (result == TransitionResult.INVALID) {
            metrics.count("ingest.events", "type", "STATUS", "outcome", "conflict");
            throw new InvalidTripTransitionException("status transition not allowed");
        }
        long tripSeq = publish(event, false);
        archive(event, tripSeq);
    }

    public void publishSos(SosAlertEvent event) {
        long tripSeq = publish(event, false);
        logger.warn("SOS accepted: tripId={}, seq={}", event.getTripId(), tripSeq);
        archive(event, tripSeq);
    }

    /** @return the sequence on the trip topic, or 0 if the event was dropped as stale */
    private long publish(BaseTripEvent event, boolean guardStale) {
        long ingestTs = System.currentTimeMillis();
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("event cannot be serialised", e);
        }
        String type = event.getEventType().name();
        int attempts = event.getEventType() == EventType.SOS ? sosRetry.attempts() : 1;
        long deadline = System.nanoTime() + sosRetry.budgetMillis() * 1_000_000L;

        PublishResult trip = publishTo("trip:" + event.getTripId(), event, ingestTs, payload, guardStale, attempts, deadline);
        if (trip.stale()) {
            metrics.count("ingest.events", "type", type, "outcome", "stale");
            return 0;
        }
        if (event.getCabId() != null) {
            publishTo("cab:" + event.getCabId(), event, ingestTs, payload, false, attempts, deadline);
        }
        if (event.getOfficeId() != null) {
            publishTo("office:" + event.getOfficeId(), event, ingestTs, payload, false, attempts, deadline);
        }
        metrics.count("ingest.events", "type", type, "outcome", trip.duplicate() ? "duplicate" : "accepted");
        return trip.seq();
    }

    private PublishResult publishTo(String topic, BaseTripEvent event, long ingestTs, String payload,
                                    boolean guardStale, int maxAttempts, long deadlineNanos) {
        for (int attempt = 1; ; attempt++) {
            try {
                return publisher.publish(topic, event.getEventType(), event.getEventId(), event.getCabId(),
                        event.getTripId(), ingestTs, event.getTimestamp(), payload, guardStale);
            } catch (DataAccessException e) {
                metrics.recordRedisError("publish");
                long backoff = sosRetry.baseBackoffMillis() * attempt;
                boolean canRetry = attempt < maxAttempts
                        && System.nanoTime() + backoff * 1_000_000L < deadlineNanos;
                if (!canRetry) {
                    metrics.count("ingest.events", "type", event.getEventType().name(), "outcome", "unavailable");
                    throw new PublishUnavailableException("event store unavailable", e);
                }
                metrics.count("ingest.retries", "type", event.getEventType().name());
                logger.warn("Publish to {} failed (attempt {}/{}), retrying: {}", topic, attempt, maxAttempts,
                        e.getClass().getSimpleName());
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new PublishUnavailableException("interrupted while retrying", ie);
                }
            }
        }
    }

    private void archive(BaseTripEvent event, long tripSeq) {
        if (tripSeq <= 0) {
            return;
        }
        boolean queued = archiveWriter.offer(new ArchiveWriter.Record(event.getTripId(), event.getEventType(),
                tripSeq, objectMapper.valueToTree(event)));
        if (!queued) {
            metrics.count("archive.dropped", "reason", "queue-full");
        }
    }
}
