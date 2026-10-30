package com.example.tripUpdates.service.business;

import com.example.tripUpdates.exception.InvalidTripTransitionException;
import com.example.tripUpdates.exception.PublishUnavailableException;
import com.example.tripUpdates.model.dto.LocationUpdateEvent;
import com.example.tripUpdates.model.dto.SosAlertEvent;
import com.example.tripUpdates.model.dto.TripStatusEvent;
import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.model.enums.TripStatus;
import com.example.tripUpdates.service.business.EventPublishingService.RetryPolicy;
import com.example.tripUpdates.service.business.TopicPublisher.PublishResult;
import com.example.tripUpdates.telemetry.MetricsService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** The ingest pipeline with a fake event store: retries, duplicates, stale events, transitions, archive isolation. */
class EventPublishingServiceTest {

    static final String TRIP = UUID.nameUUIDFromBytes("trip".getBytes()).toString();
    static final String CAB = UUID.nameUUIDFromBytes("cab".getBytes()).toString();
    static final String OFFICE = UUID.nameUUIDFromBytes("office".getBytes()).toString();
    static final long NOW = System.currentTimeMillis();

    /** In-memory event store with per-topic idempotency, a stale guard and scripted failures. */
    static class FakePublisher implements TopicPublisher {
        final Map<String, List<Long>> published = new HashMap<>();
        final Map<String, Long> seqs = new HashMap<>();
        final Set<String> seen = new HashSet<>();
        final Map<String, Long> lastLocationTs = new HashMap<>();
        final List<String> attempts = Collections.synchronizedList(new ArrayList<>());
        /** how many calls fail before succeeding, per topic prefix (e.g. "cab:") */
        final Map<String, Integer> failuresLeft = new HashMap<>();

        @Override
        public synchronized PublishResult publish(String topic, EventType type, String eventId, String cabId, String tripId,
                                                  long ingestTs, long eventTimestamp, String payloadJson, boolean guardStale) {
            attempts.add(topic);
            for (Map.Entry<String, Integer> f : failuresLeft.entrySet()) {
                if (topic.startsWith(f.getKey()) && f.getValue() > 0) {
                    f.setValue(f.getValue() - 1);
                    throw new QueryTimeoutException("redis timeout");
                }
            }
            String key = topic + "|" + eventId;
            if (seen.contains(key)) {
                return new PublishResult(seqs.get(key), true, false);
            }
            if (guardStale && type == EventType.LOCATION) {
                Long prev = lastLocationTs.get(cabId);
                if (prev != null && prev > eventTimestamp) {
                    return new PublishResult(0, false, true);
                }
                lastLocationTs.put(cabId, eventTimestamp);
            }
            long seq = seqs.merge(topic, 1L, Long::sum);
            seqs.put(key, seq);
            seen.add(key);
            published.computeIfAbsent(topic, t -> new ArrayList<>()).add(seq);
            return new PublishResult(seq, false, false);
        }
    }

    /** In-memory trip status using the real state machine. */
    static class FakeStatusStore implements TripStatusStore {
        final Map<UUID, TripStatus> status = new HashMap<>();

        @Override
        public synchronized TransitionResult transition(UUID tripId, TripStatus to) {
            TripStatus current = status.get(tripId);
            if (current == null) {
                return TransitionResult.NOT_FOUND;
            }
            if (!TripStateMachine.isAllowed(current, to)) {
                return TransitionResult.INVALID;
            }
            status.put(tripId, to);
            return TransitionResult.OK;
        }
    }

    FakePublisher store;
    FakeStatusStore trips;
    List<ArchiveWriter.Record> archived;
    ArchiveWriter archive;
    SimpleMeterRegistry meters;
    EventPublishingService service;

    @BeforeEach
    void setUp() {
        store = new FakePublisher();
        trips = new FakeStatusStore();
        trips.status.put(UUID.fromString(TRIP), TripStatus.SCHEDULED);
        archived = Collections.synchronizedList(new ArrayList<>());
        archive = new ArchiveWriter(archived::addAll, 100, 10, 10, 2, 5);
        meters = new SimpleMeterRegistry();
        service = new EventPublishingService(store, trips, archive, new ObjectMapper(), new MetricsService(meters),
                new RetryPolicy(3, 20, 500));
    }

    @AfterEach
    void tearDown() {
        archive.close();
    }

    private double count(String type, String outcome) {
        return meters.counter("ingest.events", "type", type, "outcome", outcome).count();
    }

    private LocationUpdateEvent location(String eventId, long ts) {
        LocationUpdateEvent e = new LocationUpdateEvent(CAB, TRIP, OFFICE, ts, new LocationUpdateEvent.LocationPayload(1, 2, 3));
        e.setEventId(eventId);
        return e;
    }

    private TripStatusEvent status(String eventId, TripStatus to) {
        TripStatusEvent e = new TripStatusEvent(CAB, TRIP, OFFICE, NOW, new TripStatusEvent.TripStatusPayload(to, "r"));
        e.setEventId(eventId);
        return e;
    }

    private SosAlertEvent sos(String eventId) {
        SosAlertEvent e = new SosAlertEvent(CAB, TRIP, OFFICE, NOW, new SosAlertEvent.SosAlertPayload("HIGH", "help", 1, 2));
        e.setEventId(eventId);
        return e;
    }

    // ------------------------------------------------------------------------------------ topics & duplicates

    @Test
    void anEventGoesToTripCabAndOfficeTopicsEachWithItsOwnSequence() {
        service.publishLocation(location("e1", NOW));
        service.publishLocation(location("e2", NOW + 1));

        assertThat(store.published.keySet()).containsExactlyInAnyOrder("trip:" + TRIP, "cab:" + CAB, "office:" + OFFICE);
        assertThat(store.published.get("trip:" + TRIP)).containsExactly(1L, 2L);
        assertThat(count("LOCATION", "accepted")).isEqualTo(2);
    }

    @Test
    void retryAfterAPartialFailureCompletesTheMissingTopicsWithoutDuplicatingTheFinishedOnes() {
        store.failuresLeft.put("cab:", 1);                        // the cab topic fails once
        LocationUpdateEvent event = location("e1", NOW);

        assertThatThrownBy(() -> service.publishLocation(event)).isInstanceOf(PublishUnavailableException.class);
        assertThat(store.published.get("trip:" + TRIP)).containsExactly(1L);   // trip topic already done
        assertThat(store.published).doesNotContainKey("cab:" + CAB);
        assertThat(count("LOCATION", "unavailable")).isEqualTo(1);

        service.publishLocation(event);                          // the client retries with the same eventId

        assertThat(store.published.get("trip:" + TRIP)).as("not published twice").containsExactly(1L);
        assertThat(store.published.get("cab:" + CAB)).containsExactly(1L);
        assertThat(store.published.get("office:" + OFFICE)).containsExactly(1L);
        assertThat(count("LOCATION", "duplicate")).as("retry recognised as already-seen on the trip topic").isEqualTo(1);
    }

    @Test
    void aStaleLocationIsDroppedAndPublishesNothing() {
        service.publishLocation(location("e1", NOW));
        service.publishLocation(location("e2", NOW - 10_000));   // arrives late

        assertThat(store.published.get("trip:" + TRIP)).containsExactly(1L);
        assertThat(store.published.get("cab:" + CAB)).containsExactly(1L);
        assertThat(count("LOCATION", "stale")).isEqualTo(1);
    }

    // ------------------------------------------------------------------------------------ SOS retries

    @Test
    void sosIsRetriedUntilRedisAcknowledges() {
        store.failuresLeft.put("trip:", 2);
        service.publishSos(sos("sos-1"));

        assertThat(store.attempts.stream().filter(t -> t.startsWith("trip:"))).hasSize(3);
        assertThat(store.published.get("trip:" + TRIP)).containsExactly(1L);
        assertThat(meters.counter("ingest.retries", "type", "SOS").count()).isEqualTo(2);
        assertThat(count("SOS", "accepted")).isEqualTo(1);
    }

    @Test
    void sosFailsWith503SemanticsAfterTheAttemptsAreExhausted() {
        store.failuresLeft.put("trip:", 100);
        long start = System.nanoTime();

        assertThatThrownBy(() -> service.publishSos(sos("sos-2"))).isInstanceOf(PublishUnavailableException.class);

        assertThat(store.attempts).hasSize(3);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(500));
        assertThat(store.published).isEmpty();
        assertThat(count("SOS", "unavailable")).isEqualTo(1);
    }

    @Test
    void sosRetriesStayInsideTheTimeBudget() {
        EventPublishingService slowBackoff = new EventPublishingService(store, trips, archive, new ObjectMapper(),
                new MetricsService(meters), new RetryPolicy(10, 400, 500));
        store.failuresLeft.put("trip:", 100);
        long start = System.nanoTime();

        assertThatThrownBy(() -> slowBackoff.publishSos(sos("sos-3"))).isInstanceOf(PublishUnavailableException.class);

        assertThat(store.attempts).as("second attempt after 400 ms, no third inside a 500 ms budget").hasSize(2);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(900));
    }

    @Test
    void nonSosEventsFailFastWithoutRetrying() {
        store.failuresLeft.put("trip:", 100);
        assertThatThrownBy(() -> service.publishLocation(location("e1", NOW))).isInstanceOf(PublishUnavailableException.class);
        assertThat(store.attempts).hasSize(1);
    }

    // ------------------------------------------------------------------------------------ status transitions

    @Test
    void statusFollowsTheLifecycleAndOutOfOrderEventsAreRejected() {
        service.publishStatus(status("s1", TripStatus.ACTIVE));
        service.publishStatus(status("s2", TripStatus.COMPLETED));

        long before = store.published.get("trip:" + TRIP).size();
        // a delayed ACTIVE event arriving after COMPLETED must not resurrect the trip
        assertThatThrownBy(() -> service.publishStatus(status("s3", TripStatus.ACTIVE)))
                .isInstanceOf(InvalidTripTransitionException.class);
        assertThat(store.published.get("trip:" + TRIP)).hasSize((int) before);
        assertThat(trips.status.get(UUID.fromString(TRIP))).isEqualTo(TripStatus.COMPLETED);
        assertThat(count("STATUS", "conflict")).isEqualTo(1);
    }

    @Test
    void aRetriedStatusIsNotAConflictEvenIfTheFirstAttemptAlreadyMovedTheTrip() {
        store.failuresLeft.put("trip:", 1);
        TripStatusEvent event = status("s1", TripStatus.ACTIVE);

        assertThatThrownBy(() -> service.publishStatus(event)).isInstanceOf(PublishUnavailableException.class);
        assertThat(trips.status.get(UUID.fromString(TRIP))).as("DB already moved").isEqualTo(TripStatus.ACTIVE);

        service.publishStatus(event);                             // same eventId, same target state

        assertThat(store.published.get("trip:" + TRIP)).containsExactly(1L);
    }

    @Test
    void unknownTripIsABadRequestNotAServerError() {
        trips.status.clear();
        assertThatThrownBy(() -> service.publishStatus(status("s1", TripStatus.ACTIVE)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------------------ archive

    @Test
    void statusAndSosAreArchivedWithTheTripSequenceButLocationsAndStaleEventsAreNot() {
        service.publishLocation(location("l1", NOW));
        service.publishStatus(status("s1", TripStatus.ACTIVE));
        service.publishSos(sos("sos-1"));
        service.publishLocation(location("l2", NOW - 5_000));     // stale

        await().atMost(Duration.ofSeconds(5)).until(() -> archived.size() == 2);
        assertThat(archived).extracting(ArchiveWriter.Record::type).containsExactly(EventType.STATUS, EventType.SOS);
        assertThat(archived).extracting(ArchiveWriter.Record::seq).containsExactly(2L, 3L);
        assertThat(archived).allSatisfy(r -> assertThat(r.tripId()).isEqualTo(TRIP));
    }

    @Test
    void aFullOrFailingArchiveNeverFailsOrSlowsPublishing() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        ArchiveWriter blocked = new ArchiveWriter(batch -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 3, 1, 10, 1, 1);
        EventPublishingService svc = new EventPublishingService(store, trips, blocked, new ObjectMapper(),
                new MetricsService(meters), RetryPolicy.defaults());
        trips.status.put(UUID.fromString(TRIP), TripStatus.ACTIVE);

        long start = System.nanoTime();
        AtomicInteger ok = new AtomicInteger();
        for (int i = 0; i < 100; i++) {
            svc.publishSos(sos("burst-" + i));
            ok.incrementAndGet();
        }
        assertThat(ok.get()).isEqualTo(100);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
        assertThat(blocked.dropped()).isGreaterThan(80);
        assertThat(meters.counter("archive.dropped", "reason", "queue-full").count()).isGreaterThan(80);
        assertThat(store.published.get("trip:" + TRIP)).hasSize(100);
        release.countDown();
        blocked.close();
    }
}
