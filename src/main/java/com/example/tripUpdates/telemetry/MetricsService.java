package com.example.tripUpdates.telemetry;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The counters and timers an on-call engineer needs at 3 AM. Only low-cardinality tags are used:
 * nothing is tagged per connection, per trip, per cab or per user.
 *
 * <pre>
 *   Is SOS fast?            sos.delivery.latency (histogram; p99 must stay under 2 s)
 *   Is anything lost?       events.dropped{stage=outbox,type=STATUS|SOS}  (must stay 0)
 *                           pubsub.messages.dropped, pubsub.gap.repairs, pubsub.resyncs, archive.dropped
 *   Is a client too slow?   outbox.depth, outbox.pending.events/.max, outbox.forced_resync{reason}, events.conflated.total,
 *                           ws.disconnects.total{reason}
 *   Is the pipeline lagging? pubsub.lag (ingest -> this node), pubsub.reconnects
 *   Is Redis healthy?       redis.errors{op}, ingest.retries{type}, ingest.events{outcome=unavailable}
 *   Is it an attack/misconfig? auth.rejections{surface}, ingest.events{outcome=forbidden|invalid|conflict}
 *   How are clients behaving? ws.connections, resume.total{action=NOTHING|REPLAY|SNAPSHOT}
 * </pre>
 */
@Service
public class MetricsService {

    private final MeterRegistry meterRegistry;
    private final Timer sosDeliveryLatency;
    private final Timer pubSubLag;
    private final Counter conflatedEvents;
    private final DistributionSummary outboxDepth;
    private final AtomicInteger connections = new AtomicInteger();

    public MetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        this.sosDeliveryLatency = Timer.builder("sos.delivery.latency")
                .description("SOS time from ingest to the socket write of each subscriber (SLA: p99 < 2 s)")
                .publishPercentiles(0.5, 0.9, 0.99)
                .publishPercentileHistogram()
                .serviceLevelObjectives(Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(250),
                        Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5))
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(meterRegistry);

        this.pubSubLag = Timer.builder("pubsub.lag")
                .description("Time from ingest on the publishing node to processing on this node")
                .publishPercentiles(0.5, 0.99)
                .publishPercentileHistogram()
                .serviceLevelObjectives(Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(50),
                        Duration.ofMillis(100), Duration.ofMillis(500), Duration.ofSeconds(2))
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(meterRegistry);

        this.conflatedEvents = Counter.builder("events.conflated.total")
                .description("LOCATION updates replaced by a newer one for the same cab in a slow client's outbox")
                .register(meterRegistry);

        this.outboxDepth = DistributionSummary.builder("outbox.depth")
                .description("Events pending in a connection outbox when its writer drains it")
                .publishPercentiles(0.5, 0.99)
                .serviceLevelObjectives(1, 5, 10, 50, 100, 500, 1000)
                .register(meterRegistry);

        Gauge.builder("ws.connections", connections, AtomicInteger::get)
                .description("Open WebSocket connections on this node")
                .register(meterRegistry);

        // STATUS and SOS are never dropped from an outbox (a full lane disconnects the client, which then resumes).
        // The series exist at 0 so a dashboard/alert can assert "== 0".
        Counter.builder("events.dropped").tag("stage", "outbox").tag("type", "STATUS").register(meterRegistry);
        Counter.builder("events.dropped").tag("stage", "outbox").tag("type", "SOS").register(meterRegistry);

        // Pre-register every known series at 0 so dashboards and alerts see "0", not "no data".
        for (String surface : new String[]{"login", "handshake", "subscribe", "ingest", "rest"}) {
            meterRegistry.counter("auth.rejections", "surface", surface);
        }
        for (String op : new String[]{"publish", "subscribe", "resume", "repair", "reconcile"}) {
            meterRegistry.counter("redis.errors", "op", op);
        }
        for (String action : new String[]{"NOTHING", "REPLAY", "SNAPSHOT"}) {
            meterRegistry.counter("resume.total", "action", action);
        }
        for (String reason : new String[]{"send-timeout", "send-failed", "resync-required", "sos-overflow", "writer-error", "token-expired"}) {
            meterRegistry.counter("ws.disconnects.total", "reason", reason);
        }
        for (String reason : new String[]{"resync-required", "sos-overflow"}) {
            meterRegistry.counter("outbox.forced_resync", "reason", reason);
        }
        for (String type : new String[]{"LOCATION", "STATUS", "SOS"}) {
            meterRegistry.counter("ingest.retries", "type", type);
            for (String outcome : new String[]{"accepted", "duplicate", "stale", "invalid", "forbidden", "conflict", "unavailable"}) {
                meterRegistry.counter("ingest.events", "type", type, "outcome", outcome);
            }
        }
        meterRegistry.counter("archive.dropped", "reason", "queue-full");
    }

    // ---- delivery ----

    public void recordSosLatencyMillis(long latencyMillis) {
        sosDeliveryLatency.record(latencyMillis, TimeUnit.MILLISECONDS);
    }

    public void incrementConflatedCount() {
        conflatedEvents.increment();
    }

    public void recordOutboxDepth(int depth) {
        outboxDepth.record(depth);
    }

    public void recordPubSubLagMillis(long millis) {
        pubSubLag.record(Math.max(0, millis), TimeUnit.MILLISECONDS);
    }

    /** Counts a forced connection close; closes caused by a full outbox lane also count as forced resyncs. */
    public void recordDisconnect(String reason) {
        meterRegistry.counter("ws.disconnects.total", "reason", reason).increment();
        if ("resync-required".equals(reason) || "sos-overflow".equals(reason)) {
            meterRegistry.counter("outbox.forced_resync", "reason", reason).increment();
        }
    }

    // ---- connections ----

    public void incrementActiveConnections() {
        connections.incrementAndGet();
    }

    public void decrementActiveConnections() {
        connections.decrementAndGet();
    }

    public int getActiveConnections() {
        return connections.get();
    }

    // ---- failures ----

    /** surface: login | handshake | subscribe | ingest | rest */
    public void recordAuthRejection(String surface) {
        meterRegistry.counter("auth.rejections", "surface", surface).increment();
    }

    /** op: publish | subscribe | resume | repair | reconcile */
    public void recordRedisError(String op) {
        meterRegistry.counter("redis.errors", "op", op).increment();
    }

    /** Increments a counter with the given name and key/value tags (low-cardinality values only). */
    public void count(String name, String... tags) {
        meterRegistry.counter(name, tags).increment();
    }
}
