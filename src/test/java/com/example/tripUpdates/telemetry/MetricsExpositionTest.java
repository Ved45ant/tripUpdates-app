package com.example.tripUpdates.telemetry;

import com.example.tripUpdates.config.DeliveryConfig;
import com.example.tripUpdates.config.PubSubConfig;
import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.repository.redis.RedisEventPublisher;
import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository;
import com.example.tripUpdates.service.core.ClientConnectionSession;
import com.example.tripUpdates.service.core.ConnectionRegistry;
import com.example.tripUpdates.service.core.ConnectionWriter;
import com.example.tripUpdates.service.core.EventFanOut;
import com.example.tripUpdates.service.core.FrameSink;
import com.example.tripUpdates.service.pubsub.PubSubGateway;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The gate for observability: wire the real metric producers, push events through, scrape the Prometheus
 * text format and check every series an on-call engineer relies on is there (and the "must be zero" ones are zero).
 */
class MetricsExpositionTest {

    private static GenericContainer<?> redisContainer;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;

    @BeforeAll
    static void startRedis() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required");
        redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
        redisContainer.start();
        factory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getMappedPort(6379));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        redis.afterPropertiesSet();
    }

    @AfterAll
    static void stopRedis() {
        if (factory != null) {
            factory.destroy();
        }
        if (redisContainer != null) {
            redisContainer.stop();
        }
    }

    @Test
    void prometheusExposesEverySeriesOnCallNeeds() throws Exception {
        PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsService metrics = new MetricsService(prometheus);
        MetricsDeliveryObserver observer = new MetricsDeliveryObserver(metrics);

        ConnectionRegistry registry = new ConnectionRegistry();
        ConnectionWriter writer = new ConnectionWriter(Executors.newVirtualThreadPerTaskExecutor(), observer, 2000);
        EventFanOut fanOut = new EventFanOut(registry, writer, observer);
        new DeliveryConfig().outboxGauges(registry).bindTo(prometheus);

        RedisReplayBufferRepository replay = new RedisReplayBufferRepository(redis);
        PubSubGateway gateway = new PubSubConfig().pubSubGateway(
                PubSubGateway.newClient(redisContainer.getHost(), redisContainer.getMappedPort(6379), null),
                registry, fanOut, replay, prometheus, metrics, 2, 1000, 200);
        gateway.start();

        List<String> frames = new CopyOnWriteArrayList<>();
        FrameSink sink = new FrameSink() {
            public void send(String text) {
                frames.add(text);
            }

            public boolean isOpen() {
                return true;
            }

            public void close(String reason) {
            }
        };
        ClientConnectionSession session = new ClientConnectionSession("s1", sink, new ObjectMapper());
        registry.register(session);
        registry.subscribe("s1", "trip:m1");
        gateway.whenSubscribed("trip:m1").get();
        metrics.incrementActiveConnections();

        RedisEventPublisher publisher = new RedisEventPublisher(redis, 100, 300, 3600);
        publisher.publish("trip:m1", EventType.LOCATION, "l1", "cab1", "trip1", System.currentTimeMillis(), "{}");
        publisher.publish("trip:m1", EventType.STATUS, "s1", "cab1", "trip1", System.currentTimeMillis(), "{}");
        publisher.publish("trip:m1", EventType.SOS, "sos1", "cab1", "trip1", System.currentTimeMillis(), "{}");
        await().atMost(Duration.ofSeconds(5)).until(() -> frames.size() == 3);
        writer.disconnect(registry.getSession("s1"), "resync-required");

        String scrape = prometheus.scrape();

        for (String series : List.of(
                "sos_delivery_latency_seconds_bucket", "sos_delivery_latency_seconds_count",
                "pubsub_lag_seconds_bucket", "outbox_depth_bucket", "outbox_pending_events", "outbox_pending_max",
                "events_conflated_total", "events_dropped_total{stage=\"outbox\",type=\"STATUS\"",
                "events_dropped_total{stage=\"outbox\",type=\"SOS\"", "outbox_forced_resync_total{reason=\"resync-required\"",
                "ws_connections", "ws_disconnects_total{reason=\"send-timeout\"", "auth_rejections_total{surface=\"subscribe\"",
                "auth_rejections_total{surface=\"ingest\"", "redis_errors_total{op=\"publish\"", "resume_total{action=\"SNAPSHOT\"",
                "ingest_events_total{outcome=\"unavailable\",type=\"SOS\"", "ingest_retries_total{type=\"SOS\"",
                "archive_dropped_total", "pubsub_messages_received_total", "pubsub_messages_dropped_total",
                "pubsub_gap_repairs_total", "pubsub_resyncs_total", "pubsub_reconnects_total")) {
            assertThat(scrape).as("missing series " + series).contains(series);
        }

        // the SOS really went through the instrumented path
        assertThat(sample(scrape, "sos_delivery_latency_seconds_count")).isEqualTo(1.0);
        assertThat(sample(scrape, "pubsub_lag_seconds_count")).isGreaterThanOrEqualTo(3.0);
        assertThat(sample(scrape, "outbox_forced_resync_total{reason=\"resync-required\"")).isEqualTo(1.0);
        assertThat(sample(scrape, "ws_connections")).isEqualTo(1.0);
        // invariants: critical events are never dropped from an outbox
        assertThat(sample(scrape, "events_dropped_total{stage=\"outbox\",type=\"STATUS\"")).isZero();
        assertThat(sample(scrape, "events_dropped_total{stage=\"outbox\",type=\"SOS\"")).isZero();
        // no per-connection / per-trip / per-cab label explosion
        assertThat(scrape).doesNotContain("connection_id").doesNotContain("session=").doesNotContain("trip1").doesNotContain("cab1");

        gateway.close();
        writer.close();
    }

    /** Value of the first scrape line that starts with the given prefix. */
    private static double sample(String scrape, String prefix) {
        for (String line : scrape.split("\n")) {
            if (line.startsWith(prefix)) {
                return Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
            }
        }
        throw new AssertionError("no sample starting with " + prefix);
    }
}
