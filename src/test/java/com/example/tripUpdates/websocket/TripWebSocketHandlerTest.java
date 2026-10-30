package com.example.tripUpdates.websocket;

import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.model.enums.UserRole;
import com.example.tripUpdates.repository.redis.RedisEventPublisher;
import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository;
import com.example.tripUpdates.service.business.SubscriptionAuthService;
import com.example.tripUpdates.service.core.ConnectionRegistry;
import com.example.tripUpdates.service.core.ConnectionWriter;
import com.example.tripUpdates.service.core.DeliveryObserver;
import com.example.tripUpdates.service.core.EventFanOut;
import com.example.tripUpdates.service.core.RecoveryService;
import com.example.tripUpdates.service.pubsub.PubSubGateway;
import com.example.tripUpdates.telemetry.MetricsService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Protocol tests of the WebSocket handler with the real delivery engine and a real Redis, using fake sockets.
 * Includes the case study's "phone in a lift" scenario.
 */
class TripWebSocketHandlerTest {

    private static GenericContainer<?> redisContainer;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private static StatefulRedisConnection<String, String> admin;
    private static RedisClient adminClient;

    private final ObjectMapper mapper = new ObjectMapper();
    private final ConnectionRegistry registry = new ConnectionRegistry();
    private ConnectionWriter writer;
    private PubSubGateway gateway;
    private TripWebSocketHandler handler;
    private RedisEventPublisher publisher;
    private SubscriptionAuthService auth;

    /** A fake client connection: records every frame the server sent and any close. */
    class Conn {
        final WebSocketSession ws;
        final List<JsonNode> frames = new CopyOnWriteArrayList<>();
        final AtomicReference<CloseStatus> closed = new AtomicReference<>();
        final Map<String, Object> attributes = new HashMap<>();
        final String id = UUID.randomUUID().toString();

        Conn() throws Exception {
            attributes.put("userId", "u1");
            attributes.put("tenantId", "t1");
            attributes.put("role", "EMPLOYEE");
            ws = (WebSocketSession) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{WebSocketSession.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "getId":
                                return id;
                            case "getAttributes":
                                return attributes;
                            case "isOpen":
                                return closed.get() == null;
                            case "sendMessage":
                                frames.add(mapper.readTree(((TextMessage) args[0]).getPayload()));
                                return null;
                            case "close":
                                closed.set(args != null && args.length > 0 ? (CloseStatus) args[0] : CloseStatus.NORMAL);
                                return null;
                            case "toString":
                                return "FakeSession-" + id;
                            case "hashCode":
                                return id.hashCode();
                            case "equals":
                                return proxy == args[0];
                            default:
                                return null;
                        }
                    });
            handler.afterConnectionEstablished(ws);
        }

        void send(String json) {
            handler.handleTextMessage(ws, new TextMessage(json));
        }

        void subscribe(String topic, long lastSeq) {
            send("{\"action\":\"SUBSCRIBE\",\"topic\":\"" + topic + "\",\"lastSeq\":" + lastSeq + "}");
        }

        void resume(String topic, long lastSeq) {
            send("{\"action\":\"RESUME\",\"topic\":\"" + topic + "\",\"lastSeq\":" + lastSeq + "}");
        }

        void disconnect() {
            handler.afterConnectionClosed(ws, CloseStatus.GOING_AWAY);
        }

        List<JsonNode> ofType(String type) {
            return frames.stream().filter(f -> f.get("type").asText().equals(type)).toList();
        }

        List<Long> seqs(String eventType) {
            return frames.stream()
                    .filter(f -> "EVENT".equals(f.get("type").asText()) && f.get("eventType").asText().equals(eventType))
                    .map(f -> f.get("seq").asLong()).toList();
        }

        long lastSeq() {
            return frames.stream().filter(f -> "EVENT".equals(f.get("type").asText()))
                    .mapToLong(f -> f.get("seq").asLong()).max().orElse(0);
        }
    }

    @BeforeAll
    static void startRedis() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required");
        redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
        redisContainer.start();
        factory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getMappedPort(6379));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        redis.afterPropertiesSet();
        adminClient = RedisClient.create("redis://" + redisContainer.getHost() + ":" + redisContainer.getMappedPort(6379));
        admin = adminClient.connect();
    }

    @AfterAll
    static void stopRedis() {
        if (adminClient != null) {
            adminClient.shutdown();
        }
        if (factory != null) {
            factory.destroy();
        }
        if (redisContainer != null) {
            redisContainer.stop();
        }
    }

    @BeforeEach
    void setUp() {
        admin.sync().flushall();
        buildNode(100);
    }

    private void buildNode(int bufferSize) {
        publisher = new RedisEventPublisher(redis, bufferSize, 300, 3600);
        RedisReplayBufferRepository replay = new RedisReplayBufferRepository(redis);
        writer = new ConnectionWriter(Executors.newVirtualThreadPerTaskExecutor(), DeliveryObserver.NOOP, 3000);
        EventFanOut fanOut = new EventFanOut(registry, writer, DeliveryObserver.NOOP);
        gateway = new PubSubGateway(PubSubGateway.newClient(redisContainer.getHost(), redisContainer.getMappedPort(6379), null),
                registry, fanOut, replay, new PubSubGateway.Config(4, 50_000, 200));
        gateway.start();
        auth = new SubscriptionAuthService(null) {
            @Override
            public boolean isAuthorized(String userId, String tenantId, UserRole role, String officeId, String topic) {
                if (topic.equals("trip:boom")) {
                    throw new IllegalStateException("db down");
                }
                return !topic.equals("trip:forbidden");
            }
        };
        handler = new TripWebSocketHandler(registry, auth, new RecoveryService(replay, fanOut, mapper), gateway, fanOut,
                mapper, new MetricsService(new SimpleMeterRegistry()));
    }

    @AfterEach
    void tearDown() {
        handler.shutdown();
        gateway.close();
        writer.close();
    }

    private long publish(String topic, EventType type, String id, String cab) {
        return publisher.publish(topic, type, id, cab, "trip1", System.currentTimeMillis(), "{\"id\":\"" + id + "\"}").seq();
    }

    // ---------------------------------------------------------------- the case study example

    @Test
    void phoneInALiftGetsTheThreeMissedStatusChangesInOrderAndOnlyTheLatestLocation() throws Exception {
        Conn first = new Conn();
        first.subscribe("trip:lift", 0);
        assertThat(first.ofType("ACK")).hasSize(1);

        publish("trip:lift", EventType.LOCATION, "l0", "cab1");
        publish("trip:lift", EventType.STATUS, "s0", "cab1");
        await().atMost(Duration.ofSeconds(5)).until(() -> first.seqs("STATUS").size() == 1 && first.seqs("LOCATION").size() == 1);
        long lastSeqSeen = first.lastSeq();

        // 40 seconds in a lift: the connection is gone, the world keeps moving
        first.disconnect();
        await().atMost(Duration.ofSeconds(5)).until(() -> gateway.activeTopicCount() == 0);
        List<Long> missedStatuses = new ArrayList<>();
        long lastLocationSeq = 0;
        for (int i = 0; i < 40; i++) {
            lastLocationSeq = publish("trip:lift", EventType.LOCATION, "l" + (i + 1), "cab1");
            if (i == 9 || i == 19 || i == 29) {
                missedStatuses.add(publish("trip:lift", EventType.STATUS, "s" + (i + 1), "cab1"));
            }
        }
        lastLocationSeq = Math.max(lastLocationSeq, 0);

        Conn second = new Conn();
        second.resume("trip:lift", lastSeqSeen);

        await().atMost(Duration.ofSeconds(5)).until(() -> second.seqs("STATUS").size() == 3 && second.seqs("LOCATION").size() == 1);
        Thread.sleep(300); // nothing else may trickle in
        assertThat(second.frames.get(0).get("type").asText()).as("ACK comes first").isEqualTo("ACK");
        assertThat(second.seqs("STATUS")).containsExactlyElementsOf(missedStatuses);
        assertThat(second.seqs("LOCATION")).containsExactly(lastLocationSeq);
        assertThat(second.ofType("SNAPSHOT")).isEmpty();
        assertThat(second.frames).hasSize(1 + 3 + 1);
    }

    // ---------------------------------------------------------------- resume variants

    @Test
    void clientFarBehindGetsASnapshotThenLiveEvents() throws Exception {
        buildTinyBuffer();
        for (int i = 1; i <= 10; i++) {
            publish("trip:far", EventType.STATUS, "s" + i, "cab1");
        }
        Conn c = new Conn();
        c.resume("trip:far", 1);

        await().atMost(Duration.ofSeconds(5)).until(() -> !c.ofType("SNAPSHOT").isEmpty());
        JsonNode snapshot = c.ofType("SNAPSHOT").get(0);
        assertThat(snapshot.get("data").get("currentSeq").asLong()).isEqualTo(10);
        assertThat(snapshot.get("data").get("latest").has("status:trip1")).isTrue();
        assertThat(c.seqs("STATUS")).as("old events are not replayed one by one").isEmpty();

        long live = publish("trip:far", EventType.STATUS, "live", "cab1");
        await().atMost(Duration.ofSeconds(5)).until(() -> c.seqs("STATUS").contains(live));
        assertThat(c.seqs("STATUS")).containsExactly(live);
    }

    private void buildTinyBuffer() {
        tearDown();
        registry.forEachSession(s -> registry.unregister(s.getSessionId()));
        buildNode(3);
    }

    @Test
    void newSubscriberOnAnActiveTopicStartsFromASnapshot() throws Exception {
        publish("trip:new", EventType.LOCATION, "l1", "cab1");
        publish("trip:new", EventType.STATUS, "s1", "cab1");

        Conn c = new Conn();
        c.subscribe("trip:new", 0);

        await().atMost(Duration.ofSeconds(5)).until(() -> !c.ofType("SNAPSHOT").isEmpty());
        JsonNode latest = c.ofType("SNAPSHOT").get(0).get("data").get("latest");
        assertThat(latest.has("loc:cab1")).isTrue();
        assertThat(latest.has("status:trip1")).isTrue();
    }

    @RepeatedTest(10)
    void resumeWhilePublishingNeverLosesDuplicatesOrReordersCriticalEvents() throws Exception {
        for (int i = 1; i <= 20; i++) {
            publish("trip:race", EventType.STATUS, "seed" + i, "cab1");
        }
        final int total = 300;
        Thread publishing = new Thread(() -> {
            for (int i = 21; i <= total; i++) {
                publish("trip:race", EventType.STATUS, "p" + i, "cab1");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        publishing.start();
        Thread.sleep(40);

        Conn c = new Conn();
        c.resume("trip:race", 5);
        publishing.join();

        await().atMost(Duration.ofSeconds(10)).until(() -> c.seqs("STATUS").size() >= total - 5);
        Thread.sleep(200);
        List<Long> statuses = c.seqs("STATUS");
        List<Long> expected = new ArrayList<>();
        for (long s = 6; s <= total; s++) {
            expected.add(s);
        }
        assertThat(statuses).as("every missed STATUS exactly once, in order").containsExactlyElementsOf(expected);
    }

    // ---------------------------------------------------------------- authorization & robustness

    @Test
    void deniedMalformedAndBrokenRequestsGetGenericErrorsAndTheConnectionStaysHealthy() throws Exception {
        Conn c = new Conn();

        c.subscribe("trip:forbidden", 0);
        c.resume("trip:forbidden", 5);                         // resume is authorized as strictly as subscribe
        c.subscribe("trip:boom", 0);                           // auth backend failure == denied, not a 500
        c.subscribe("garbage", 0);
        c.subscribe("trip:../../etc", 0);
        c.send("{\"action\":\"SUBSCRIBE\"}");                  // missing topic
        c.send("{not json");
        c.send("{\"action\":\"HACK\",\"topic\":\"trip:x\"}");
        c.send("{\"action\":\"SUBSCRIBE\",\"topic\":\"trip:x\",\"lastSeq\":-4}");
        c.send("x".repeat(TripWebSocketHandler.MAX_MESSAGE_CHARS + 1));

        List<JsonNode> errors = c.ofType("ERROR");
        assertThat(errors).hasSize(10);
        assertThat(errors.get(0).get("code").asInt()).isEqualTo(403);
        assertThat(errors.get(0).get("topic").asText()).isEqualTo("trip:forbidden");
        assertThat(errors.get(1).get("code").asInt()).isEqualTo(403);
        assertThat(errors.get(2).get("code").asInt()).isEqualTo(403);
        for (int i = 3; i <= 5; i++) {
            assertThat(errors.get(i).get("code").asInt()).isEqualTo(403);
            assertThat(errors.get(i).get("topic").isNull() || errors.get(i).get("topic").asText().isEmpty()).isTrue();
        }
        for (int i = 6; i <= 9; i++) {
            assertThat(errors.get(i).get("code").asInt()).isEqualTo(400);
        }
        // the same wording for every denial and nothing from the request is echoed back
        assertThat(errors.get(0).get("message").asText()).isEqualTo(errors.get(3).get("message").asText());
        assertThat(c.frames.toString()).doesNotContain("etc").doesNotContain("HACK").doesNotContain("db down");

        assertThat(registry.getSession(c.id).getSubscriptionCount()).isZero();
        assertThat(c.closed.get()).as("connection is still open").isNull();

        c.subscribe("trip:fine", 0);
        assertThat(c.ofType("ACK")).hasSize(1);
    }

    @Test
    void unsubscribeStopsDeliveryAndUnknownUnsubscribeIs404() throws Exception {
        Conn c = new Conn();
        c.subscribe("trip:u", 0);
        publish("trip:u", EventType.STATUS, "s1", "cab1");
        await().atMost(Duration.ofSeconds(5)).until(() -> c.seqs("STATUS").size() == 1);

        c.send("{\"action\":\"UNSUBSCRIBE\",\"topic\":\"trip:u\"}");
        assertThat(c.ofType("ACK")).extracting(f -> f.get("status").asText()).containsExactly("SUBSCRIBED", "UNSUBSCRIBED");
        publish("trip:u", EventType.STATUS, "s2", "cab1");
        Thread.sleep(400);
        assertThat(c.seqs("STATUS")).hasSize(1);

        c.send("{\"action\":\"UNSUBSCRIBE\",\"topic\":\"trip:u\"}");
        assertThat(c.ofType("ERROR").get(0).get("code").asInt()).isEqualTo(404);
    }

    @Test
    void aConnectionCannotHoldUnboundedSubscriptions() throws Exception {
        Conn c = new Conn();
        for (int i = 0; i < TripWebSocketHandler.MAX_SUBSCRIPTIONS_PER_CONNECTION; i++) {
            c.subscribe("trip:many" + i, 0);
        }
        assertThat(c.ofType("ACK")).hasSize(TripWebSocketHandler.MAX_SUBSCRIPTIONS_PER_CONNECTION);
        c.subscribe("trip:one-too-many", 0);
        assertThat(c.ofType("ERROR").get(0).get("code").asInt()).isEqualTo(429);
        c.subscribe("trip:many0", 0); // re-subscribing an existing topic is still allowed
        assertThat(c.ofType("ACK")).hasSize(TripWebSocketHandler.MAX_SUBSCRIPTIONS_PER_CONNECTION + 1);
    }

    @Test
    void expiredTokensAreRejectedOnTheNextMessageAndByThePeriodicSweep() throws Exception {
        Conn onMessage = new Conn();
        onMessage.attributes.put("expiresAtMs", System.currentTimeMillis() + 60_000);
        registry.getSession(onMessage.id).setExpiresAtMillis(System.currentTimeMillis() - 1);
        onMessage.subscribe("trip:e", 0);
        assertThat(onMessage.closed.get()).isNotNull();
        assertThat(onMessage.closed.get().getCode()).isEqualTo(CloseStatus.POLICY_VIOLATION.getCode());
        assertThat(onMessage.frames).isEmpty();

        Conn idle = new Conn();
        registry.getSession(idle.id).setExpiresAtMillis(System.currentTimeMillis() + 1_000);
        handler.sweepExpired(System.currentTimeMillis());
        assertThat(idle.closed.get()).isNull();
        handler.sweepExpired(System.currentTimeMillis() + 5_000);
        assertThat(idle.closed.get().getCode()).isEqualTo(CloseStatus.POLICY_VIOLATION.getCode());
    }
}
