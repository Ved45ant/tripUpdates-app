package com.example.tripUpdates.service.pubsub;

import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.repository.redis.RedisEventPublisher;
import com.example.tripUpdates.repository.redis.RedisKeys;
import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository;
import com.example.tripUpdates.service.core.ClientConnectionSession;
import com.example.tripUpdates.service.core.ConnectionRegistry;
import com.example.tripUpdates.service.core.ConnectionWriter;
import com.example.tripUpdates.service.core.DeliveryObserver;
import com.example.tripUpdates.service.core.EventFanOut;
import com.example.tripUpdates.service.core.FrameSink;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.KillArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Cross-node delivery, subscription lifecycle, SOS isolation and loss repair against a real Redis. */
class PubSubGatewayTest {

    /** Records frames; close() is observable. */
    static class Sink implements FrameSink {
        final List<String> frames = new CopyOnWriteArrayList<>();
        final List<Long> times = new CopyOnWriteArrayList<>();
        volatile boolean open = true;
        volatile String closeReason;

        public void send(String text) throws IOException {
            if (!open) {
                throw new IOException("closed");
            }
            frames.add(text);
            times.add(System.nanoTime());
        }

        public boolean isOpen() {
            return open;
        }

        public void close(String reason) {
            closeReason = reason;
            open = false;
        }

        Set<Long> seqs(String type) {
            Set<Long> out = new TreeSet<>();
            ObjectMapper m = new ObjectMapper();
            for (String f : frames) {
                try {
                    JsonNode n = m.readTree(f);
                    if (type == null || n.get("eventType").asText().equals(type)) {
                        out.add(n.get("seq").asLong());
                    }
                } catch (Exception ignored) {
                }
            }
            return out;
        }

        long timeOfFirst(String needle) {
            for (int i = 0; i < frames.size() && i < times.size(); i++) {
                if (frames.get(i).contains(needle)) {
                    return times.get(i);
                }
            }
            return -1;
        }
    }

    /** One simulated gateway node: its own registry, writer, fan-out and pub/sub gateway. */
    static class Node implements AutoCloseable {
        final ConnectionRegistry registry = new ConnectionRegistry();
        final ConnectionWriter writer = new ConnectionWriter(Executors.newVirtualThreadPerTaskExecutor(),
                DeliveryObserver.NOOP, 3000);
        final EventFanOut fanOut = new EventFanOut(registry, writer, DeliveryObserver.NOOP);
        final PubSubGateway gateway;
        private final ObjectMapper mapper = new ObjectMapper();

        Node(RedisReplayBufferRepository replay, long graceMillis) {
            RedisClient client = PubSubGateway.newClient(redisContainer.getHost(), redisContainer.getMappedPort(6379), null);
            gateway = new PubSubGateway(client, registry, fanOut, replay,
                    new PubSubGateway.Config(4, 50_000, graceMillis));
            gateway.start();
        }

        Sink connect(String sessionId, String topic) throws Exception {
            Sink sink = new Sink();
            registry.register(new ClientConnectionSession(sessionId, sink, mapper));
            registry.subscribe(sessionId, topic);
            gateway.whenSubscribed(topic).get(10, TimeUnit.SECONDS);
            return sink;
        }

        @Override
        public void close() {
            gateway.close();
            writer.close();
        }
    }

    private static GenericContainer<?> redisContainer;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private static RedisClient adminClient;
    private static StatefulRedisConnection<String, String> admin;

    private RedisEventPublisher publisher;
    private RedisReplayBufferRepository replay;
    private final List<Node> nodes = new ArrayList<>();

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
        publisher = new RedisEventPublisher(redis, 100, 300, 3600);
        replay = new RedisReplayBufferRepository(redis);
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(Node::close);
        nodes.clear();
    }

    private Node node(long graceMillis) {
        Node n = new Node(replay, graceMillis);
        nodes.add(n);
        return n;
    }

    private void publish(String topic, EventType type, String id) {
        publisher.publish(topic, type, id, "cab1", "trip1", System.currentTimeMillis(), "{\"id\":\"" + id + "\"}");
    }

    @Test
    void eventPublishedOnceReachesSubscribersOnEveryNode() throws Exception {
        Node a = node(200);
        Node b = node(200);
        Sink onA = a.connect("sa", "trip:x1");
        Sink onB = b.connect("sb", "trip:x1");

        publish("trip:x1", EventType.STATUS, "e1");

        await().atMost(Duration.ofSeconds(5)).until(() -> onA.seqs("STATUS").contains(1L) && onB.seqs("STATUS").contains(1L));
        assertThat(onA.frames).hasSize(1);
        assertThat(onB.frames).hasSize(1);
    }

    @Test
    void nodeIsSubscribedToRedisOnlyWhileItHasALocalSubscriber() throws Exception {
        Node a = node(200);
        Sink sink = a.connect("s1", "trip:life");
        assertThat(admin.sync().pubsubNumsub(RedisKeys.eventChannel("trip:life")).get(RedisKeys.eventChannel("trip:life"))).isEqualTo(1L);
        assertThat(admin.sync().pubsubNumsub(RedisKeys.sosChannel("trip:life")).get(RedisKeys.sosChannel("trip:life"))).isEqualTo(1L);
        assertThat(a.gateway.activeTopicCount()).isEqualTo(1);

        a.registry.unsubscribe("s1", "trip:life");
        await().atMost(Duration.ofSeconds(5)).until(() ->
                admin.sync().pubsubNumsub(RedisKeys.eventChannel("trip:life")).get(RedisKeys.eventChannel("trip:life")) == 0L);
        assertThat(a.gateway.activeTopicCount()).isZero();

        publish("trip:life", EventType.STATUS, "e1");
        Thread.sleep(300);
        assertThat(sink.frames).as("nothing delivered after unsubscribe").isEmpty();
    }

    @Test
    void sosIsDeliveredPromptlyDuringALocationFlood() throws Exception {
        Node a = node(200);
        Sink sink = a.connect("s1", "trip:flood");

        AtomicBoolean flooding = new AtomicBoolean(true);
        Thread flooder = new Thread(() -> {
            int i = 0;
            while (flooding.get()) {
                publisher.publish("trip:flood", EventType.LOCATION, "l" + i, "cab" + (i % 50), "trip1", 1L, "{}");
                i++;
            }
        });
        flooder.start();
        Thread.sleep(1000); // let the event path saturate

        long start = System.nanoTime();
        publisher.publish("trip:flood", EventType.SOS, "sos-1", "cab1", "trip1", System.currentTimeMillis(), "{\"level\":\"HIGH\"}");
        await().pollInterval(Duration.ofMillis(5)).atMost(Duration.ofSeconds(5))
                .until(() -> sink.timeOfFirst("\"eventType\":\"SOS\"") > 0);
        long latencyMs = (sink.timeOfFirst("\"eventType\":\"SOS\"") - start) / 1_000_000;
        flooding.set(false);
        flooder.join();

        assertThat(latencyMs).as("SOS latency while locations flood the same topic").isLessThan(1000);
    }

    @Test
    void lostMessageIsRepairedFromTheReplayBuffer() throws Exception {
        Node a = node(150);
        Sink sink = a.connect("s1", "trip:g");
        a.gateway.setMessageFilter((channel, json) -> !json.contains("\"seq\":3}")); // Redis "loses" seq 3

        for (int i = 1; i <= 5; i++) {
            publish("trip:g", EventType.STATUS, "e" + i);
        }

        await().atMost(Duration.ofSeconds(5)).until(() -> sink.seqs("STATUS").size() == 5);
        assertThat(sink.seqs("STATUS")).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(sink.frames).as("no duplicates").hasSize(5);
        assertThat(a.gateway.gapRepairs()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void loneLateMessageOnTheOtherChannelIsNotMistakenForLoss() throws Exception {
        Node a = node(500);
        Sink sink = a.connect("s1", "trip:late");
        // SOS (seq 2) is delayed longer than the events around it; within the grace period no repair is needed
        a.gateway.setMessageFilter((channel, json) -> true);
        publish("trip:late", EventType.STATUS, "e1");
        publish("trip:late", EventType.SOS, "e2");
        publish("trip:late", EventType.STATUS, "e3");
        await().atMost(Duration.ofSeconds(5)).until(() -> sink.seqs(null).size() == 3);
        Thread.sleep(800);
        assertThat(a.gateway.gapRepairs()).isZero();
        assertThat(sink.seqs(null)).containsExactly(1L, 2L, 3L);
    }

    @Test
    void lossBeyondTheBufferClosesLocalSubscribersSoTheyResume() throws Exception {
        publisher = new RedisEventPublisher(redis, 3, 300, 3600); // tiny buffer: only the newest 3 STATUS kept
        Node a = node(150);
        Sink sink = a.connect("s1", "trip:big");
        a.gateway.setMessageFilter((channel, json) -> {
            for (int s = 2; s <= 6; s++) {
                if (json.contains("\"seq\":" + s + "}")) {
                    return false;
                }
            }
            return true;
        });

        for (int i = 1; i <= 10; i++) {
            publish("trip:big", EventType.STATUS, "e" + i);
        }

        await().atMost(Duration.ofSeconds(5)).until(() -> !sink.open);
        assertThat(sink.closeReason).isEqualTo("resync-required");
        assertThat(a.gateway.resyncs()).isEqualTo(1);
    }

    @Test
    void redisConnectionDropDoesNotLoseCriticalEvents() throws Exception {
        Node a = node(150);
        Sink sink = a.connect("s1", "trip:kill");

        final int total = 200;
        Thread publishing = new Thread(() -> {
            for (int i = 1; i <= total; i++) {
                try {
                    publish("trip:kill", EventType.STATUS, "e" + i);
                    Thread.sleep(15);
                } catch (Exception ignored) {
                }
            }
        });
        publishing.start();
        Thread.sleep(500);
        admin.sync().clientKill(KillArgs.Builder.typePubsub()); // server drops every pub/sub connection
        publishing.join();

        await().atMost(Duration.ofSeconds(20)).until(() -> sink.seqs("STATUS").size() == total);
        assertThat(sink.seqs("STATUS")).hasSize(total);
        assertThat(sink.frames).as("each event delivered once").hasSize(total);
        assertThat(a.gateway.reconnects() + a.gateway.gapRepairs()).isGreaterThan(0);
    }
}
