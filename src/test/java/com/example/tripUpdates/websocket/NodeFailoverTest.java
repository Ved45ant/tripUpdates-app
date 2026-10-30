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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Node-crash failure injection. Two gateway nodes share one Redis; clients are spread over both, events (locations,
 * STATUS, SOS) are published continuously, one node is killed mid-stream and its clients reconnect to the survivor and
 * resume with only what they remember. Every client must end up with every STATUS and SOS of its trips, exactly once
 * and in order, and clients of the surviving node must not notice at all.
 */
class NodeFailoverTest {

    private static GenericContainer<?> redisContainer;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private static RedisClient adminClient;
    private static StatefulRedisConnection<String, String> admin;

    private final ObjectMapper mapper = new ObjectMapper();

    /** One gateway node: the same wiring as production, minus Tomcat. */
    class Node {
        final String name;
        final ConnectionRegistry registry = new ConnectionRegistry();
        final ConnectionWriter writer = new ConnectionWriter(Executors.newVirtualThreadPerTaskExecutor(), DeliveryObserver.NOOP, 3000);
        final PubSubGateway gateway;
        final TripWebSocketHandler handler;
        volatile boolean crashed;

        Node(String name) {
            this.name = name;
            EventFanOut fanOut = new EventFanOut(registry, writer, DeliveryObserver.NOOP);
            RedisReplayBufferRepository replay = new RedisReplayBufferRepository(redis);
            gateway = new PubSubGateway(PubSubGateway.newClient(redisContainer.getHost(), redisContainer.getMappedPort(6379), null),
                    registry, fanOut, replay, new PubSubGateway.Config(4, 50_000, 150));
            gateway.start();
            SubscriptionAuthService allowAll = new SubscriptionAuthService(null) {
                @Override
                public boolean isAuthorized(String u, String t, UserRole r, String o, String topic) {
                    return true;
                }
            };
            handler = new TripWebSocketHandler(registry, allowAll, new RecoveryService(replay, fanOut, mapper), gateway, fanOut,
                    mapper, new MetricsService(new SimpleMeterRegistry()));
        }

        /** A process kill: no goodbye frames, no cleanup callbacks; sockets simply stop working. */
        void crash(List<Client> clients) {
            crashed = true;
            gateway.close();
            writer.close();
            clients.forEach(c -> {
                if (c.node == this) {
                    c.connectionLost();
                }
            });
        }
    }

    /**
     * A client that only remembers what a real one would: the highest STATUS/SOS sequence per topic and the baseline
     * sequence from its first ACK. It deduplicates by (topic, seq) and keeps the arrival log for the assertions.
     */
    class Client {
        final String name;
        final List<String> topics;
        volatile Node node;
        volatile Fake socket;
        final Map<String, Long> lastCritical = new HashMap<>();
        final Map<String, Long> baseline = new HashMap<>();
        final Set<String> seen = new HashSet<>();
        final Map<String, List<Long>> criticalArrival = new HashMap<>();
        final Map<String, Long> latestLocation = new HashMap<>();
        final Map<String, Long> firstSent = new HashMap<>();
        int processed;
        final AtomicBoolean connected = new AtomicBoolean();
        final List<Long> sosLatenciesMs = new CopyOnWriteArrayList<>();
        int reconnects;

        Client(String name, List<String> topics) {
            this.name = name;
            this.topics = topics;
        }

        void connect(Node target) {
            node = target;
            socket = new Fake(target);
            processed = 0;
            connected.set(true);
            for (String topic : topics) {
                long sent = Math.max(lastCritical.getOrDefault(topic, 0L), baseline.getOrDefault(topic, 0L));
                // baseline only becomes the resume point once the client has actually received events or been told one
                firstSent.putIfAbsent(topic, sent);
                socket.send("{\"action\":\"RESUME\",\"topic\":\"" + topic + "\",\"lastSeq\":" + sent + "}");
            }
        }

        void connectionLost() {
            connected.set(false);
            socket.dropped = true;
        }

        /** Reads what arrived since the last call. */
        synchronized void pump() {
            if (socket == null) {
                return;
            }
            List<JsonNode> frames = socket.frames;
            while (processed < frames.size()) {
                JsonNode f = frames.get(processed++);
                String type = f.get("type").asText();
                String topic = f.has("topic") && !f.get("topic").isNull() ? f.get("topic").asText() : null;
                if (type.equals("ACK") && topic != null && f.get("status").asText().equals("SUBSCRIBED")) {
                    if (firstSent.getOrDefault(topic, 0L) == 0L) {
                        baseline.putIfAbsent(topic, f.get("seq").asLong());
                    }
                } else if (type.equals("EVENT")) {
                    long seq = f.get("seq").asLong();
                    String eventType = f.get("eventType").asText();
                    if (!seen.add(topic + "|" + seq)) {
                        continue; // duplicate (replay overlap): at-least-once, dedup on the client
                    }
                    if (eventType.equals("LOCATION")) {
                        latestLocation.merge(topic, seq, Math::max);
                    } else {
                        criticalArrival.computeIfAbsent(topic, t -> new ArrayList<>()).add(seq);
                        lastCritical.merge(topic, seq, Math::max);
                        if (eventType.equals("SOS")) {
                            long sentAt = f.get("data").get("payload").get("sentAtMs").asLong();
                            sosLatenciesMs.add(System.currentTimeMillis() - sentAt);
                        }
                    }
                }
            }
        }
    }

    /** Fake server-side socket of one client connection. */
    class Fake {
        final Node node;
        final String id = UUID.randomUUID().toString();
        final List<JsonNode> frames = new CopyOnWriteArrayList<>();
        final AtomicReference<CloseStatus> closed = new AtomicReference<>();
        final Map<String, Object> attributes = new HashMap<>();
        volatile boolean dropped;
        final WebSocketSession ws;

        Fake(Node node) {
            this.node = node;
            attributes.put("userId", "u");
            attributes.put("tenantId", "t");
            attributes.put("role", "SECURITY_OPERATOR");
            ws = (WebSocketSession) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{WebSocketSession.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "getId":
                                return id;
                            case "getAttributes":
                                return attributes;
                            case "isOpen":
                                return closed.get() == null && !dropped;
                            case "sendMessage":
                                if (dropped) {
                                    throw new java.io.IOException("connection reset");
                                }
                                frames.add(mapper.readTree(((TextMessage) args[0]).getPayload()));
                                return null;
                            case "close":
                                closed.set(args != null && args.length > 0 ? (CloseStatus) args[0] : CloseStatus.NORMAL);
                                return null;
                            case "hashCode":
                                return id.hashCode();
                            case "equals":
                                return proxy == args[0];
                            default:
                                return null;
                        }
                    });
            try {
                node.handler.afterConnectionEstablished(ws);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        void send(String json) {
            node.handler.handleTextMessage(ws, new TextMessage(json));
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
    void flush() {
        admin.sync().flushall();
    }

    @Test
    void killingANodeMidStreamLosesNoStatusOrSosAndKeepsPerTripOrder() throws Exception {
        final int trips = 10;
        final int clientsPerTrip = 4;
        final int events = 2500;

        Node nodeA = new Node("A");
        Node nodeB = new Node("B");
        RedisEventPublisher publisher = new RedisEventPublisher(redis, 100, 300, 3600);

        List<Client> clients = new ArrayList<>();
        for (int t = 0; t < trips; t++) {
            for (int c = 0; c < clientsPerTrip; c++) {
                Client client = new Client("c" + t + "-" + c, List.of("trip:t" + t));
                client.connect(c % 2 == 0 ? nodeA : nodeB);
                clients.add(client);
            }
        }
        // every client is subscribed (the topic is empty: nothing to replay yet)
        clients.forEach(Client::pump);
        assertThat(clients).allSatisfy(c -> assertThat(c.baseline).containsKey(c.topics.get(0)));

        // a background pump plays the role of each client's receive loop and reconnect logic
        AtomicBoolean running = new AtomicBoolean(true);
        Thread pumper = new Thread(() -> {
            while (running.get()) {
                for (Client c : clients) {
                    c.pump();
                    if (!c.connected.get()) {
                        c.reconnects++;
                        c.connect(nodeB);                 // the load balancer routes to the survivor
                    }
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        pumper.start();

        // publish a mix of events; remember every STATUS/SOS sequence per trip
        Map<String, List<Long>> published = new HashMap<>();
        Map<String, Long> lastLocationSeq = new HashMap<>();
        Thread crasher = new Thread(() -> {
            try {
                Thread.sleep(1200);
            } catch (InterruptedException e) {
                return;
            }
            nodeA.crash(clients);
        });
        crasher.start();

        for (int i = 0; i < events; i++) {
            String trip = "t" + (i % trips);
            String topic = "trip:" + trip;
            EventType type = i % 7 == 3 ? EventType.STATUS : i % 31 == 5 ? EventType.SOS : EventType.LOCATION;
            long now = System.currentTimeMillis();
            String payload = "{\"payload\":{\"sentAtMs\":" + now + ",\"n\":" + i + "}}";
            long seq = publisher.publish(topic, type, "e" + i, "cab-" + trip, trip, now, payload).seq();
            if (type == EventType.LOCATION) {
                lastLocationSeq.put(topic, seq);
            } else {
                published.computeIfAbsent(topic, k -> new ArrayList<>()).add(seq);
            }
            Thread.sleep(1);
        }
        crasher.join();

        // settle: everything published must reach every client
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            for (Client c : clients) {
                String topic = c.topics.get(0);
                synchronized (c) {
                    assertThat(new TreeSet<>(c.criticalArrival.getOrDefault(topic, List.of())))
                            .as("%s critical events on %s", c.name, topic)
                            .containsExactlyInAnyOrderElementsOf(published.get(topic));
                }
            }
        });
        Thread.sleep(500);
        running.set(false);
        pumper.join();
        clients.forEach(Client::pump);

        assertThat(nodeA.crashed).isTrue();
        assertThat(clients.stream().filter(c -> c.reconnects > 0)).as("clients of the crashed node reconnected")
                .hasSize(trips * clientsPerTrip / 2);

        for (Client c : clients) {
            String topic = c.topics.get(0);
            List<Long> arrival = c.criticalArrival.get(topic);
            assertThat(arrival).as("%s: no duplicates delivered to the application", c.name).doesNotHaveDuplicates();
            assertThat(arrival).as("%s: critical events in sequence order across the failover", c.name).isSorted();
            assertThat(arrival).as("%s: every STATUS/SOS of %s", c.name, topic).containsExactlyElementsOf(published.get(topic));
            assertThat(c.latestLocation.get(topic)).as("%s: ends on the latest location", c.name).isEqualTo(lastLocationSeq.get(topic));
        }

        // clients on the surviving node never noticed: they never reconnected and SOS stayed fast for them
        List<Long> survivorSos = new ArrayList<>();
        clients.stream().filter(c -> c.reconnects == 0).forEach(c -> survivorSos.addAll(c.sosLatenciesMs));
        assertThat(survivorSos).isNotEmpty();
        assertThat(Collections.max(survivorSos)).as("worst SOS latency on the surviving node").isLessThan(2000);

        nodeB.gateway.close();
        nodeB.writer.close();
    }

    @Test
    void nodeShutdownClosesClientsSoTheyReconnectImmediately() throws Exception {
        Node node = new Node("A");
        Client client = new Client("c", List.of("trip:x"));
        client.connect(node);
        client.pump();

        node.handler.shutdown();                         // graceful stop: every client is told to go elsewhere

        assertThat(client.socket.closed.get()).isNotNull();
        assertThat(client.socket.closed.get().getCode()).isEqualTo(1013);
        node.gateway.close();
        node.writer.close();
    }
}
