package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.enums.EventType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

/** Slow-consumer isolation, priority and delivery behaviour using fake sockets (no Spring, no Redis). */
class ConnectionWriterIsolationTest {

    private static final String TOPIC = "trip:t1";

    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger conflated = new AtomicInteger();
    private final List<String> disconnects = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<Long> sosLatencies = new java.util.concurrent.CopyOnWriteArrayList<>();

    private ExecutorService executor;
    private ConnectionWriter writer;
    private ConnectionRegistry registry;
    private EventFanOut fanOut;
    private final AtomicLong seq = new AtomicLong();

    @BeforeEach
    void setUp() {
        DeliveryObserver observer = new DeliveryObserver() {
            @Override
            public void conflated() {
                conflated.incrementAndGet();
            }

            @Override
            public void disconnected(String reason) {
                disconnects.add(reason);
            }

            @Override
            public void sosDelivered(long latencyMillis) {
                sosLatencies.add(latencyMillis);
            }
        };
        executor = Executors.newVirtualThreadPerTaskExecutor();
        writer = new ConnectionWriter(executor, observer, 400);
        registry = new ConnectionRegistry();
        fanOut = new EventFanOut(registry, writer, observer);
    }

    @AfterEach
    void tearDown() {
        writer.close();
        executor.shutdownNow();
    }

    private ClientConnectionSession connect(String id, TestSink sink) {
        ClientConnectionSession s = new ClientConnectionSession(id, sink, mapper);
        registry.register(s);
        registry.subscribe(id, TOPIC);
        return s;
    }

    private EventEnvelope event(EventType type, String cab) {
        return new EventEnvelope(TOPIC, seq.incrementAndGet(), type, cab, System.currentTimeMillis(),
                "{\"n\":" + seq.get() + "}");
    }

    @Test
    void frozenConsumerDoesNotDelayOthersAndIsDisconnectedAfterTimeout() throws Exception {
        TestSink frozen = TestSink.frozen();
        ClientConnectionSession frozenSession = connect("frozen", frozen);
        List<TestSink> healthy = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            TestSink sink = TestSink.normal();
            healthy.add(sink);
            connect("ok" + i, sink);
        }

        // location + status traffic first: the frozen client's first write blocks forever
        for (int i = 0; i < 2_000; i++) {
            fanOut.fanOut(event(EventType.LOCATION, "cab" + (i % 20)));
        }
        fanOut.fanOut(event(EventType.STATUS, null));

        long start = System.nanoTime();
        fanOut.fanOut(event(EventType.SOS, "cab1"));

        long worstNanos = 0;
        for (TestSink sink : healthy) {
            await().pollInterval(Duration.ofMillis(5)).atMost(Duration.ofSeconds(3))
                    .until(() -> sink.countFramesContaining("\"eventType\":\"SOS\"") == 1);
            worstNanos = Math.max(worstNanos, sink.timeOfFirst("\"eventType\":\"SOS\"") - start);
        }
        long elapsedMs = worstNanos / 1_000_000; // write time of the slowest healthy client, not poll time
        assertThat(elapsedMs).as("SOS reached all 50 healthy clients while one client was frozen").isLessThan(500);

        // the frozen client is cut off by the watchdog and releases its writer task
        assertThat(frozen.awaitClosed(3_000)).as("frozen sink closed").isTrue();
        assertThat(frozen.closeReason).isEqualTo("send-timeout");
        assertThat(frozenSession.isOpen()).isFalse();
        assertThat(disconnects).contains("send-timeout");
        await().atMost(Duration.ofSeconds(2)).until(() -> writer.inFlightCount() == 0);

        // and its outbox never exceeded its bound
        assertThat(frozenSession.getOutbox().size()).isLessThanOrEqualTo(frozenSession.getOutbox().maxRetainedEvents());
    }

    @Test
    void slowConsumerHoldsAtMostLatestLocationPerCabAndEndsUpToDate() {
        TestSink slow = TestSink.slow(5);
        connect("slow", slow);

        int cabs = 10;
        int updates = 2_000;
        long lastSeq = 0;
        for (int i = 0; i < updates; i++) {
            EventEnvelope e = event(EventType.LOCATION, "cab" + (i % cabs));
            lastSeq = e.seq();
            fanOut.fanOut(e);
        }
        final long finalSeq = lastSeq;

        await().atMost(Duration.ofSeconds(5)).until(() -> slow.countFramesContaining("\"seq\":" + finalSeq + ",") == 1);
        assertThat(slow.frames.size()).as("conflation delivered far fewer frames than updates").isLessThan(updates / 2);
        assertThat(conflated.get()).isGreaterThan(0);
    }

    @Test
    void sosOvertakesAlreadyDrainedBacklogOnTheSameConnection() {
        TestSink slow = TestSink.slow(20);
        connect("slow", slow);

        for (int i = 0; i < 100; i++) {
            fanOut.fanOut(event(EventType.LOCATION, "cab" + i)); // 100 distinct cabs backlog (~2s to flush)
        }
        await().atMost(Duration.ofSeconds(2)).until(() -> !slow.frames.isEmpty()); // writer is mid-backlog
        fanOut.fanOut(event(EventType.SOS, "cab1"));

        await().atMost(Duration.ofSeconds(2)).until(() -> slow.countFramesContaining("\"eventType\":\"SOS\"") == 1);
        int position = 0;
        for (String f : slow.frames) {
            position++;
            if (f.contains("\"eventType\":\"SOS\"")) {
                break;
            }
        }
        assertThat(position).as("SOS written within the next couple of frames, not after the backlog").isLessThanOrEqualTo(12); // backlog is 100 frames; awaitility polls every 100 ms, which is ~5 frames
        assertThat(sosLatencies).hasSize(1);
        assertThat(sosLatencies.get(0)).isLessThan(500);
    }

    @Test
    void duplicateSeqIsWrittenOnlyOnceButSosOvertakeStillDeliversLowerSeq() {
        TestSink sink = TestSink.normal();
        ClientConnectionSession s = connect("c", sink);

        EventEnvelope status = new EventEnvelope(TOPIC, 3, EventType.STATUS, null, 1L, "{}");
        EventEnvelope sos = new EventEnvelope(TOPIC, 5, EventType.SOS, "cab1", 1L, "{}");
        fanOut.deliverTo(s, sos);
        fanOut.deliverTo(s, status);   // lower seq arriving after the SOS must still be delivered
        fanOut.deliverTo(s, status);   // exact duplicate must not
        fanOut.deliverTo(s, sos);

        await().atMost(Duration.ofSeconds(2)).until(() -> sink.frames.size() >= 2);
        assertThat(sink.frames).hasSize(2);
        assertThat(sink.countFramesContaining("\"seq\":5,")).isEqualTo(1);
        assertThat(sink.countFramesContaining("\"seq\":3,")).isEqualTo(1);
    }

    @Test
    void sosLaneOverflowDisconnectsInsteadOfDroppingSilently() {
        TestSink frozen = TestSink.frozen();
        ClientConnectionSession s = new ClientConnectionSession("tiny", frozen, mapper, new ConnectionOutbox(2, 4));
        registry.register(s);
        registry.subscribe("tiny", TOPIC);

        fanOut.fanOut(event(EventType.LOCATION, "cab1"));           // writer picks this up and blocks
        await().atMost(Duration.ofSeconds(1)).until(() -> writer.inFlightCount() == 1);
        fanOut.fanOut(event(EventType.SOS, "cab1"));
        fanOut.fanOut(event(EventType.SOS, "cab1"));
        fanOut.fanOut(event(EventType.SOS, "cab1"));                // third SOS cannot be stored

        assertThat(disconnects).contains("sos-overflow");
        assertThat(s.isOpen()).isFalse();
    }

    @Test
    void closedSessionsReceiveNothingAndAreSkipped() {
        TestSink sink = TestSink.normal();
        ClientConnectionSession s = connect("c", sink);
        registry.unregister("c");
        assertThat(fanOut.fanOut(event(EventType.STATUS, null))).isZero();
        assertThat(fanOut.deliverTo(s, event(EventType.STATUS, null))).isFalse();
        assertThat(sink.frames).isEmpty();
    }

    @Test
    void frameHasExpectedJsonShape() throws Exception {
        TestSink sink = TestSink.normal();
        connect("c", sink);
        fanOut.fanOut(new EventEnvelope(TOPIC, 42, EventType.STATUS, null, 1715421000000L, "{\"status\":\"ACTIVE\"}"));
        await().atMost(Duration.ofSeconds(2)).until(() -> sink.frames.size() == 1);

        var node = mapper.readTree(sink.frames.get(0));
        assertThat(node.get("type").asText()).isEqualTo("EVENT");
        assertThat(node.get("topic").asText()).isEqualTo(TOPIC);
        assertThat(node.get("seq").asLong()).isEqualTo(42);
        assertThat(node.get("eventType").asText()).isEqualTo("STATUS");
        assertThat(node.get("timestamp").asLong()).isEqualTo(1715421000000L);
        assertThat(node.get("data").get("status").asText()).isEqualTo("ACTIVE");
    }
}
