package com.example.tripUpdates.repository.redis;

import com.example.tripUpdates.model.enums.EventType;
import com.example.tripUpdates.service.business.TopicPublisher.PublishResult;
import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository.ResumeData;
import com.example.tripUpdates.service.core.EnvelopeJson;
import com.example.tripUpdates.service.core.EventEnvelope;
import com.example.tripUpdates.service.core.ReplayDecision;
import com.example.tripUpdates.service.core.ReplayDecision.Action;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real-Redis tests of the atomic publish script and the resume read path (needs Docker). */
class RedisPublishPathTest {

    private static GenericContainer<?> redisContainer;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private static RedisClient pubSubClient;

    private RedisEventPublisher publisher;
    private RedisReplayBufferRepository replay;

    private StatefulRedisPubSubConnection<String, String> subscriber;
    private final List<String> received = Collections.synchronizedList(new ArrayList<>());

    @BeforeAll
    static void startRedis() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required");
        redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
        redisContainer.start();
        factory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getMappedPort(6379));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        redis.afterPropertiesSet();
        pubSubClient = RedisClient.create("redis://" + redisContainer.getHost() + ":" + redisContainer.getMappedPort(6379));
    }

    @AfterAll
    static void stopRedis() {
        if (pubSubClient != null) {
            pubSubClient.shutdown();
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
        redis.execute((org.springframework.data.redis.core.RedisCallback<Object>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
        publisher = new RedisEventPublisher(redis, 100, 300, 3600);
        replay = new RedisReplayBufferRepository(redis);
        received.clear();
    }

    @AfterEach
    void tearDown() {
        if (subscriber != null) {
            subscriber.close();
            subscriber = null;
        }
    }

    /** Single-threaded (Netty event loop) subscriber, so arrival order == Redis delivery order. */
    private void subscribe(String... channels) {
        subscriber = pubSubClient.connectPubSub();
        subscriber.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                received.add(channel + "|" + message);
            }
        });
        subscriber.sync().subscribe(channels);
    }

    private PublishResult publish(String topic, EventType type, String eventId, String cab, String trip) {
        return publisher.publish(topic, type, eventId, cab, trip, 1_000L, "{\"n\":\"" + eventId + "\"}");
    }

    private static void awaitSize(List<?> list, int n) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (list.size() < n && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    @RepeatedTest(20)
    void concurrentPublishersGetContiguousSequencesAndPubSubOrderEqualsSequenceOrder() throws Exception {
        final int threads = 32;
        final int perThread = 200;
        final int total = threads * perThread;
        String topic = "trip:ordering";
        redis.getConnectionFactory().getConnection().serverCommands().flushAll();
        received.clear();
        subscribe(RedisKeys.eventChannel(topic), RedisKeys.sosChannel(topic)); // critical events use their own channel

        Set<Long> seqs = Collections.synchronizedSet(new HashSet<>());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final int thread = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        EventType type = i % 4 == 0 ? EventType.STATUS : EventType.LOCATION;
                        seqs.add(publish(topic, type, "t" + thread + "-" + i, "cab" + thread, "ordering").seq());
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();
        assertThat(failures.get()).isZero();

        // every publish got a unique sequence and together they are exactly 1..N
        assertThat(seqs).hasSize(total);
        assertThat(seqs.stream().mapToLong(Long::longValue).min().getAsLong()).isEqualTo(1);
        assertThat(seqs.stream().mapToLong(Long::longValue).max().getAsLong()).isEqualTo(total);

        awaitSize(received, total);
        assertThat(received).hasSize(total);
        long previous = 0;
        for (String raw : received) {
            long seq = EnvelopeJson.parse(raw.substring(raw.indexOf('|') + 1)).seq();
            assertThat(seq).as("pub/sub delivery order must equal sequence order").isEqualTo(previous + 1);
            previous = seq;
        }
        assertThat(replay.currentSeq(topic)).isEqualTo(total);
    }

    @Test
    void duplicateEventIdReturnsOriginalSeqAndPublishesOnce() throws Exception {
        subscribe(RedisKeys.sosChannel("trip:d"), RedisKeys.sosChannel("cab:c1"));

        PublishResult first = publish("trip:d", EventType.STATUS, "evt-1", "c1", "d");
        PublishResult retry = publish("trip:d", EventType.STATUS, "evt-1", "c1", "d");
        PublishResult otherTopic = publish("cab:c1", EventType.STATUS, "evt-1", "c1", "d");
        PublishResult next = publish("trip:d", EventType.STATUS, "evt-2", "c1", "d");

        assertThat(first).isEqualTo(new PublishResult(1, false, false));
        assertThat(retry).isEqualTo(new PublishResult(1, true, false));
        assertThat(otherTopic).as("same eventId on another topic is independent").isEqualTo(new PublishResult(1, false, false));
        assertThat(next.seq()).isEqualTo(2);

        awaitSize(received, 3);
        assertThat(received).hasSize(3); // first, otherTopic, next: the retry published nothing
        assertThat(replay.criticalBufferSize("trip:d")).isEqualTo(2);
    }

    @Test
    void criticalBufferTrimsToNewestNAndRecordsFloor() {
        for (int i = 1; i <= 250; i++) {
            publish("trip:buf", EventType.STATUS, "s" + i, "c", "buf");
        }
        assertThat(replay.criticalBufferSize("trip:buf")).isEqualTo(100);
        assertThat(replay.criticalFloor("trip:buf")).isEqualTo(150);

        ResumeData caughtUp = replay.read("trip:buf", 200);
        assertThat(caughtUp.events()).extracting(EventEnvelope::seq)
                .containsExactlyElementsOf(java.util.stream.LongStream.rangeClosed(201, 250).boxed().toList());
        assertThat(ReplayDecision.decide(200, caughtUp.currentSeq(), caughtUp.criticalFloor())).isEqualTo(Action.REPLAY);

        ResumeData stale = replay.read("trip:buf", 120);
        assertThat(ReplayDecision.decide(120, stale.currentSeq(), stale.criticalFloor())).isEqualTo(Action.SNAPSHOT);
        ResumeData boundary = replay.read("trip:buf", 150);
        assertThat(ReplayDecision.decide(150, boundary.currentSeq(), boundary.criticalFloor())).isEqualTo(Action.REPLAY);
        assertThat(boundary.events()).hasSize(100);
    }

    @Test
    void locationsAreNeverBufferedSoTheyCannotEvictCriticalEvents() {
        publish("trip:mix", EventType.STATUS, "a", "c1", "mix");                  // seq 1
        for (int i = 0; i < 500; i++) {
            publish("trip:mix", EventType.LOCATION, "l" + i, "c1", "mix");       // seq 2..501
        }
        publish("trip:mix", EventType.STATUS, "b", "c1", "mix");                  // seq 502
        publish("trip:mix", EventType.SOS, "sos", "c1", "mix");                   // seq 503
        for (int i = 0; i < 100; i++) {
            publish("trip:mix", EventType.LOCATION, "m" + i, "c1", "mix");       // seq 504..603
        }

        assertThat(replay.criticalBufferSize("trip:mix")).isEqualTo(3);
        assertThat(replay.criticalFloor("trip:mix")).isZero();

        // client that saw seq 1 and then went into a lift: gets both missed critical events in order
        ResumeData data = replay.read("trip:mix", 1);
        assertThat(ReplayDecision.decide(1, data.currentSeq(), data.criticalFloor())).isEqualTo(Action.REPLAY);
        assertThat(data.events()).extracting(EventEnvelope::seq).containsExactly(502L, 503L);
        assertThat(data.events()).extracting(EventEnvelope::type).containsExactly(EventType.STATUS, EventType.SOS);
        // ... plus only the single latest location of that cab
        assertThat(data.snapshot().get("loc:c1").seq()).isEqualTo(603);
        assertThat(data.snapshot().keySet()).containsExactlyInAnyOrder("loc:c1", "status:mix", "sos:mix");
    }

    @Test
    void snapshotHoldsLatestPerFieldAndAllKeysHaveTtl() {
        publish("office:o1", EventType.LOCATION, "1", "cabA", "t1");
        publish("office:o1", EventType.LOCATION, "2", "cabB", "t2");
        publish("office:o1", EventType.LOCATION, "3", "cabA", "t1");
        publish("office:o1", EventType.STATUS, "4", "cabA", "t1");

        ResumeData data = replay.read("office:o1", 0);
        assertThat(data.snapshot().keySet()).containsExactlyInAnyOrder("loc:cabA", "loc:cabB", "status:t1");
        assertThat(data.snapshot().get("loc:cabA").seq()).isEqualTo(3);
        assertThat(data.snapshot().get("loc:cabB").seq()).isEqualTo(2);

        for (String key : List.of(RedisKeys.seq("office:o1"), RedisKeys.snapshot("office:o1"),
                RedisKeys.criticalBuffer("office:o1"))) {
            Long ttl = redis.getExpire(key);
            assertThat(ttl).as("TTL of %s", key).isNotNull().isGreaterThan(0).isLessThanOrEqualTo(3600);
        }
    }

    @Test
    void criticalEventsShareOneOrderedChannelAndLocationsUseAnother() throws Exception {
        subscribe(RedisKeys.sosChannel("trip:x"), RedisKeys.eventChannel("trip:x"));
        publish("trip:x", EventType.SOS, "s", "c1", "x");
        publish("trip:x", EventType.STATUS, "t", "c1", "x");
        publish("trip:x", EventType.LOCATION, "l", "c1", "x");

        awaitSize(received, 3);
        assertThat(received).hasSize(3);
        assertThat(received.get(0)).startsWith("sos:trip:x|");
        assertThat(received.get(1)).as("STATUS rides the critical channel with SOS, in order").startsWith("sos:trip:x|");
        assertThat(received.get(2)).startsWith("evt:trip:x|");
        assertThat(EnvelopeJson.parse(received.get(0).substring(received.get(0).indexOf('|') + 1)).type())
                .isEqualTo(EventType.SOS);
    }

    @Test
    void olderLocationOfTheSameCabIsDroppedWithoutConsumingASequence() throws Exception {
        subscribe(RedisKeys.eventChannel("trip:stale"));
        PublishResult first = guarded("trip:stale", "a", "cabS", 1_000);
        PublishResult newer = guarded("trip:stale", "b", "cabS", 2_000);
        PublishResult older = guarded("trip:stale", "c", "cabS", 1_500);   // arrives late
        PublishResult sameTime = guarded("trip:stale", "d", "cabS", 2_000); // equal timestamp is not older
        PublishResult otherCab = guarded("trip:stale", "e", "cabT", 100);   // other cab has its own clock

        assertThat(first).isEqualTo(new PublishResult(1, false, false));
        assertThat(newer).isEqualTo(new PublishResult(2, false, false));
        assertThat(older).isEqualTo(new PublishResult(0, false, true));
        assertThat(sameTime.seq()).isEqualTo(3);
        assertThat(otherCab.seq()).isEqualTo(4);
        awaitSize(received, 4);
        assertThat(received).as("the stale event was never published").hasSize(4);
        assertThat(replay.currentSeq("trip:stale")).isEqualTo(4);
    }

    @Test
    void retryOfAnAcceptedLocationIsADuplicateNotStale() {
        guarded("trip:retry", "x", "cabR", 5_000);
        guarded("trip:retry", "y", "cabR", 9_000);
        PublishResult retry = guarded("trip:retry", "x", "cabR", 5_000); // older than 9_000 but already accepted
        assertThat(retry).isEqualTo(new PublishResult(1, true, false));
    }

    @Test
    void staleGuardNeverAppliesToStatusOrSos() {
        guarded("trip:crit", "l", "cabC", 9_000);
        for (EventType type : new EventType[]{EventType.STATUS, EventType.SOS}) {
            PublishResult r = publisher.publish("trip:crit", type, "id-" + type, "cabC", "trip1", 1L, 1_000L, "{}", true);
            assertThat(r.stale()).as(type.name()).isFalse();
        }
    }

    private PublishResult guarded(String topic, String id, String cab, long eventTs) {
        return publisher.publish(topic, EventType.LOCATION, id, cab, "trip1", System.currentTimeMillis(), eventTs, "{}", true);
    }

    @Test
    void emptyTopicReadsAsZeroes() {
        ResumeData data = replay.read("trip:none", 0);
        assertThat(data.currentSeq()).isZero();
        assertThat(data.criticalFloor()).isZero();
        assertThat(data.events()).isEmpty();
        assertThat(data.snapshot()).isEmpty();
        assertThat(ReplayDecision.decide(0, 0, 0)).isEqualTo(Action.NOTHING);
    }
}
