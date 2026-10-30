package com.example.tripUpdates.service.pubsub;

import com.example.tripUpdates.repository.redis.RedisKeys;
import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository;
import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository.ResumeData;
import com.example.tripUpdates.service.core.ConnectionRegistry;
import com.example.tripUpdates.service.core.EnvelopeJson;
import com.example.tripUpdates.service.core.EventEnvelope;
import com.example.tripUpdates.service.core.EventFanOut;
import com.example.tripUpdates.service.core.ReceiveTracker;
import com.example.tripUpdates.service.core.ReplayDecision;
import com.example.tripUpdates.service.core.ReplayDecision.Action;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cross-node delivery: connects Redis pub/sub to this node's {@link EventFanOut}.
 *
 * <ul>
 *   <li><b>Per-topic subscriptions.</b> It subscribes to {@code evt:{topic}} and {@code sos:{topic}} only while
 *       this node has a local subscriber for the topic (driven by {@link ConnectionRegistry.TopicListener}),
 *       so a node never receives traffic nobody on it wants.</li>
 *   <li><b>SOS isolation.</b> SOS channels use their own Redis connection and their own processing thread; event
 *       channels use another connection and per-topic striped, bounded workers. A location flood can delay or
 *       overflow only the event path.</li>
 *   <li><b>Loss repair.</b> Pub/sub is at-most-once. A {@link ReceiveTracker} per topic detects missing
 *       sequences; after a short grace period (the other channel may just be a little behind) the missing
 *       STATUS/SOS events are re-delivered from the replay buffer. If more critical events were lost than the
 *       buffer holds, local subscribers are closed so they resume with a snapshot. After a Redis reconnect every
 *       active topic is reconciled the same way.</li>
 *   <li><b>Never blocks I/O threads.</b> Callbacks only enqueue; Redis reads for repair run on virtual threads.</li>
 * </ul>
 *
 * Trade-off: a repaired event can reach a client after later events of the same topic (the client sees seq
 * numbers out of order). Clients dedup and order by seq, see {@code SeenWindow}.
 */
public class PubSubGateway implements ConnectionRegistry.TopicListener, AutoCloseable {

    public record Config(int stripes, int queueCapacityPerStripe, long gapGraceMillis) {
        public static Config defaults() {
            return new Config(4, 50_000, 200);
        }
    }

    /** Optional telemetry hooks. */
    public interface Metrics {
        default void lagMillis(long millis) {
        }

        default void redisError(String op) {
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(PubSubGateway.class);

    private static final class TopicState {
        final ReceiveTracker tracker = new ReceiveTracker();
        final AtomicBoolean gapCheckPending = new AtomicBoolean(false);
        final CompletableFuture<Void> ready = new CompletableFuture<>();
    }

    private final RedisClient client;
    private final ConnectionRegistry registry;
    private final EventFanOut fanOut;
    private final RedisReplayBufferRepository replayBuffer;
    private final Config config;

    private final ConcurrentHashMap<String, TopicState> topics = new ConcurrentHashMap<>();
    private final TopicDispatcher dispatcher;
    private final ExecutorService sosExecutor = Executors.newSingleThreadExecutor(daemon("pubsub-sos"));
    private final java.util.concurrent.ConcurrentLinkedQueue<Map.Entry<String, TopicState>> pendingSubscribes = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final ExecutorService control = Executors.newSingleThreadExecutor(daemon("pubsub-control"));
    private final ExecutorService repairExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(daemon("pubsub-timer"));

    private StatefulRedisPubSubConnection<String, String> eventConn;
    private StatefulRedisPubSubConnection<String, String> sosConn;
    private volatile boolean started;
    private volatile boolean everConnected;
    private volatile Metrics metrics = new Metrics() {
    };
    private volatile java.util.function.BiPredicate<String, String> messageFilter;

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong parseErrors = new AtomicLong();
    private final AtomicLong gapRepairs = new AtomicLong();
    private final AtomicLong resyncs = new AtomicLong();
    private final AtomicLong reconnects = new AtomicLong();

    public PubSubGateway(RedisClient client, ConnectionRegistry registry, EventFanOut fanOut,
                         RedisReplayBufferRepository replayBuffer, Config config) {
        this.client = client;
        this.registry = registry;
        this.fanOut = fanOut;
        this.replayBuffer = replayBuffer;
        this.config = config;
        this.dispatcher = new TopicDispatcher(config.stripes(), config.queueCapacityPerStripe(), "pubsub-evt");
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    public static RedisClient newClient(String host, int port, String password) {
        io.lettuce.core.RedisURI.Builder uri = io.lettuce.core.RedisURI.builder().withHost(host).withPort(port);
        if (password != null && !password.isBlank()) {
            uri.withPassword(password.toCharArray());
        }
        RedisClient client = RedisClient.create(uri.build());
        client.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                // while disconnected, fail commands fast; subscriptions are retried on reconnect
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(2)))
                .build());
        return client;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    public void start() {
        if (started) {
            return;
        }
        eventConn = client.connectPubSub();
        sosConn = client.connectPubSub();
        eventConn.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                inbound(channel, message);
            }
        });
        sosConn.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                inbound(channel, message);
            }
        });
        client.addListener(new RedisConnectionStateListener() {
            @Override
            public void onRedisConnected(io.lettuce.core.RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
                if (everConnected) {
                    reconnects.incrementAndGet();
                    scheduler.schedule(() -> repairExecutor.execute(PubSubGateway.this::reconcileAll),
                            config.gapGraceMillis(), TimeUnit.MILLISECONDS);
                }
            }

            @Override
            public void onRedisDisconnected(io.lettuce.core.RedisChannelHandler<?, ?> connection) {
                logger.warn("Redis pub/sub connection lost; Lettuce will reconnect and resubscribe");
            }
        });
        everConnected = true;
        registry.setTopicListener(this);
        scheduler.scheduleWithFixedDelay(this::retryPendingSubscriptions, 1, 1, TimeUnit.SECONDS);
        started = true;
        logger.info("PubSubGateway started (stripes={}, queue={}, gapGrace={}ms)", config.stripes(),
                config.queueCapacityPerStripe(), config.gapGraceMillis());
    }

    // ---- topic lifecycle (called by ConnectionRegistry under the per-topic lock: must not block) ----

    @Override
    public void onFirstSubscriber(String topic) {
        TopicState state = new TopicState();
        topics.put(topic, state);
        enqueueSubscribe(topic, state);
    }

    @Override
    public void onLastUnsubscriber(String topic) {
        topics.remove(topic);
        control.execute(() -> {
            try {
                eventConn.sync().unsubscribe(RedisKeys.eventChannel(topic));
                sosConn.sync().unsubscribe(RedisKeys.sosChannel(topic));
            } catch (RuntimeException e) {
                logger.debug("Unsubscribe failed for {}: {}", topic, e.getClass().getSimpleName());
            }
        });
    }

    private static final int SUBSCRIBE_BATCH = 256;

    private void enqueueSubscribe(String topic, TopicState state) {
        pendingSubscribes.add(Map.entry(topic, state));
        control.execute(this::drainSubscribes);
    }

    /**
     * Subscribes everything queued so far with ONE command per connection. A connection storm (thousands of
     * clients, hundreds of new topics) would otherwise queue two blocking round trips per topic behind each other
     * and outlast the clients' subscribe timeout.
     */
    private void drainSubscribes() {
        List<Map.Entry<String, TopicState>> batch = new ArrayList<>();
        Map.Entry<String, TopicState> next;
        while (batch.size() < SUBSCRIBE_BATCH && (next = pendingSubscribes.poll()) != null) {
            if (topics.get(next.getKey()) == next.getValue() && !next.getValue().ready.isDone()) {
                batch.add(next); // stale generations and finished ones are skipped
            }
        }
        if (batch.isEmpty()) {
            return;
        }
        try {
            eventConn.sync().subscribe(batch.stream().map(e -> RedisKeys.eventChannel(e.getKey())).toArray(String[]::new));
            sosConn.sync().subscribe(batch.stream().map(e -> RedisKeys.sosChannel(e.getKey())).toArray(String[]::new));
            for (Map.Entry<String, TopicState> e : batch) {
                // baseline: older history is the resume flow's job
                e.getValue().tracker.advanceTo(replayBuffer.currentSeq(e.getKey()));
                e.getValue().ready.complete(null);
            }
        } catch (RuntimeException e) {
            metrics.redisError("subscribe");
            logger.warn("Subscribe of {} topics failed, will retry: {}", batch.size(), e.getClass().getSimpleName());
        }
    }

    private void retryPendingSubscriptions() {
        topics.forEach((topic, state) -> {
            if (!state.ready.isDone()) {
                enqueueSubscribe(topic, state);
            }
        });
    }

    /**
     * Completes once this node is actually subscribed to the topic's Redis channels. A resume must
     * wait for it before reading the replay buffer, otherwise events published in between could be missed.
     */
    public CompletableFuture<Void> whenSubscribed(String topic) {
        TopicState state = topics.get(topic);
        return state == null ? CompletableFuture.completedFuture(null) : state.ready;
    }

    // ---- message path ----

    /**
     * Fault-injection seam (tests, chaos runs): messages for which the filter returns false are
     * discarded as if Redis had lost them. Null (default) keeps everything.
     */
    public void setMessageFilter(java.util.function.BiPredicate<String, String> keep) {
        this.messageFilter = keep;
    }

    private void inbound(String channel, String json) {
        java.util.function.BiPredicate<String, String> filter = messageFilter;
        if (filter == null || filter.test(channel, json)) {
            handleMessage(channel, json);
        }
    }

    /** Entry point for every pub/sub message. Runs on a Redis I/O thread: only enqueues. */
    public void handleMessage(String channel, String json) {
        received.incrementAndGet();
        if (channel.startsWith(RedisKeys.SOS_CHANNEL_PREFIX)) {
            String topic = channel.substring(RedisKeys.SOS_CHANNEL_PREFIX.length());
            sosExecutor.execute(() -> process(topic, json));
        } else if (channel.startsWith(RedisKeys.EVENT_CHANNEL_PREFIX)) {
            String topic = channel.substring(RedisKeys.EVENT_CHANNEL_PREFIX.length());
            if (!dispatcher.execute(topic, () -> process(topic, json))) {
                dropped.incrementAndGet(); // overloaded: gap detection repairs lost STATUS/SOS later
            }
        }
    }

    private void process(String topic, String json) {
        TopicState state = topics.get(topic);
        if (state == null) {
            return; // unsubscribed meanwhile
        }
        EventEnvelope event;
        try {
            event = EnvelopeJson.parse(json);
        } catch (Exception e) {
            parseErrors.incrementAndGet();
            return;
        }
        state.tracker.record(event.seq());
        metrics.lagMillis(System.currentTimeMillis() - event.ingestTs());
        fanOut.fanOut(event);
        if (state.tracker.hasGap()) {
            scheduleGapCheck(topic, state);
        }
    }

    // ---- loss repair ----

    private void scheduleGapCheck(String topic, TopicState state) {
        if (state.gapCheckPending.compareAndSet(false, true)) {
            scheduler.schedule(() -> repairExecutor.execute(() -> repair(topic, state)),
                    config.gapGraceMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void repair(String topic, TopicState state) {
        state.gapCheckPending.set(false);
        if (topics.get(topic) != state || !state.tracker.hasGap()) {
            return; // gone, or the late message arrived during the grace period
        }
        long from = state.tracker.contiguous();
        long upTo = state.tracker.highest();
        try {
            catchUp(topic, from);
            state.tracker.markRepaired();
            gapRepairs.incrementAndGet();
            logger.info("Repaired suspected loss on {} after seq {} (up to {})", topic, from, upTo);
        } catch (RuntimeException e) {
            metrics.redisError("repair");
            logger.warn("Gap repair for {} failed, will retry: {}", topic, e.getClass().getSimpleName());
            scheduleGapCheck(topic, state);
        }
    }

    private void reconcileAll() {
        topics.forEach((topic, state) -> {
            if (!state.ready.isDone()) {
                return;
            }
            try {
                long from = state.tracker.highest();
                long current = catchUp(topic, from);
                state.tracker.advanceTo(current);
            } catch (RuntimeException e) {
                metrics.redisError("reconcile");
                logger.warn("Reconcile of {} after reconnect failed: {}", topic, e.getClass().getSimpleName());
            }
        });
    }

    /** Re-delivers buffered STATUS/SOS after {@code from}, or closes local subscribers if too much was lost. @return topic's current seq */
    private long catchUp(String topic, long from) {
        ResumeData data = replayBuffer.read(topic, from);
        Action action = ReplayDecision.decide(from, data.currentSeq(), data.criticalFloor());
        if (action == Action.SNAPSHOT) {
            resyncs.incrementAndGet();
            logger.warn("Too many events lost on {} (after seq {}); closing local subscribers to resync", topic, from);
            fanOut.resyncAll(topic);
        } else {
            for (EventEnvelope e : data.events()) {
                fanOut.fanOut(e);
            }
        }
        return data.currentSeq();
    }

    // ---- stats / lifecycle ----

    public int activeTopicCount() {
        return topics.size();
    }

    public long received() {
        return received.get();
    }

    public long dropped() {
        return dropped.get();
    }

    public long parseErrors() {
        return parseErrors.get();
    }

    public long gapRepairs() {
        return gapRepairs.get();
    }

    public long resyncs() {
        return resyncs.get();
    }

    public long reconnects() {
        return reconnects.get();
    }

    @Override
    public void close() {
        started = false;
        scheduler.shutdownNow();
        control.shutdownNow();
        sosExecutor.shutdownNow();
        repairExecutor.shutdownNow();
        dispatcher.close();
        try {
            if (eventConn != null) {
                eventConn.close();
            }
            if (sosConn != null) {
                sosConn.close();
            }
        } catch (RuntimeException e) {
            logger.debug("Error closing pub/sub connections", e);
        }
    }
}
