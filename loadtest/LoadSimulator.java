import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load and failure simulator for the live trip updates gateway. JDK only: run with
 * <pre>  java LoadSimulator.java --connections=10000 --cabs=2000 --duration=90 --kill-node=trip-gateway-2 --kill-at=40</pre>
 *
 * What it does
 * <ol>
 *   <li>Logs in as the demo admin and security operator, mints one device token per cab.</li>
 *   <li>Opens N WebSocket consumers: a few "security desks" on the office topic (every event of every trip) and
 *       the rest as trip watchers (one trip each), all with the reference client protocol: dedup, resume with the
 *       highest critical sequence, reconnect with jittered backoff.</li>
 *   <li>Publishes: every cab sends a LOCATION on an interval, occasional STATUS, and a steady stream of SOS.</li>
 *   <li>Optionally kills a gateway container mid-run.</li>
 *   <li>Verifies, per consumer, that every acknowledged STATUS/SOS arrived exactly once and in order, and reports
 *       SOS latency percentiles measured from the moment the successful POST attempt started.</li>
 * </ol>
 * Every STATUS/SOS carries a per-trip counter {@code n} in its text field, which is how loss is detected end to end.
 */
public class LoadSimulator {

    // ------------------------------------------------------------------ configuration
    static final Map<String, String> ARGS = new HashMap<>();
    static String base;
    static String wsBase;
    static int connections, cabs, desks, durationSec, killAtSec, locIntervalMs, sosIntervalMs, statusIntervalSec, settleSec;
    static String killNode;

    static final String TENANT = "12345678-1234-1234-1234-123456789012";
    static final String OFFICE = "12345678-1234-1234-1234-123456789013";
    static final String ADMIN = "12345678-1234-1234-1234-123456789016";
    static final String OPERATOR = "12345678-1234-1234-1234-123456789017";

    static int intArg(String key, int def) {
        return Integer.parseInt(ARGS.getOrDefault(key, String.valueOf(def)));
    }

    // ------------------------------------------------------------------ shared state
    static final HttpClient[] WS_CLIENTS = new HttpClient[8];
    static HttpClient http;
    static final ScheduledExecutorService TIMER = Executors.newScheduledThreadPool(4);
    static final AtomicBoolean STOPPING = new AtomicBoolean();
    static String operatorToken;
    static final String[] CAB_ID = new String[100_000];
    static final String[] TRIP_ID = new String[100_000];
    static final String[] CAB_TOKEN = new String[100_000];
    static final Map<String, Integer> TRIP_INDEX = new HashMap<>();

    static final AtomicIntegerArray NEXT_N = new AtomicIntegerArray(100_000);
    static final Set<Long> ACKED = ConcurrentHashMap.newKeySet();           // tripIdx * 1_000_000 + n
    static final AtomicLong POSTS = new AtomicLong(), POST_FAIL = new AtomicLong(), POST_RETRIES = new AtomicLong();
    static final AtomicLong LOC_OK = new AtomicLong(), STATUS_OK = new AtomicLong(), SOS_OK = new AtomicLong();
    static final AtomicIntegerArray POST_MS = new AtomicIntegerArray(10_002);
    static final List<Long> SOS_LATENCY_ALL = Collections.synchronizedList(new ArrayList<>());
    static final List<Long> SOS_LATENCY_DESK = Collections.synchronizedList(new ArrayList<>());
    /** Distinguishes this run's eventIds from earlier runs (the server dedups eventIds for 5 minutes). */
    static final String RUN = Long.toString(System.currentTimeMillis(), 36);
    static final AtomicInteger CONNECTED = new AtomicInteger(), RECONNECTS = new AtomicInteger(), CONNECT_FAIL = new AtomicInteger();
    static final AtomicLong ERROR_FRAMES = new AtomicLong();
    static final List<Consumer> CONSUMERS = new ArrayList<>();

    // ------------------------------------------------------------------ tiny JSON helpers (frames are flat enough)
    static long longAfter(String json, String key) {
        int i = json.indexOf(key);
        if (i < 0) {
            return -1;
        }
        i += key.length();
        int j = i;
        while (j < json.length() && Character.isDigit(json.charAt(j))) {
            j++;
        }
        return j == i ? -1 : Long.parseLong(json, i, j, 10);
    }

    static String strAfter(String json, String key) {
        int i = json.indexOf(key);
        if (i < 0) {
            return null;
        }
        i += key.length();
        int j = json.indexOf('"', i);
        return j < 0 ? null : json.substring(i, j);
    }

    // ------------------------------------------------------------------ consumer
    static final class Consumer implements WebSocket.Listener {
        final int id;
        final boolean desk;
        final String topic;
        final int tripIdx;                         // -1 for desks
        final StringBuilder buf = new StringBuilder();
        volatile WebSocket ws;
        volatile boolean connected;
        boolean everConnected;
        long lastCritical, baseline = -1, lastSent;
        long lastCriticalArrival;
        final Set<Long> seenSeq = new HashSet<>();
        final Set<Long> received = new HashSet<>();     // tripIdx * 1_000_000 + n
        int duplicates, orderViolations;
        int attempt;

        Consumer(int id, boolean desk, int tripIdx) {
            this.id = id;
            this.desk = desk;
            this.tripIdx = tripIdx;
            this.topic = desk ? "office:" + OFFICE : "trip:" + TRIP_ID[tripIdx];
        }

        void connect() {
            if (STOPPING.get()) {
                return;
            }
            HttpClient client = WS_CLIENTS[id % WS_CLIENTS.length];
            client.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(20))
                    .buildAsync(URI.create(wsBase + "/ws/updates?token=" + operatorToken), this)
                    .whenComplete((socket, error) -> {
                        if (error != null) {
                            CONNECT_FAIL.incrementAndGet();
                            scheduleReconnect();
                        }
                    });
        }

        void scheduleReconnect() {
            if (STOPPING.get()) {
                return;
            }
            attempt++;
            long delay = Math.min(5000, 200L * (1L << Math.min(attempt, 5))) / 2 + ThreadLocalRandom.current().nextLong(500);
            TIMER.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
        }

        @Override
        public synchronized void onOpen(WebSocket webSocket) {
            ws = webSocket;
            connected = true;
            CONNECTED.incrementAndGet();
            if (everConnected) {
                RECONNECTS.incrementAndGet();
            }
            everConnected = true;
            attempt = 0;
            lastSent = Math.max(lastCritical, Math.max(baseline, 0));
            buf.setLength(0);
            webSocket.sendText("{\"action\":\"RESUME\",\"topic\":\"" + topic + "\",\"lastSeq\":" + lastSent + "}", true);
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            synchronized (this) {
                buf.append(data);
                if (last) {
                    handle(buf.toString());
                    buf.setLength(0);
                }
            }
            webSocket.request(1);
            return null;
        }

        private void handle(String f) {
            if (f.startsWith("{\"type\":\"EVENT\"")) {
                long seq = longAfter(f, "\"seq\":");
                if (!seenSeq.add(seq)) {
                    duplicates++;
                    return;
                }
                String type = strAfter(f, "\"eventType\":\"");
                if ("LOCATION".equals(type)) {
                    return;
                }
                if (seq < lastCriticalArrival) {
                    orderViolations++;
                }
                lastCriticalArrival = Math.max(lastCriticalArrival, seq);
                lastCritical = Math.max(lastCritical, seq);
                String trip = strAfter(f, "\"tripId\":\"");
                Integer tripIdx = trip == null ? null : TRIP_INDEX.get(trip);
                // text field is "s<sentAtMs>n<n>" in message (SOS) or reason (STATUS)
                int marker = f.indexOf("\"message\":\"s");
                int skip = 12;
                if (marker < 0) {
                    marker = f.indexOf("\"reason\":\"s");
                    skip = 11;
                }
                if (marker >= 0 && tripIdx != null) {
                    int nPos = f.indexOf('n', marker + skip);
                    long sentAt = Long.parseLong(f.substring(marker + skip, nPos));
                    int end = nPos + 1;
                    while (end < f.length() && Character.isDigit(f.charAt(end))) {
                        end++;
                    }
                    long n = Long.parseLong(f.substring(nPos + 1, end));
                    received.add(tripIdx * 1_000_000L + n);
                    if ("SOS".equals(type)) {
                        long latency = System.currentTimeMillis() - sentAt;
                        SOS_LATENCY_ALL.add(latency);
                        if (desk) {
                            SOS_LATENCY_DESK.add(latency);
                        }
                    }
                }
            } else if (f.startsWith("{\"type\":\"ACK\"")) {
                if (baseline < 0 && lastSent == 0) {
                    baseline = longAfter(f, "\"seq\":");
                }
            } else if (f.startsWith("{\"type\":\"ERROR\"")) {
                ERROR_FRAMES.incrementAndGet();
                if (f.contains("\"code\":503") && !STOPPING.get()) {
                    // reference client rule: 503 means "try again", with backoff, using the same resume point
                    WebSocket socket = ws;
                    long delay = 300 + ThreadLocalRandom.current().nextLong(1200);
                    TIMER.schedule(() -> {
                        if (connected && socket == ws && !STOPPING.get()) {
                            socket.sendText("{\"action\":\"RESUME\",\"topic\":\"" + topic + "\",\"lastSeq\":" + lastSent + "}", true);
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                }
            }
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            lost();
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            lost();
        }

        private void lost() {
            synchronized (this) {
                if (connected) {
                    connected = false;
                    CONNECTED.decrementAndGet();
                }
            }
            scheduleReconnect();
        }
    }

    // ------------------------------------------------------------------ http helpers
    static HttpResponse<String> post(String path, String token, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) {
            b.header("Authorization", "Bearer " + token);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    static String login(String userId) throws Exception {
        HttpResponse<String> r = post("/api/v1/auth/login", null, "{\"username\":\"" + userId + "\",\"password\":\"password123\"}");
        if (r.statusCode() != 200) {
            throw new IllegalStateException("login failed: " + r.statusCode() + " (is the stack up and seeded?)");
        }
        return strAfter(r.body(), "\"token\":\"");
    }

    /** One event with retries on 5xx / network errors (idempotent thanks to eventId). */
    static void publish(String path, int cab, String body, String kind) {
        long start = System.nanoTime();
        for (int attempt = 1; attempt <= 6; attempt++) {
            POSTS.incrementAndGet();
            long attemptStart = System.nanoTime();
            try {
                HttpResponse<String> r = post("/api/v1/events/" + path, CAB_TOKEN[cab], body);
                int ms = (int) Math.min(10_000, (System.nanoTime() - attemptStart) / 1_000_000);
                POST_MS.incrementAndGet(ms);
                if (r.statusCode() == 202) {
                    return;
                }
                if (r.statusCode() < 500) {
                    POST_FAIL.incrementAndGet();
                    System.err.println("POST " + path + " rejected with " + r.statusCode());
                    return;
                }
            } catch (Exception e) {
                // fall through to retry
            }
            POST_RETRIES.incrementAndGet();
            try {
                Thread.sleep(200L * attempt);
            } catch (InterruptedException ie) {
                return;
            }
        }
        POST_FAIL.incrementAndGet();
    }

    static String event(String type, int cab, String eventId, String payload) {
        return "{\"eventType\":\"" + type + "\",\"eventId\":\"" + eventId + "\",\"cabId\":\"" + CAB_ID[cab] + "\",\"tripId\":\"" + TRIP_ID[cab]
                + "\",\"timestamp\":" + System.currentTimeMillis() + ",\"payload\":" + payload + "}";
    }

    /** Critical event (STATUS or SOS) with a per-trip counter; the counter is only "acked" after a 202. */
    static void criticalEvent(boolean sos, int cab) {
        int n = NEXT_N.incrementAndGet(cab);
        String id = (sos ? "sos-" : "st-") + RUN + "-" + cab + "-" + n;
        String text = "s" + System.currentTimeMillis() + "n" + n;
        String payload = sos ? "{\"level\":\"HIGH\",\"message\":\"" + text + "\",\"lat\":12.9,\"lng\":77.5}"
                : "{\"status\":\"ACTIVE\",\"reason\":\"" + text + "\"}";
        long failBefore = POST_FAIL.get();
        publish(sos ? "sos" : "status", cab, event(sos ? "SOS" : "STATUS", cab, id, payload), sos ? "SOS" : "STATUS");
        if (POST_FAIL.get() == failBefore) {
            ACKED.add(cab * 1_000_000L + n);
            (sos ? SOS_OK : STATUS_OK).incrementAndGet();
        }
    }

    // ------------------------------------------------------------------ main
    public static void main(String[] args) throws Exception {
        for (String a : args) {
            if (a.startsWith("--") && a.contains("=")) {
                ARGS.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
            }
        }
        base = ARGS.getOrDefault("base", "http://localhost:8088");
        wsBase = base.replaceFirst("^http", "ws");
        connections = intArg("connections", 10_000);
        cabs = intArg("cabs", 2_000);
        desks = intArg("desks", 50);
        durationSec = intArg("duration", 90);
        killAtSec = intArg("kill-at", 40);
        locIntervalMs = intArg("loc-interval-ms", 2_000);
        sosIntervalMs = intArg("sos-interval-ms", 1_000);
        statusIntervalSec = intArg("status-interval-sec", 15);
        settleSec = intArg("settle", 20);
        killNode = ARGS.get("kill-node");
        String[] metricsUrls = ARGS.getOrDefault("metrics", "http://localhost:8091,http://localhost:8092,http://localhost:8093").split(",");

        for (int i = 0; i < WS_CLIENTS.length; i++) {
            WS_CLIENTS[i] = HttpClient.newBuilder().executor(Executors.newVirtualThreadPerTaskExecutor())
                    .connectTimeout(Duration.ofSeconds(20)).build();
        }
        http = HttpClient.newBuilder().executor(Executors.newVirtualThreadPerTaskExecutor()).version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10)).build();

        System.out.printf("config: base=%s connections=%d desks=%d cabs=%d duration=%ds kill=%s@%ds loc=%dms sos=%dms%n",
                base, connections, desks, cabs, durationSec, killNode, killAtSec, locIntervalMs, sosIntervalMs);

        // ---- tokens
        String adminToken = login(ADMIN);
        operatorToken = login(OPERATOR);
        for (int i = 0; i < cabs; i++) {
            CAB_ID[i] = UUID.nameUUIDFromBytes(("load-cab-" + i).getBytes(StandardCharsets.UTF_8)).toString();
            TRIP_ID[i] = UUID.nameUUIDFromBytes(("load-trip-" + i).getBytes(StandardCharsets.UTF_8)).toString();
            TRIP_INDEX.put(TRIP_ID[i], i);
        }
        Semaphore mintPermits = new Semaphore(64);
        List<CompletableFuture<Void>> minting = new ArrayList<>();
        for (int i = 0; i < cabs; i++) {
            final int cab = i;
            mintPermits.acquire();
            minting.add(CompletableFuture.runAsync(() -> {
                try {
                    HttpResponse<String> r = null;
                    for (int attempt = 1; attempt <= 4 && (r == null || r.statusCode() != 200); attempt++) {
                        try {
                            r = post("/api/v1/auth/cab-token", adminToken, "{\"cabId\":\"" + CAB_ID[cab] + "\"}");
                        } catch (Exception e) {
                            r = null;
                            Thread.sleep(300L * attempt);
                        }
                    }
                    if (r == null || r.statusCode() != 200) {
                        if (r == null) {
                            throw new IllegalStateException("cab-token request failed repeatedly for cab " + cab);
                        }
                        throw new IllegalStateException("cab-token " + r.statusCode() + " for cab " + cab + " body=[" + r.body() + "] headers=" + r.headers().map() + " (is the stack seeded with SEED_LOAD_CABS?)");
                    }
                    CAB_TOKEN[cab] = strAfter(r.body(), "\"token\":\"");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    mintPermits.release();
                }
            }, Executors.newVirtualThreadPerTaskExecutor()));
        }
        CompletableFuture.allOf(minting.toArray(new CompletableFuture[0])).get();
        System.out.println("minted " + cabs + " cab tokens");

        // ---- consumers
        Semaphore connectPermits = new Semaphore(200);
        long rampStart = System.currentTimeMillis();
        for (int i = 0; i < connections; i++) {
            Consumer c = new Consumer(i, i < desks, i < desks ? -1 : (i % cabs));
            CONSUMERS.add(c);
            connectPermits.acquire();
            c.connect();
            TIMER.schedule(() -> connectPermits.release(), 40, TimeUnit.MILLISECONDS);   // paces the ramp to ~5k connects/s at most
        }
        while (CONNECTED.get() < connections * 0.99 && System.currentTimeMillis() - rampStart < 180_000) {
            Thread.sleep(500);
            System.out.printf("  connected %d/%d%n", CONNECTED.get(), connections);
        }
        System.out.printf("ramp done in %.1fs: %d connected, %d connect failures%n",
                (System.currentTimeMillis() - rampStart) / 1000.0, CONNECTED.get(), CONNECT_FAIL.get());
        Thread.sleep(3000);                                                      // let SUBSCRIBE/ACK finish everywhere

        // ---- publishing
        long startMs = System.currentTimeMillis();
        System.out.println("PUBLISHING_STARTED");
        Random rnd = new Random(42);
        for (int i = 0; i < cabs; i++) {
            final int cab = i;
            TIMER.scheduleAtFixedRate(() -> {
                if (STOPPING.get()) {
                    return;
                }
                Executors.newVirtualThreadPerTaskExecutor().execute(() -> {
                    publish("location", cab, event("LOCATION", cab, "loc-" + cab + "-" + System.nanoTime(),
                            "{\"lat\":12.97,\"lng\":77.59,\"speed\":40}"), "LOCATION");
                    LOC_OK.incrementAndGet();
                });
            }, rnd.nextInt(locIntervalMs), locIntervalMs, TimeUnit.MILLISECONDS);
            TIMER.scheduleAtFixedRate(() -> {
                if (!STOPPING.get()) {
                    Executors.newVirtualThreadPerTaskExecutor().execute(() -> criticalEvent(false, cab));
                }
            }, rnd.nextInt(statusIntervalSec * 1000), statusIntervalSec * 1000L, TimeUnit.MILLISECONDS);
        }
        TIMER.scheduleAtFixedRate(() -> {
            if (!STOPPING.get()) {
                int cab = ThreadLocalRandom.current().nextInt(cabs);
                Executors.newVirtualThreadPerTaskExecutor().execute(() -> criticalEvent(true, cab));
            }
        }, 0, sosIntervalMs, TimeUnit.MILLISECONDS);

        // ---- failure injection and recovery measurement
        long killedAtMs = -1;
        int connectedBeforeKill = -1;
        long recoveredAfterMs = -1;
        int steadyConnected = 0;
        int minConnected = Integer.MAX_VALUE;
        while ((System.currentTimeMillis() - startMs) / 1000 < durationSec) {
            Thread.sleep(250);
            long elapsed = System.currentTimeMillis() - startMs;
            if (killNode != null && killedAtMs < 0 && elapsed >= killAtSec * 1000L) {
                connectedBeforeKill = CONNECTED.get();
                System.out.printf("[%ds] KILLING %s (connected before: %d)%n", elapsed / 1000, killNode, connectedBeforeKill);
                killedAtMs = System.currentTimeMillis();
                new ProcessBuilder("docker", "kill", killNode).inheritIO().start().waitFor();
                killNode = killNode + " (killed by simulator)";
            }
            if (killedAtMs < 0 && killNode == null && elapsed > 5000) {
                // external kill (docker kill from the host): detect it as a sudden drop of connected consumers
                int now = CONNECTED.get();
                if (steadyConnected > 0 && now < steadyConnected * 0.95) {
                    killedAtMs = System.currentTimeMillis() - 250;
                    connectedBeforeKill = steadyConnected;
                    killNode = "(external)";
                    System.out.printf("[%ds] node loss detected: connected dropped %d -> %d%n", elapsed / 1000, steadyConnected, now);
                } else {
                    steadyConnected = Math.max(steadyConnected, now);
                }
            }
            if (killedAtMs > 0) {
                minConnected = Math.min(minConnected, CONNECTED.get());
                if (recoveredAfterMs < 0 && System.currentTimeMillis() - killedAtMs > 1000 && CONNECTED.get() >= connectedBeforeKill * 0.999) {
                    recoveredAfterMs = System.currentTimeMillis() - killedAtMs;
                    System.out.printf("[%ds] all consumers reconnected %.1fs after the kill%n", elapsed / 1000, recoveredAfterMs / 1000.0);
                }
            }
            if (elapsed % 10_000 < 250) {
                System.out.printf("[%ds] connected=%d posts=%d retries=%d fail=%d acked-critical=%d sos-samples=%d%n", elapsed / 1000,
                        CONNECTED.get(), POSTS.get(), POST_RETRIES.get(), POST_FAIL.get(), ACKED.size(), SOS_LATENCY_ALL.size());
            }
        }
        STOPPING.set(true);
        long publishSeconds = (System.currentTimeMillis() - startMs) / 1000;
        System.out.println("publishing stopped; settling " + settleSec + "s ...");
        Thread.sleep(settleSec * 1000L);
        // reconnecting consumers need STOPPING false to finish resuming; give stragglers a final chance
        STOPPING.set(false);
        Thread.sleep(8000);
        STOPPING.set(true);

        // ---- verification
        long expectedTotal = 0, missingTotal = 0, duplicates = 0, orderViolations = 0;
        int consumersWithLoss = 0, notConnectedAtEnd = 0;
        for (Consumer c : CONSUMERS) {
            if (!c.connected) {
                notConnectedAtEnd++;
            }
            long expected = 0, missing = 0;
            synchronized (c) {
                for (long key : ACKED) {
                    if (c.desk || key / 1_000_000L == c.tripIdx) {
                        expected++;
                        if (!c.received.contains(key)) {
                            missing++;
                        }
                    }
                }
                duplicates += c.duplicates;
                orderViolations += c.orderViolations;
            }
            expectedTotal += expected;
            missingTotal += missing;
            if (missing > 0) {
                consumersWithLoss++;
            }
        }

        // ---- report
        System.out.println();
        System.out.println("==================== RESULT ====================");
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        System.out.printf("machine: %d logical cores, %.1f GB RAM, %s %s, JDK %s%n", Runtime.getRuntime().availableProcessors(),
                os.getTotalMemorySize() / 1e9, System.getProperty("os.name"), System.getProperty("os.version"), System.getProperty("java.version"));
        System.out.printf("topology: nginx -> 3 gateway nodes (512 MB heap each) -> redis + postgres, all in Docker; simulator on the same host%n");
        System.out.printf("load: %d connections (%d desks on the office topic, %d trip watchers), %d cabs, %ds of publishing%n",
                connections, desks, connections - desks, cabs, publishSeconds);
        System.out.printf("published: %d locations, %d STATUS, %d SOS acknowledged; %d POST attempts (%.0f/s), %d retries, %d permanently failed%n",
                LOC_OK.get(), STATUS_OK.get(), SOS_OK.get(), POSTS.get(), POSTS.get() / (double) Math.max(1, publishSeconds), POST_RETRIES.get(), POST_FAIL.get());
        System.out.printf("ingest POST latency: p50=%dms p90=%dms p99=%dms p99.9=%dms%n", postPercentile(0.50), postPercentile(0.90), postPercentile(0.99), postPercentile(0.999));
        printSos("SOS delivery latency, all receivers", SOS_LATENCY_ALL);
        printSos("SOS delivery latency, security desks only", SOS_LATENCY_DESK);
        System.out.printf("critical delivery check: %d consumer*event pairs expected, %d missing in %d consumers, %d duplicates seen by the client, %d order violations%n",
                expectedTotal, missingTotal, consumersWithLoss, duplicates, orderViolations);
        System.out.printf("connections: %d reconnects, %d connect failures, %d error frames, %d not connected at the end%n",
                RECONNECTS.get(), CONNECT_FAIL.get(), ERROR_FRAMES.get(), notConnectedAtEnd);
        if (killedAtMs > 0) {
            System.out.printf("node kill (%s): connections dropped from %d to %d; all back after %s%n", killNode, connectedBeforeKill, minConnected,
                    recoveredAfterMs < 0 ? "NOT RECOVERED" : String.format("%.1fs", recoveredAfterMs / 1000.0));
        }
        System.out.println("-------- server-side metrics (surviving nodes) --------");
        for (String metricsUrl : metricsUrls) {
            try {
                HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(metricsUrl + "/actuator/prometheus"))
                        .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
                System.out.println("node " + metricsUrl);
                for (String line : r.body().split("\n")) {
                    if (line.startsWith("sos_delivery_latency_seconds_count") || line.startsWith("sos_delivery_latency_seconds_bucket{") && (line.contains("le=\"1.0\"") || line.contains("le=\"2.0\"") || line.contains("le=\"+Inf\""))
                            || line.startsWith("ws_connections") || line.startsWith("events_dropped_total") || line.startsWith("pubsub_gap_repairs_total")
                            || line.startsWith("pubsub_resyncs_total") || line.startsWith("pubsub_messages_dropped_total") || line.startsWith("outbox_forced_resync_total")
                            || line.startsWith("redis_errors_total") && !line.endsWith(" 0.0") || line.startsWith("ws_disconnects_total") && !line.endsWith(" 0.0")
                            || line.startsWith("pubsub_lag_seconds_count") || line.startsWith("events_conflated_total")) {
                        System.out.println("  " + line);
                    }
                }
            } catch (IOException e) {
                System.out.println("node " + metricsUrl + " not reachable (killed?)");
            }
        }
        boolean pass = missingTotal == 0 && orderViolations == 0 && percentile(SOS_LATENCY_ALL, 0.99) < 2000 && POST_FAIL.get() == 0;
        System.out.println(pass ? "VERDICT: PASS (no STATUS/SOS lost, in order, SOS p99 < 2 s)" : "VERDICT: FAIL (see numbers above)");
        System.exit(pass ? 0 : 1);
    }

    static long postPercentile(double p) {
        long total = 0;
        for (int i = 0; i < POST_MS.length(); i++) {
            total += POST_MS.get(i);
        }
        long target = (long) Math.ceil(total * p), seen = 0;
        for (int i = 0; i < POST_MS.length(); i++) {
            seen += POST_MS.get(i);
            if (seen >= target) {
                return i;
            }
        }
        return POST_MS.length();
    }

    static long percentile(List<Long> values, double p) {
        List<Long> copy;
        synchronized (values) {
            copy = new ArrayList<>(values);
        }
        if (copy.isEmpty()) {
            return Long.MAX_VALUE;
        }
        Collections.sort(copy);
        return copy.get(Math.min(copy.size() - 1, (int) Math.ceil(copy.size() * p) - 1));
    }

    static void printSos(String label, List<Long> values) {
        if (values.isEmpty()) {
            System.out.println(label + ": no samples");
            return;
        }
        System.out.printf("%s: n=%d p50=%dms p90=%dms p99=%dms max=%dms%n", label, values.size(), percentile(values, 0.5), percentile(values, 0.90), percentile(values, 0.99), percentile(values, 1.0));
    }
}
