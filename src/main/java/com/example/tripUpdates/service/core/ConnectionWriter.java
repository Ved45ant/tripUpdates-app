package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.enums.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Drives delivery from each connection's outbox to its socket.
 *
 * Isolation model: there is no shared bounded worker pool. A connection is drained by at most one
 * task at a time (guarded by {@code writerScheduled}) that runs on the supplied executor, which in
 * production is a virtual-thread-per-task executor. A client that stops reading therefore blocks
 * only its own task; everyone else keeps flowing. A watchdog force-closes connections whose
 * write has been stuck longer than the send timeout, which unblocks and frees the task.
 *
 * Priority: between every socket write the writer checks the critical lane (SOS and STATUS), so they never wait
 * behind a location backlog that was already drained, only behind the single write in progress.
 */
public class ConnectionWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionWriter.class);

    private final Executor executor;
    private final DeliveryObserver observer;
    private final long sendTimeoutNanos;
    private final Set<ClientConnectionSession> inFlight = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService watchdog;

    public ConnectionWriter(Executor executor, DeliveryObserver observer, long sendTimeoutMillis) {
        this.executor = executor;
        this.observer = observer == null ? DeliveryObserver.NOOP : observer;
        this.sendTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(sendTimeoutMillis);
        this.watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "connection-writer-watchdog");
            t.setDaemon(true);
            return t;
        });
        long period = Math.max(10, Math.min(250, sendTimeoutMillis / 4));
        watchdog.scheduleWithFixedDelay(this::sweep, period, period, TimeUnit.MILLISECONDS);
    }

    /** Ensures the session's outbox gets drained. Cheap and non-blocking; safe from any thread. */
    public void wake(ClientConnectionSession session) {
        if (session.writerScheduled.compareAndSet(false, true)) {
            try {
                executor.execute(() -> run(session));
            } catch (RuntimeException e) {
                session.writerScheduled.set(false);
                throw e;
            }
        }
    }

    /** Force-closes the connection; the client is expected to reconnect and resume. */
    public void disconnect(ClientConnectionSession session, String reason) {
        if (!session.markClosed()) {
            return;
        }
        observer.disconnected(reason);
        logger.warn("Disconnecting session {} reason={}", session.getSessionId(), reason);
        Thread stuck = session.sendingThread;
        try {
            executor.execute(() -> session.sink().close(reason));
        } catch (RuntimeException e) {
            session.sink().close(reason);
        }
        if (stuck != null) {
            stuck.interrupt();
        }
    }

    private void run(ClientConnectionSession session) {
        ConnectionOutbox outbox = session.getOutbox();
        try {
            while (session.isOpen()) {
                List<EventEnvelope> batch = outbox.drain();
                if (batch.isEmpty()) {
                    break;
                }
                observer.outboxDepth(batch.size());
                for (EventEnvelope event : batch) {
                    // Newly arrived critical events (later than everything in this batch) go out before the
                    // batch's locations, never before the batch's own, older critical events.
                    if (event.type() == EventType.LOCATION && !sendPendingCritical(session)) {
                        return;
                    }
                    if (!deliver(session, event)) {
                        return;
                    }
                }
                if (!sendPendingCritical(session)) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            logger.error("Writer failure for session {}", session.getSessionId(), e);
            disconnect(session, "writer-error");
        } finally {
            session.writerScheduled.set(false);
            if (session.isOpen() && !outbox.isEmpty()) {
                wake(session); // something arrived between the last drain and the flag reset
            }
        }
    }

    private boolean sendPendingCritical(ClientConnectionSession session) {
        for (EventEnvelope critical : session.getOutbox().drainCritical()) {
            if (!deliver(session, critical)) {
                return false;
            }
        }
        return true;
    }

    /** @return false if the connection is (now) closed and the writer must stop. */
    private boolean deliver(ClientConnectionSession session, EventEnvelope event) {
        if (!session.acceptForDelivery(event)) {
            return session.isOpen(); // duplicate or no longer subscribed
        }
        session.sendingThread = Thread.currentThread();
        session.sendStartedNanos = System.nanoTime();
        inFlight.add(session);
        boolean ok;
        try {
            ok = session.send(EventFrames.event(event));
        } finally {
            inFlight.remove(session);
            session.sendStartedNanos = 0L;
            session.sendingThread = null;
            Thread.interrupted(); // clear a watchdog interrupt that raced with a completed send
        }
        if (!ok) {
            disconnect(session, "send-failed");
            return false;
        }
        if (event.type() == EventType.SOS && event.ingestTs() > 0) {
            observer.sosDelivered(Math.max(0, System.currentTimeMillis() - event.ingestTs()));
        }
        return true;
    }

    private void sweep() {
        long now = System.nanoTime();
        for (ClientConnectionSession session : inFlight) {
            long started = session.sendStartedNanos;
            if (started != 0L && now - started > sendTimeoutNanos) {
                disconnect(session, "send-timeout");
            }
        }
    }

    public int inFlightCount() {
        return inFlight.size();
    }

    @Override
    public void close() {
        watchdog.shutdownNow();
    }
}
