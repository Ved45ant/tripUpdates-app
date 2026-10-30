package com.example.tripUpdates.websocket;

import com.example.tripUpdates.model.enums.UserRole;
import com.example.tripUpdates.model.ws.WsAckFrame;
import com.example.tripUpdates.model.ws.WsErrorFrame;
import com.example.tripUpdates.model.ws.WsInboundMessage;
import com.example.tripUpdates.service.business.SubscriptionAuthService;
import com.example.tripUpdates.service.core.ClientConnectionSession;
import com.example.tripUpdates.service.core.ConnectionRegistry;
import com.example.tripUpdates.service.core.EventFanOut;
import com.example.tripUpdates.service.core.RecoveryService;
import com.example.tripUpdates.service.pubsub.PubSubGateway;
import com.example.tripUpdates.telemetry.MetricsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Client protocol: SUBSCRIBE / RESUME / UNSUBSCRIBE.
 *
 * <pre>
 * SUBSCRIBE|RESUME {topic, lastSeq}
 *   1. authorize (every time, including resumes)
 *   2. start buffering live events of the topic on this connection
 *   3. register the subscription and wait until this node is really subscribed in Redis
 *   4. send ACK
 *   5. replay missed STATUS/SOS + latest location, or a SNAPSHOT (RecoveryService)
 *   6. flush the live events that arrived meanwhile, then go live
 * </pre>
 *
 * Step 2-3 before 5 means nothing published during the replay read can be missed; step 6 means the
 * client sees replay first and the per-topic dedup window removes overlap. {@code lastSeq == 0}
 * (new subscription) receives a SNAPSHOT of the latest state followed by live events.
 *
 * Errors are generic and never echo client input. Malformed frames, bad topics and denied topics do
 * not close the connection.
 */
@Component
public class TripWebSocketHandler extends TextWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(TripWebSocketHandler.class);

    static final int MAX_MESSAGE_CHARS = 4096;
    static final int MAX_SUBSCRIPTIONS_PER_CONNECTION = 100;
    static final long SUBSCRIBE_READY_TIMEOUT_SECONDS = 5;
    private static final Pattern TOPIC = Pattern.compile("^(trip|cab|office):[A-Za-z0-9_-]{1,64}$");

    private static final String MSG_FORBIDDEN = "Unauthorized access to requested topic";

    private final ConnectionRegistry connectionRegistry;
    private final SubscriptionAuthService subscriptionAuthService;
    private final RecoveryService recoveryService;
    private final PubSubGateway pubSubGateway;
    private final EventFanOut eventFanOut;
    private final ObjectMapper objectMapper;
    private final MetricsService metricsService;
    private final ScheduledExecutorService expirySweeper;

    public TripWebSocketHandler(ConnectionRegistry connectionRegistry,
                                SubscriptionAuthService subscriptionAuthService,
                                RecoveryService recoveryService,
                                PubSubGateway pubSubGateway,
                                EventFanOut eventFanOut,
                                ObjectMapper objectMapper,
                                MetricsService metricsService) {
        this.connectionRegistry = connectionRegistry;
        this.subscriptionAuthService = subscriptionAuthService;
        this.recoveryService = recoveryService;
        this.pubSubGateway = pubSubGateway;
        this.eventFanOut = eventFanOut;
        this.objectMapper = objectMapper;
        this.metricsService = metricsService;
        this.expirySweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-token-expiry");
            t.setDaemon(true);
            return t;
        });
        this.expirySweeper.scheduleWithFixedDelay(() -> sweepExpired(System.currentTimeMillis()), 15, 15, TimeUnit.SECONDS);
    }

    @PreDestroy
    void shutdown() {
        expirySweeper.shutdownNow();
        // Close every client so it reconnects to another node right away (and resumes from its last sequence).
        connectionRegistry.forEachSession(ClientConnectionSession::closeForShutdown);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        ClientConnectionSession clientSession =
                new ClientConnectionSession(session.getId(), new WebSocketFrameSink(session), objectMapper);
        Object exp = session.getAttributes().get("expiresAtMs");
        if (exp instanceof Long) {
            clientSession.setExpiresAtMillis((Long) exp);
        }
        connectionRegistry.register(clientSession);
        metricsService.incrementActiveConnections();
        logger.info("WebSocket connected: sessionId={}", session.getId());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        if (connectionRegistry.unregister(session.getId()) != null) {
            metricsService.decrementActiveConnections();
        }
        logger.info("WebSocket closed: sessionId={}, code={}", session.getId(), status.getCode());
    }

    /** Closes connections whose token has expired. Runs periodically; public for tests. */
    public void sweepExpired(long nowMillis) {
        connectionRegistry.forEachSession(s -> {
            if (s.isExpired(nowMillis)) {
                s.closeBecauseExpired();
            }
        });
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        MDC.put("sessionId", session.getId());
        try {
            process(session, message);
        } finally {
            MDC.remove("sessionId");
        }
    }

    private void process(WebSocketSession session, TextMessage message) {
        ClientConnectionSession client = connectionRegistry.getSession(session.getId());
        if (client == null) {
            closeQuietly(session, CloseStatus.SERVER_ERROR);
            return;
        }
        if (client.isExpired(System.currentTimeMillis())) {
            client.closeBecauseExpired();
            return;
        }
        String payload = message.getPayload();
        if (payload.length() > MAX_MESSAGE_CHARS) {
            sendError(client, null, 400, "Invalid message");
            return;
        }

        WsInboundMessage inbound;
        try {
            inbound = objectMapper.readValue(payload, WsInboundMessage.class);
        } catch (JsonProcessingException e) {
            sendError(client, null, 400, "Invalid message");
            return;
        }
        String action = inbound.getAction();
        if (action == null) {
            sendError(client, null, 400, "Invalid message");
            return;
        }

        try {
            switch (action) {
                case "SUBSCRIBE":
                case "RESUME":
                    handleSubscribe(session, client, inbound);
                    break;
                case "UNSUBSCRIBE":
                    handleUnsubscribe(client, inbound);
                    break;
                default:
                    sendError(client, null, 400, "Unsupported action");
            }
        } catch (RuntimeException e) {
            logger.error("Error handling {} for sessionId={}: {}", action, session.getId(), e.getClass().getSimpleName());
            sendError(client, null, 500, "Internal server error");
        }
    }

    private void handleSubscribe(WebSocketSession session, ClientConnectionSession client, WsInboundMessage inbound) {
        String topic = inbound.getTopic();
        long lastSeq = inbound.getLastSeq();
        if (lastSeq < 0) {
            sendError(client, null, 400, "Invalid message");
            return;
        }
        if (topic == null || !TOPIC.matcher(topic).matches()) {
            metricsService.recordAuthRejection("subscribe");
            sendError(client, null, 403, MSG_FORBIDDEN); // same answer as "not yours": no enumeration
            return;
        }
        if (!isAuthorized(session, topic)) {
            metricsService.recordAuthRejection("subscribe");
            logger.warn("Subscription denied: sessionId={}, topic={}", session.getId(), topic);
            sendError(client, topic, 403, MSG_FORBIDDEN);
            return;
        }
        if (!client.isSubscribedTo(topic) && client.getSubscriptionCount() >= MAX_SUBSCRIPTIONS_PER_CONNECTION) {
            sendError(client, topic, 429, "Too many subscriptions");
            return;
        }

        client.startBuffering(topic);
        boolean completed = false;
        try {
            if (!connectionRegistry.subscribe(client.getSessionId(), topic)) {
                sendError(client, topic, 500, "Internal server error");
                return;
            }
            pubSubGateway.whenSubscribed(topic).get(SUBSCRIBE_READY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.sendMessage(new WsAckFrame(topic, "SUBSCRIBED", recoveryService.currentSeq(topic)));
            recoveryService.handleResume(client, topic, lastSeq);
            completed = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.warn("Subscribe failed for sessionId={}, topic={}: {}", session.getId(), topic, e.getClass().getSimpleName());
            metricsService.recordRedisError("subscribe");
            sendError(client, topic, 503, "Service temporarily unavailable");
        } finally {
            boolean overflowed = client.finishBuffering(topic, completed ? e -> eventFanOut.deliverReplay(client, e) : null);
            if (!completed) {
                connectionRegistry.unsubscribe(client.getSessionId(), topic);
            } else if (overflowed) {
                logger.warn("Live buffer overflow during subscribe: sessionId={}, topic={}", session.getId(), topic);
                closeQuietly(session, new CloseStatus(1013, "try again later"));
            }
        }
    }

    private void handleUnsubscribe(ClientConnectionSession client, WsInboundMessage inbound) {
        String topic = inbound.getTopic();
        if (topic != null && connectionRegistry.unsubscribe(client.getSessionId(), topic)) {
            client.sendMessage(new WsAckFrame(topic, "UNSUBSCRIBED"));
        } else {
            sendError(client, null, 404, "Subscription not found");
        }
    }

    private boolean isAuthorized(WebSocketSession session, String topic) {
        try {
            String userId = (String) session.getAttributes().get("userId");
            String tenantId = (String) session.getAttributes().get("tenantId");
            String roleStr = (String) session.getAttributes().get("role");
            String officeId = (String) session.getAttributes().get("officeId");
            if (userId == null || tenantId == null || roleStr == null) {
                return false;
            }
            return subscriptionAuthService.isAuthorized(userId, tenantId, UserRole.valueOf(roleStr), officeId, topic);
        } catch (RuntimeException e) {
            return false; // malformed ids/claims are treated exactly like "not allowed"
        }
    }

    private void sendError(ClientConnectionSession client, String topic, int code, String message) {
        client.sendMessage(new WsErrorFrame(topic, code, message));
    }

    private void closeQuietly(WebSocketSession session, CloseStatus status) {
        try {
            session.close(status);
        } catch (IOException | RuntimeException e) {
            logger.debug("Error closing session {}", session.getId());
        }
    }
}
