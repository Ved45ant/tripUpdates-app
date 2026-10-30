package com.example.tripUpdates.websocket;

import com.example.tripUpdates.security.JwtTokenProvider;
import io.jsonwebtoken.Claims;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.util.Map;

/**
 * WebSocket handshake interceptor for JWT authentication.
 * Validates JWT token before WebSocket upgrade.
 * 
 * PROCESS:
 * 1. Extract JWT from query parameter: ?token=...
 * 2. Verify JWT signature with secret key
 * 3. Extract claims: userId, tenantId, role, officeId
 * 4. Store claims in WebSocketSession attributes
 * 5. Allow/reject handshake based on validation
 * 
 * JWT STRUCTURE:
 * {
 *   "userId": "user-uuid-123",
 *   "tenantId": "tenant-uuid-456",
 *   "role": "EMPLOYEE" | "ADMIN" | "SECURITY_OPERATOR",
 *   "officeId": "office-uuid-789" (optional)
 * }
 * 
 * SECURITY:
 * - Rejects connection if token missing
 * - Rejects connection if token invalid/expired
 * - Logs rejection reason (but does not expose to client)
 * - Client receives 1008 Policy Violation close code
 * 
 * ATTRIBUTES STORED:
 * - "userId" → String
 * - "tenantId" → String
 * - "role" → String
 * - "officeId" → String (nullable)
 * 
 * Used by TripWebSocketHandler for authorization checks.
 */
@Component
public class WsAuthHandshakeInterceptor implements HandshakeInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(WsAuthHandshakeInterceptor.class);

    private final JwtTokenProvider jwtTokenProvider;

    private final com.example.tripUpdates.telemetry.MetricsService metrics;

    public WsAuthHandshakeInterceptor(JwtTokenProvider jwtTokenProvider) {
        this(jwtTokenProvider, null);
    }

    private boolean reject() {
        if (metrics != null) {
            metrics.recordAuthRejection("handshake");
        }
        return false;
    }

    @org.springframework.beans.factory.annotation.Autowired
    public WsAuthHandshakeInterceptor(JwtTokenProvider jwtTokenProvider,
                                      com.example.tripUpdates.telemetry.MetricsService metrics) {
        this.metrics = metrics;
        this.jwtTokenProvider = jwtTokenProvider;
    }

    /**
     * Validates JWT token before WebSocket handshake.
     * Called before upgrading HTTP connection to WebSocket.
     * 
     * @param request HTTP request
     * @param response HTTP response
     * @param wsHandler WebSocket handler
     * @param attributes Session attributes (output)
     * @return true if handshake allowed, false to reject
     */
    @Override
    public boolean beforeHandshake(ServerHttpRequest request,
                                   ServerHttpResponse response,
                                   WebSocketHandler wsHandler,
                                   Map<String, Object> attributes) throws Exception {

        // Extract token from query parameter
        String token = extractToken(request);
        if (token == null || token.isEmpty()) {
            logger.warn("WebSocket handshake rejected: Missing token");
            return reject();
        }

        try {
            // Validate and extract claims
            Claims claims = jwtTokenProvider.validateAndGetClaims(token);

            // Extract required claims
            String userId = claims.get("userId", String.class);
            String tenantId = claims.get("tenantId", String.class);
            String role = claims.get("role", String.class);
            String officeId = claims.get("officeId", String.class); // Optional

            // Validate required fields
            boolean userRole = role != null && java.util.Arrays.stream(com.example.tripUpdates.model.enums.UserRole.values())
                    .anyMatch(r -> r.name().equals(role));
            if (userId == null || tenantId == null || !userRole) { // cab-device tokens cannot consume
                logger.warn("WebSocket handshake rejected: Missing required claims (userId, tenantId, or role)");
                return reject();
            }

            // Store claims in WebSocketSession attributes
            attributes.put("userId", userId);
            attributes.put("tenantId", tenantId);
            attributes.put("role", role);
            if (officeId != null) {
                attributes.put("officeId", officeId);
            }
            if (claims.getExpiration() != null) {
                attributes.put("expiresAtMs", claims.getExpiration().getTime());
            }

            logger.info("WebSocket handshake accepted: userId={}, tenantId={}, role={}",
                    userId, tenantId, role);
            return true;

        } catch (Exception e) {
            logger.warn("WebSocket handshake rejected: Invalid JWT - {}", e.getMessage());
            return reject();
        }
    }

    /**
     * Called after WebSocket handshake (successful or failed).
     * No-op for this implementation.
     */
    @Override
    public void afterHandshake(ServerHttpRequest request,
                               ServerHttpResponse response,
                               WebSocketHandler wsHandler,
                               Exception exception) {
        // No-op
    }

    /**
     * Extracts JWT token from query parameter.
     * Expected format: /ws/updates?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
     * 
     * @param request HTTP request
     * @return Token string, or null if not found
     */
    private String extractToken(ServerHttpRequest request) {
        if (request instanceof ServletServerHttpRequest) {
            ServletServerHttpRequest servletRequest = (ServletServerHttpRequest) request;
            return servletRequest.getServletRequest().getParameter("token");
        }
        return null;
    }
}
