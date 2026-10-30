package com.example.tripUpdates.config;

import com.example.tripUpdates.websocket.TripWebSocketHandler;
import com.example.tripUpdates.websocket.WsAuthHandshakeInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * WebSocket configuration for the TripUpdates system.
 * Registers WebSocket endpoints and handshake interceptors.
 * 
 * ENDPOINT:
 * - URI: wss://api.domain.com/ws/updates?token={JWT}
 * - Handler: TripWebSocketHandler
 * - Interceptor: WsAuthHandshakeInterceptor (JWT validation)
 * 
 * SECURITY:
 * - JWT token required in query parameter
 * - Token validated before WebSocket upgrade
 * - Invalid token → Connection rejected with 1008 Policy Violation
 * 
 * USAGE:
 * Client connects:
 *   wss://api.domain.com/ws/updates?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 * 
 * Interceptor validates JWT and extracts:
 *   - userId
 *   - tenantId
 *   - role
 *   - officeId (optional)
 * 
 * Handler manages:
 *   - SUBSCRIBE/UNSUBSCRIBE/RESUME frames
 *   - Session lifecycle
 *   - Authorization checks
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final TripWebSocketHandler tripWebSocketHandler;
    private final WsAuthHandshakeInterceptor wsAuthHandshakeInterceptor;

    public WebSocketConfig(TripWebSocketHandler tripWebSocketHandler,
                           WsAuthHandshakeInterceptor wsAuthHandshakeInterceptor) {
        this.tripWebSocketHandler = tripWebSocketHandler;
        this.wsAuthHandshakeInterceptor = wsAuthHandshakeInterceptor;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(tripWebSocketHandler, "/ws/updates")
                .addInterceptors(wsAuthHandshakeInterceptor)
                .setAllowedOrigins("*"); // Configure based on environment
    }
}
