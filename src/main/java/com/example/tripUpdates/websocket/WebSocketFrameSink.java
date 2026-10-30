package com.example.tripUpdates.websocket;

import com.example.tripUpdates.service.core.FrameSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

/** {@link FrameSink} backed by a Spring {@link WebSocketSession}. */
public class WebSocketFrameSink implements FrameSink {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketFrameSink.class);

    private final WebSocketSession session;

    public WebSocketFrameSink(WebSocketSession session) {
        this.session = session;
    }

    @Override
    public void send(String text) throws IOException {
        // Callers serialise writes per connection (ClientConnectionSession#send).
        session.sendMessage(new TextMessage(text));
    }

    @Override
    public boolean isOpen() {
        return session.isOpen();
    }

    @Override
    public void closeUnauthorized(String reason) {
        try {
            session.close(CloseStatus.POLICY_VIOLATION);
        } catch (IOException | RuntimeException e) {
            logger.debug("Error closing session {} ({}): {}", session.getId(), reason, e.getClass().getSimpleName());
        }
    }

    @Override
    public void close(String reason) {
        try {
            session.close(new CloseStatus(1013, "try again later")); // client should reconnect and resume
        } catch (IOException | RuntimeException e) {
            logger.debug("Error closing session {} ({}): {}", session.getId(), reason, e.getClass().getSimpleName());
        }
    }
}
