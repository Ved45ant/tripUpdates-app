package com.example.tripUpdates.service.core;

import java.io.IOException;

/**
 * Minimal view of a client socket as seen by the core engine. Keeps the engine free of any
 * WebSocket library so isolation behaviour can be tested with fake sinks.
 *
 * Implementations may block in {@link #send(String)} (slow client). {@link #close(String)} must
 * be safe to call from another thread while a send is in progress and should unblock it.
 */
public interface FrameSink {

    /** Writes one text frame; may block while the client is slow. */
    void send(String text) throws IOException;

    boolean isOpen();

    /** Closes the connection asking the client to reconnect/resume. Must not throw. */
    void close(String reason);

    /** Closes the connection because its credentials are no longer valid (client must re-authenticate). */
    default void closeUnauthorized(String reason) {
        close(reason);
    }
}
