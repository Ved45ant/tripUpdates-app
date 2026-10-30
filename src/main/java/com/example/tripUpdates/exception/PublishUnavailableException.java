package com.example.tripUpdates.exception;

/** The event could not be handed to the delivery layer right now; the caller should retry (idempotent). */
public class PublishUnavailableException extends RuntimeException {
    public PublishUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
