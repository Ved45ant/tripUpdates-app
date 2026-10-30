package com.example.tripUpdates.exception;

/**
 * Exception thrown when a user attempts to access a topic they are not authorized for.
 * Used to trigger 403 Forbidden response from GlobalExceptionHandler.
 */
public class UnauthorizedSubscriptionException extends RuntimeException {

    public UnauthorizedSubscriptionException(String message) {
        super(message);
    }

    public UnauthorizedSubscriptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
