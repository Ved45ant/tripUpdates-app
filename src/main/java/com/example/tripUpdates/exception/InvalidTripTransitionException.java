package com.example.tripUpdates.exception;

/** A STATUS event would move the trip to a state that is not reachable from its current one. */
public class InvalidTripTransitionException extends RuntimeException {
    public InvalidTripTransitionException(String message) {
        super(message);
    }
}
