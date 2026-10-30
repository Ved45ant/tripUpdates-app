package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.enums.TripStatus;

import java.util.UUID;

/** Authoritative current status of a trip (PostgreSQL). Status changes are rare, so this is a synchronous write. */
public interface TripStatusStore {

    enum TransitionResult {
        OK, NOT_FOUND, INVALID
    }

    /** Atomically validates {@link TripStateMachine} rules against the stored status and applies the change. */
    TransitionResult transition(UUID tripId, TripStatus to);
}
