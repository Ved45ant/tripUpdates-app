package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.enums.TripStatus;

/**
 * Allowed trip lifecycle transitions. Staying in the same state is allowed (a retried STATUS event
 * must not be a conflict); COMPLETED and CANCELLED are terminal.
 *
 * <pre>
 *   SCHEDULED -> ACTIVE | CANCELLED
 *   ACTIVE    -> COMPLETED | CANCELLED
 * </pre>
 * A late or out-of-order event that would move a trip backwards is therefore rejected.
 */
public final class TripStateMachine {

    private TripStateMachine() {
    }

    public static boolean isAllowed(TripStatus from, TripStatus to) {
        if (from == null || to == null) {
            return false;
        }
        if (from == to) {
            return true;
        }
        switch (from) {
            case SCHEDULED:
                return to == TripStatus.ACTIVE || to == TripStatus.CANCELLED;
            case ACTIVE:
                return to == TripStatus.COMPLETED || to == TripStatus.CANCELLED;
            default:
                return false;
        }
    }
}
