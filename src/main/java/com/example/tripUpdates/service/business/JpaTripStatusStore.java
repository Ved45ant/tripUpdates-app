package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.entity.Trip;
import com.example.tripUpdates.model.enums.TripStatus;
import com.example.tripUpdates.repository.TripRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

/** Row-locks the trip so two concurrent status events cannot both pass the transition check. */
@Component
public class JpaTripStatusStore implements TripStatusStore {

    private final TripRepository trips;

    public JpaTripStatusStore(TripRepository trips) {
        this.trips = trips;
    }

    @Override
    @Transactional
    public TransitionResult transition(UUID tripId, TripStatus to) {
        Optional<Trip> found = trips.findByIdForUpdate(tripId);
        if (found.isEmpty()) {
            return TransitionResult.NOT_FOUND;
        }
        Trip trip = found.get();
        if (!TripStateMachine.isAllowed(trip.getStatus(), to)) {
            return TransitionResult.INVALID;
        }
        if (trip.getStatus() != to) {
            trip.setStatus(to);
            trip.setUpdatedAt(LocalDateTime.now());
            trips.save(trip);
        }
        return TransitionResult.OK;
    }
}
