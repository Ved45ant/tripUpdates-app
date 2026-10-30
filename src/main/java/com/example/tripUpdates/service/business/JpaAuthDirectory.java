package com.example.tripUpdates.service.business;

import com.example.tripUpdates.repository.CabRepository;
import com.example.tripUpdates.repository.OfficeRepository;
import com.example.tripUpdates.repository.TripPassengerRepository;
import com.example.tripUpdates.repository.TripRepository;
import com.example.tripUpdates.repository.UserRepository;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

/** PostgreSQL-backed {@link AuthDirectory}; always accessed through {@link CachedDirectory}. */
@Component
public class JpaAuthDirectory implements AuthDirectory {

    private final TripRepository trips;
    private final CabRepository cabs;
    private final OfficeRepository offices;
    private final TripPassengerRepository passengers;
    private final UserRepository users;

    public JpaAuthDirectory(TripRepository trips, CabRepository cabs, OfficeRepository offices,
                            TripPassengerRepository passengers, UserRepository users) {
        this.trips = trips;
        this.cabs = cabs;
        this.offices = offices;
        this.passengers = passengers;
        this.users = users;
    }

    @Override
    public Optional<TripInfo> findTrip(UUID tripId) {
        return trips.findById(tripId).map(t -> new TripInfo(t.getId(), t.getTenantId(), t.getOfficeId(), t.getCabId()));
    }

    @Override
    public Optional<CabInfo> findCab(UUID cabId) {
        return cabs.findById(cabId).map(c -> new CabInfo(c.getId(), c.getTenantId(), c.getOfficeId()));
    }

    @Override
    public Optional<OfficeInfo> findOffice(UUID officeId) {
        return offices.findById(officeId).map(o -> new OfficeInfo(o.getId(), o.getTenantId()));
    }

    @Override
    public boolean isPassenger(UUID tripId, UUID userId) {
        return passengers.existsByTripIdAndUserId(tripId, userId);
    }

    @Override
    public boolean isUserActive(UUID userId) {
        return users.findById(userId).map(u -> u.isActive()).orElse(false);
    }
}
