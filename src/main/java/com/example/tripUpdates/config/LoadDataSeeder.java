package com.example.tripUpdates.config;

import com.example.tripUpdates.model.entity.Cab;
import com.example.tripUpdates.model.entity.Trip;
import com.example.tripUpdates.model.enums.TripStatus;
import com.example.tripUpdates.repository.CabRepository;
import com.example.tripUpdates.repository.TripRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Creates {@code seed.load.cabs} extra cabs, each with one ACTIVE trip, in the demo tenant/office, using ids that
 * the load simulator can compute ({@link UUID#nameUUIDFromBytes} of {@code load-cab-N} / {@code load-trip-N}).
 * Idempotent; a no-op unless the property is set. Not for production.
 */
@Component
@Order(2)
public class LoadDataSeeder implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(LoadDataSeeder.class);
    private static final String TENANT = "12345678-1234-1234-1234-123456789012";
    private static final String OFFICE = "12345678-1234-1234-1234-123456789013";

    private final CabRepository cabs;
    private final TripRepository trips;
    private final int count;

    public LoadDataSeeder(CabRepository cabs, TripRepository trips, @Value("${seed.load.cabs:0}") int count) {
        this.cabs = cabs;
        this.trips = trips;
        this.count = count;
    }

    @Override
    public void run(String... args) {
        if (count <= 0) {
            return;
        }
        UUID lastCab = UUID.nameUUIDFromBytes(("load-cab-" + (count - 1)).getBytes());
        if (cabs.existsById(lastCab)) {
            logger.info("Load data already present ({} cabs)", count);
            return;
        }
        List<Cab> cabBatch = new ArrayList<>();
        List<Trip> tripBatch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Cab cab = new Cab(TENANT, "LOAD" + i);
            cab.setId(UUID.nameUUIDFromBytes(("load-cab-" + i).getBytes()));
            cab.setOfficeId(UUID.fromString(OFFICE));
            cabBatch.add(cab);

            Trip trip = new Trip(TENANT, OFFICE, cab.getId().toString(), TripStatus.ACTIVE);
            trip.setId(UUID.nameUUIDFromBytes(("load-trip-" + i).getBytes()));
            tripBatch.add(trip);

            if (cabBatch.size() == 500 || i == count - 1) {
                cabs.saveAll(cabBatch);
                trips.saveAll(tripBatch);
                cabBatch.clear();
                tripBatch.clear();
            }
        }
        logger.info("Seeded {} load-test cabs and trips", count);
    }
}
