package com.example.tripUpdates.service.business;

import com.example.tripUpdates.service.business.AuthDirectory.CabInfo;
import com.example.tripUpdates.service.business.AuthDirectory.OfficeInfo;
import com.example.tripUpdates.service.business.AuthDirectory.TripInfo;
import com.github.benmanes.caffeine.cache.Ticker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Hot-path cache in front of {@link AuthDirectory}, shared by subscription and ingest authorization.
 *
 * <pre>
 *   what is cached            positive TTL   negative TTL   what can be stale
 *   trip / cab / office rows  60 s           5 s            reassignment of a trip/cab/office
 *   passenger membership      30 s           5 s            a passenger removed from a trip
 *   user active flag          30 s           5 s            a user deactivated
 * </pre>
 *
 * Staleness is therefore bounded by the TTL, never by "until restart": reads do not extend an
 * entry's life. Already-open subscriptions are bounded by the JWT lifetime (the connection is closed at
 * token expiry), not by these TTLs. Maximum 200k entries per cache; beyond that least-useful entries are evicted.
 */
@Component
public class CachedDirectory {

    private static final long MAX_ENTRIES = 200_000;

    private final AuthDirectory delegate;
    private final TtlCache<UUID, Optional<TripInfo>> trips;
    private final TtlCache<UUID, Optional<CabInfo>> cabs;
    private final TtlCache<UUID, Optional<OfficeInfo>> offices;
    private final TtlCache<String, Boolean> passengers;
    private final TtlCache<UUID, Boolean> activeUsers;

    @Autowired
    public CachedDirectory(AuthDirectory delegate,
                           @Value("${auth.cache.entity-ttl-seconds:60}") long entityTtlSeconds,
                           @Value("${auth.cache.membership-ttl-seconds:30}") long membershipTtlSeconds,
                           @Value("${auth.cache.negative-ttl-seconds:5}") long negativeTtlSeconds) {
        this(delegate, Duration.ofSeconds(entityTtlSeconds), Duration.ofSeconds(membershipTtlSeconds),
                Duration.ofSeconds(negativeTtlSeconds), Ticker.systemTicker());
    }

    public CachedDirectory(AuthDirectory delegate, Duration entityTtl, Duration membershipTtl, Duration negativeTtl,
                           Ticker ticker) {
        this.delegate = delegate;
        this.trips = new TtlCache<>(MAX_ENTRIES, entityTtl, negativeTtl, Optional::isEmpty, ticker);
        this.cabs = new TtlCache<>(MAX_ENTRIES, entityTtl, negativeTtl, Optional::isEmpty, ticker);
        this.offices = new TtlCache<>(MAX_ENTRIES, entityTtl, negativeTtl, Optional::isEmpty, ticker);
        this.passengers = new TtlCache<>(MAX_ENTRIES, membershipTtl, negativeTtl, found -> !found, ticker);
        this.activeUsers = new TtlCache<>(MAX_ENTRIES, membershipTtl, negativeTtl, active -> !active, ticker);
    }

    public Optional<TripInfo> trip(UUID id) {
        return trips.get(id, delegate::findTrip);
    }

    public Optional<CabInfo> cab(UUID id) {
        return cabs.get(id, delegate::findCab);
    }

    public Optional<OfficeInfo> office(UUID id) {
        return offices.get(id, delegate::findOffice);
    }

    public boolean isPassenger(UUID tripId, UUID userId) {
        return passengers.get(tripId + "|" + userId, k -> delegate.isPassenger(tripId, userId));
    }

    public boolean isUserActive(UUID userId) {
        return activeUsers.get(userId, delegate::isUserActive);
    }
}
