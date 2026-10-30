package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.enums.UserRole;
import com.example.tripUpdates.service.business.AuthDirectory.CabInfo;
import com.example.tripUpdates.service.business.AuthDirectory.OfficeInfo;
import com.example.tripUpdates.service.business.AuthDirectory.TripInfo;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tenant isolation and role rules. Two tenants, three offices, three trips; every role is tried against
 * its own data, a neighbour's data in the same tenant, and another tenant's data.
 */
public class SubscriptionAuthServiceTest {

    public static UUID id(String name) {
        return UUID.nameUUIDFromBytes(name.getBytes());
    }

    // tenants / offices / cabs / trips
    public static final UUID T1 = id("tenant-1"), T2 = id("tenant-2");
    public static final UUID O1 = id("office-1"), O2 = id("office-2"), O3 = id("office-3");
    public static final UUID C1 = id("cab-1"), C2 = id("cab-2"), C3 = id("cab-3");
    public static final UUID TR1 = id("trip-1"), TR2 = id("trip-2"), TR3 = id("trip-3");
    // users
    public static final UUID EMP_A1 = id("emp-a1"), EMP_A2 = id("emp-a2"), EMP_B = id("emp-b"), EMP_OFF = id("emp-off");
    public static final UUID ADMIN_O1 = id("admin-o1"), ADMIN_O3 = id("admin-o3");
    public static final UUID SEC_T1 = id("sec-t1"), SEC_T2 = id("sec-t2");

    /** In-memory data with call counters. */
    public static class FakeDirectory implements AuthDirectory {
        public final Map<UUID, TripInfo> trips = new HashMap<>();
        public final Map<UUID, CabInfo> cabs = new HashMap<>();
        public final Map<UUID, OfficeInfo> offices = new HashMap<>();
        public final Set<String> passengers = new HashSet<>();
        public final Set<UUID> activeUsers = new HashSet<>();
        public final AtomicInteger tripLookups = new AtomicInteger();
        public final AtomicInteger passengerLookups = new AtomicInteger();
        public volatile boolean failing;

        public Optional<TripInfo> findTrip(UUID id) {
            tripLookups.incrementAndGet();
            check();
            return Optional.ofNullable(trips.get(id));
        }

        public Optional<CabInfo> findCab(UUID id) {
            check();
            return Optional.ofNullable(cabs.get(id));
        }

        public Optional<OfficeInfo> findOffice(UUID id) {
            check();
            return Optional.ofNullable(offices.get(id));
        }

        public boolean isPassenger(UUID tripId, UUID userId) {
            passengerLookups.incrementAndGet();
            check();
            return passengers.contains(tripId + "|" + userId);
        }

        public boolean isUserActive(UUID userId) {
            check();
            return activeUsers.contains(userId);
        }

        private void check() {
            if (failing) {
                throw new IllegalStateException("database unavailable");
            }
        }
    }

    FakeDirectory data;
    AtomicLong clockNanos;
    SubscriptionAuthService auth;

    @BeforeEach
    void setUp() {
        data = new FakeDirectory();
        data.offices.put(O1, new OfficeInfo(O1, T1));
        data.offices.put(O2, new OfficeInfo(O2, T1));
        data.offices.put(O3, new OfficeInfo(O3, T2));
        data.cabs.put(C1, new CabInfo(C1, T1, O1));
        data.cabs.put(C2, new CabInfo(C2, T1, O2));
        data.cabs.put(C3, new CabInfo(C3, T2, O3));
        data.trips.put(TR1, new TripInfo(TR1, T1, O1, C1));
        data.trips.put(TR2, new TripInfo(TR2, T1, O2, C2));
        data.trips.put(TR3, new TripInfo(TR3, T2, O3, C3));
        data.passengers.add(TR1 + "|" + EMP_A1);
        data.passengers.add(TR2 + "|" + EMP_A2);
        data.passengers.add(TR3 + "|" + EMP_B);
        data.passengers.add(TR1 + "|" + EMP_OFF);
        for (UUID u : new UUID[]{EMP_A1, EMP_A2, EMP_B, ADMIN_O1, ADMIN_O3, SEC_T1, SEC_T2}) {
            data.activeUsers.add(u); // EMP_OFF stays inactive
        }
        clockNanos = new AtomicLong();
        Ticker ticker = clockNanos::get;
        auth = new SubscriptionAuthService(new CachedDirectory(data, Duration.ofSeconds(60), Duration.ofSeconds(30),
                Duration.ofSeconds(5), ticker));
    }

    private boolean can(UUID user, UUID tenant, UserRole role, UUID office, String topic) {
        return auth.isAuthorized(user.toString(), tenant.toString(), role, office == null ? null : office.toString(), topic);
    }

    private static String trip(UUID id) {
        return "trip:" + id;
    }

    private static String cab(UUID id) {
        return "cab:" + id;
    }

    private static String office(UUID id) {
        return "office:" + id;
    }

    // ------------------------------------------------------------------------------------ the mandatory test

    @Test
    void anEmployeeCannotSubscribeToAnotherClientsTrip() {
        // EMP_A1 belongs to tenant 1. Trip 3 belongs to tenant 2 (another client).
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR3))).isFalse();
        // even a same-tenant trip they are not a passenger on
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR2))).isFalse();
        // and the other way round
        assertThat(can(EMP_B, T2, UserRole.EMPLOYEE, null, trip(TR1))).isFalse();
        // their own trip works
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).isTrue();
    }

    @Test
    void anEmployeeWhoseTokenClaimsTheWrongTenantStillCannotReachThatTripsData() {
        // forged/mismatching claims: EMP_A1 is a passenger of TR1 (tenant 1) but presents tenant 2
        assertThat(can(EMP_A1, T2, UserRole.EMPLOYEE, null, trip(TR1))).isFalse();
        // passenger of TR3 presenting tenant 1
        assertThat(can(EMP_B, T1, UserRole.EMPLOYEE, null, trip(TR3))).isFalse();
    }

    // ------------------------------------------------------------------------------------ role matrix

    @Test
    void employeeMatrix() {
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).as("own trip").isTrue();
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, cab(C1))).as("cab of own trip").isFalse();
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, office(O1))).as("own office").isFalse();
    }

    @Test
    void adminMatrix() {
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, office(O1))).as("own office").isTrue();
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, office(O2))).as("other office, same tenant").isFalse();
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, office(O3))).as("other tenant's office").isFalse();
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, trip(TR1))).as("trip of own office").isTrue();
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, trip(TR2))).as("trip of other office").isFalse();
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, trip(TR3))).as("other tenant's trip").isFalse();
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, cab(C1))).as("cab of own office").isTrue();
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, cab(C2))).as("cab of other office").isFalse();
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O1, cab(C3))).as("other tenant's cab").isFalse();
        // forged: tenant-1 admin claims office 3 (an office of tenant 2)
        assertThat(can(ADMIN_O1, T1, UserRole.ADMIN, O3, office(O3))).as("office from another tenant").isFalse();
        assertThat(can(ADMIN_O3, T2, UserRole.ADMIN, null, office(O3))).as("admin without office claim").isFalse();
    }

    @Test
    void securityOperatorSeesEverythingInTheirTenantAndNothingOutsideIt() {
        for (String own : new String[]{trip(TR1), trip(TR2), cab(C1), cab(C2), office(O1), office(O2)}) {
            assertThat(can(SEC_T1, T1, UserRole.SECURITY_OPERATOR, null, own)).as(own).isTrue();
        }
        for (String foreign : new String[]{trip(TR3), cab(C3), office(O3)}) {
            assertThat(can(SEC_T1, T1, UserRole.SECURITY_OPERATOR, null, foreign)).as("tenant-1 operator -> " + foreign).isFalse();
        }
        for (String foreign : new String[]{trip(TR1), cab(C1), office(O1)}) {
            assertThat(can(SEC_T2, T2, UserRole.SECURITY_OPERATOR, null, foreign)).as("tenant-2 operator -> " + foreign).isFalse();
        }
    }

    @Test
    void inactiveUsersAreDenied() {
        assertThat(can(EMP_OFF, T1, UserRole.EMPLOYEE, O1, trip(TR1))).isFalse();
    }

    // ------------------------------------------------------------------------------------ garbage in, "no" out

    @Test
    void unknownMalformedAndHostileInputIsDeniedWithoutExceptions() {
        UUID unknown = id("does-not-exist");
        String[] topics = {
                trip(unknown), cab(unknown), office(unknown),               // nonexistent entities
                "trip:", ":", "trip", "", "   ", "trip:not-a-uuid", "trip:123",
                "unknown:" + TR1, "TRIP:" + TR1, "trip:" + TR1 + ":extra",
                "trip:" + TR1 + "\nadmin-granted",                          // log injection attempt
                "trip:" + "a".repeat(10_000), "office:../../" + O1,
        };
        for (String topic : topics) {
            assertThat(can(SEC_T1, T1, UserRole.SECURITY_OPERATOR, null, topic)).as("topic [%s]", topic.length() > 40 ? "long" : topic).isFalse();
        }
        assertThat(auth.isAuthorized(null, T1.toString(), UserRole.EMPLOYEE, null, trip(TR1))).isFalse();
        assertThat(auth.isAuthorized("x", T1.toString(), UserRole.EMPLOYEE, null, trip(TR1))).isFalse();
        assertThat(auth.isAuthorized(EMP_A1.toString(), "nope", UserRole.EMPLOYEE, null, trip(TR1))).isFalse();
        assertThat(auth.isAuthorized(EMP_A1.toString(), T1.toString(), UserRole.EMPLOYEE, "bad-office", trip(TR1))).isFalse();
        assertThat(auth.isAuthorized(EMP_A1.toString(), T1.toString(), null, null, trip(TR1))).isFalse();
        assertThat(auth.isAuthorized(EMP_A1.toString(), T1.toString(), UserRole.EMPLOYEE, null, null)).isFalse();
    }

    @Test
    void unknownAndForeignEntitiesAreIndistinguishableToTheCaller() {
        // both are just "false": nothing in the result tells "does not exist" from "belongs to someone else"
        boolean nonexistent = can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(id("ghost")));
        boolean foreign = can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR3));
        assertThat(nonexistent).isEqualTo(foreign).isFalse();
    }

    @Test
    void backendFailureFailsClosed() {
        data.failing = true;
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).isFalse();
        assertThat(can(SEC_T1, T1, UserRole.SECURITY_OPERATOR, null, trip(TR1))).isFalse();
    }

    // ------------------------------------------------------------------------------------ caching & staleness

    @Test
    void hotPathHitsTheCacheNotTheDatabase() {
        for (int i = 0; i < 1_000; i++) {
            assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).isTrue();
        }
        assertThat(data.tripLookups.get()).as("one trip lookup for 1000 authorizations").isEqualTo(1);
        assertThat(data.passengerLookups.get()).isEqualTo(1);
    }

    @Test
    void aRemovedPassengerLosesAccessWithinTheMembershipTtlEvenIfTheyKeepAsking() {
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).isTrue();   // t=0, loads and caches
        data.passengers.remove(TR1 + "|" + EMP_A1);

        for (int second : new int[]{10, 20, 29}) {                                 // reads must not extend the entry
            clockNanos.set(Duration.ofSeconds(second).toNanos());
            assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).as("t=%ds still cached", second).isTrue();
        }
        clockNanos.set(Duration.ofSeconds(31).toNanos());
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).as("t=31s: staleness bound (30s) reached").isFalse();
    }

    @Test
    void aNewlyAddedPassengerGetsAccessAfterTheShortNegativeTtl() {
        data.passengers.remove(TR1 + "|" + EMP_A1);
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).isFalse();  // t=0, negative cached
        data.passengers.add(TR1 + "|" + EMP_A1);

        clockNanos.set(Duration.ofSeconds(4).toNanos());
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).as("within the 5s negative TTL").isFalse();
        clockNanos.set(Duration.ofSeconds(6).toNanos());
        assertThat(can(EMP_A1, T1, UserRole.EMPLOYEE, O1, trip(TR1))).as("after the negative TTL").isTrue();
    }

    @Test
    void aTripCreatedAfterAMissIsVisibleAfterTheNegativeTtl() {
        UUID later = id("trip-later");
        assertThat(can(SEC_T1, T1, UserRole.SECURITY_OPERATOR, null, trip(later))).isFalse();
        data.trips.put(later, new TripInfo(later, T1, O1, C1));
        clockNanos.set(Duration.ofSeconds(6).toNanos());
        assertThat(can(SEC_T1, T1, UserRole.SECURITY_OPERATOR, null, trip(later))).isTrue();
    }
}
