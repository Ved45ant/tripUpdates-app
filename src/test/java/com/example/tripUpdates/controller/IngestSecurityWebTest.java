package com.example.tripUpdates.controller;

import com.example.tripUpdates.config.SecurityConfig;
import com.example.tripUpdates.model.dto.BaseTripEvent;
import com.example.tripUpdates.model.dto.LocationUpdateEvent;
import com.example.tripUpdates.model.dto.SosAlertEvent;
import com.example.tripUpdates.model.dto.TripStatusEvent;
import com.example.tripUpdates.model.entity.User;
import com.example.tripUpdates.model.enums.UserRole;
import com.example.tripUpdates.security.JwtAuthenticationFilter;
import com.example.tripUpdates.security.JwtTokenProvider;
import com.example.tripUpdates.service.business.AuthDirectory;
import com.example.tripUpdates.service.business.CachedDirectory;
import com.example.tripUpdates.service.business.EventPublishingService;
import com.example.tripUpdates.service.business.IngestAuthorizer;
import com.example.tripUpdates.service.business.SubscriptionAuthServiceTest;
import com.example.tripUpdates.service.business.SubscriptionAuthServiceTest.FakeDirectory;
import com.example.tripUpdates.exception.InvalidTripTransitionException;
import com.example.tripUpdates.exception.PublishUnavailableException;
import com.example.tripUpdates.telemetry.MetricsService;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** REST security: who may publish events and who may mint cab tokens (web layer only, no database). */
@WebMvcTest(controllers = {CabIngestController.class, CabTokenController.class})
@Import({SecurityConfig.class, JwtAuthenticationFilter.class, JwtTokenProvider.class,
        IngestSecurityWebTest.Beans.class})
@TestPropertySource(properties = "jwt.secret=test-secret-test-secret-test-secret-test-secret")
class IngestSecurityWebTest {

    static final FakeDirectory DATA = new FakeDirectory();
    static final List<BaseTripEvent> PUBLISHED = new CopyOnWriteArrayList<>();

    @TestConfiguration
    static class Beans {
        @Bean
        CachedDirectory cachedDirectory() {
            return new CachedDirectory(DATA, Duration.ofSeconds(60), Duration.ofSeconds(30), Duration.ofSeconds(5),
                    com.github.benmanes.caffeine.cache.Ticker.systemTicker());
        }

        @Bean
        IngestAuthorizer ingestAuthorizer(CachedDirectory directory) {
            return new IngestAuthorizer(directory);
        }

        @Bean
        MetricsService metricsService() {
            return new MetricsService(new SimpleMeterRegistry());
        }

        @Bean
        EventPublishingService eventPublishingService() {
            return new EventPublishingService(null, null, null, new ObjectMapper(), null,
                    EventPublishingService.RetryPolicy.defaults()) {
                private void simulate(BaseTripEvent event) {
                    if (event.getEventId().startsWith("conflict-")) {
                        throw new InvalidTripTransitionException("no");
                    }
                    if (event.getEventId().startsWith("down-")) {
                        throw new PublishUnavailableException("redis down", null);
                    }
                    PUBLISHED.add(event);
                }

                @Override
                public void publishLocation(LocationUpdateEvent event) {
                    simulate(event);
                }

                @Override
                public void publishStatus(TripStatusEvent event) {
                    simulate(event);
                }

                @Override
                public void publishSos(SosAlertEvent event) {
                    simulate(event);
                }
            };
        }
    }

    @Autowired
    MockMvc mvc;
    @Autowired
    JwtTokenProvider jwt;

    static final UUID T1 = SubscriptionAuthServiceTest.T1, T2 = SubscriptionAuthServiceTest.T2;
    static final UUID O1 = SubscriptionAuthServiceTest.O1, O2 = SubscriptionAuthServiceTest.O2;
    static final UUID C1 = SubscriptionAuthServiceTest.C1, C2 = SubscriptionAuthServiceTest.C2, C3 = SubscriptionAuthServiceTest.C3;
    static final UUID TR1 = SubscriptionAuthServiceTest.TR1, TR2 = SubscriptionAuthServiceTest.TR2, TR3 = SubscriptionAuthServiceTest.TR3;

    @BeforeEach
    void setUp() {
        DATA.offices.clear();
        DATA.cabs.clear();
        DATA.trips.clear();
        DATA.offices.put(O1, new AuthDirectory.OfficeInfo(O1, T1));
        DATA.offices.put(O2, new AuthDirectory.OfficeInfo(O2, T1));
        DATA.cabs.put(C1, new AuthDirectory.CabInfo(C1, T1, O1));
        DATA.cabs.put(C2, new AuthDirectory.CabInfo(C2, T1, O2));
        DATA.cabs.put(C3, new AuthDirectory.CabInfo(C3, T2, SubscriptionAuthServiceTest.O3));
        DATA.trips.put(TR1, new AuthDirectory.TripInfo(TR1, T1, O1, C1));
        DATA.trips.put(TR2, new AuthDirectory.TripInfo(TR2, T1, O2, C2));
        DATA.trips.put(TR3, new AuthDirectory.TripInfo(TR3, T2, SubscriptionAuthServiceTest.O3, C3));
        PUBLISHED.clear();
    }

    private String userToken(UserRole role, UUID tenant, UUID office) {
        User u = new User();
        u.setId(UUID.randomUUID());
        u.setTenantId(tenant);
        u.setOfficeId(office);
        u.setRole(role);
        u.setActive(true);
        return jwt.generateToken(u);
    }

    private String cabToken(UUID cab, UUID tenant) {
        return jwt.generateCabToken(cab, tenant);
    }

    private static String location(UUID cab, UUID trip, String office) {
        return location(cab, trip, office, "ev-1");
    }

    private static String location(UUID cab, UUID trip, String office, String eventId) {
        return "{\"eventType\":\"LOCATION\",\"eventId\":\"" + eventId + "\",\"cabId\":\"" + cab + "\",\"tripId\":\"" + trip
                + "\",\"officeId\":\"" + office + "\",\"timestamp\":1,\"payload\":{\"lat\":1.0,\"lng\":2.0,\"speed\":3}}";
    }

    private MockHttpServletRequestBuilder ingest(String path, String token, String body) {
        MockHttpServletRequestBuilder b = post("/api/v1/events/" + path).contentType(MediaType.APPLICATION_JSON).content(body);
        return token == null ? b : b.header("Authorization", "Bearer " + token);
    }

    // ------------------------------------------------------------------------------------ authentication

    @Test
    void missingOrInvalidTokensAreRejectedWith401() throws Exception {
        String body = location(C1, TR1, O1.toString());
        mvc.perform(ingest("location", null, body)).andExpect(status().isUnauthorized());
        mvc.perform(ingest("location", "garbage", body)).andExpect(status().isUnauthorized());
        mvc.perform(ingest("location", cabToken(C1, T1) + "x", body)).andExpect(status().isUnauthorized());
        assertThat(PUBLISHED).isEmpty();
    }

    @Test
    void userTokensCannotPublishEvents() throws Exception {
        String body = location(C1, TR1, O1.toString());
        for (UserRole role : UserRole.values()) {
            mvc.perform(ingest("location", userToken(role, T1, O1), body)).andExpect(status().isForbidden());
        }
        assertThat(PUBLISHED).isEmpty();
    }

    // ------------------------------------------------------------------------------------ ownership

    @Test
    void aCabPublishesForItsOwnTripAndTheOfficeComesFromTheTripNotTheClient() throws Exception {
        String lyingOffice = O2.toString(); // the client claims another office
        mvc.perform(ingest("location", cabToken(C1, T1), location(C1, TR1, lyingOffice))).andExpect(status().isAccepted());

        assertThat(PUBLISHED).hasSize(1);
        assertThat(PUBLISHED.get(0).getOfficeId()).isEqualTo(O1.toString());
    }

    @Test
    void aCabCannotPublishForAnotherCabOrTripOrTenantOrUnknownTrip() throws Exception {
        String token = cabToken(C1, T1);
        mvc.perform(ingest("location", token, location(C2, TR1, O1.toString()))).andExpect(status().isForbidden()); // other cab id
        mvc.perform(ingest("location", token, location(C1, TR2, O1.toString()))).andExpect(status().isForbidden()); // trip of another cab
        mvc.perform(ingest("location", token, location(C1, TR3, O1.toString()))).andExpect(status().isForbidden()); // other tenant's trip
        mvc.perform(ingest("location", token, location(C1, UUID.randomUUID(), O1.toString()))).andExpect(status().isForbidden());
        mvc.perform(ingest("location", token, location(C1, TR1, O1.toString()).replace(TR1.toString(), "not-a-uuid")))
                .andExpect(status().isForbidden());
        // a token for the right cab id but the wrong tenant
        mvc.perform(ingest("location", cabToken(C1, T2), location(C1, TR1, O1.toString()))).andExpect(status().isForbidden());
        assertThat(PUBLISHED).isEmpty();
    }

    @Test
    void statusAndSosAreGuardedLikeLocation() throws Exception {
        String token = cabToken(C1, T1);
        String status = "{\"eventType\":\"STATUS\",\"eventId\":\"s-1\",\"cabId\":\"" + C1 + "\",\"tripId\":\"" + TR2 + "\",\"timestamp\":1,"
                + "\"payload\":{\"status\":\"ACTIVE\"}}";
        String sos = "{\"eventType\":\"SOS\",\"eventId\":\"sos-1\",\"cabId\":\"" + C1 + "\",\"tripId\":\"" + TR2 + "\",\"timestamp\":1,"
                + "\"payload\":{\"level\":\"HIGH\"}}";
        mvc.perform(ingest("status", token, status)).andExpect(status().isForbidden());
        mvc.perform(ingest("sos", token, sos)).andExpect(status().isForbidden());

        mvc.perform(ingest("status", token, status.replace(TR2.toString(), TR1.toString()))).andExpect(status().isAccepted());
        mvc.perform(ingest("sos", token, sos.replace(TR2.toString(), TR1.toString()))).andExpect(status().isAccepted());
        assertThat(PUBLISHED).hasSize(2);
        assertThat(PUBLISHED).allSatisfy(e -> assertThat(e.getOfficeId()).isEqualTo(O1.toString()));
    }

    // ------------------------------------------------------------------------------------ outcomes

    @Test
    void invalidEventsGet400BeforeAnythingElse() throws Exception {
        String token = cabToken(C1, T1);
        String valid = location(C1, TR1, O1.toString());
        mvc.perform(ingest("location", token, valid.replace("\"eventId\":\"ev-1\",", ""))).andExpect(status().isBadRequest());   // no eventId
        mvc.perform(ingest("location", token, valid.replace("\"lat\":1.0", "\"lat\":123.0"))).andExpect(status().isBadRequest());
        mvc.perform(ingest("location", token, valid.replace("\"timestamp\":1", "\"timestamp\":0"))).andExpect(status().isBadRequest());
        mvc.perform(ingest("location", token, valid.replace("\"timestamp\":1", "\"timestamp\":99999999999999"))).andExpect(status().isBadRequest());
        mvc.perform(ingest("location", token, valid.replace("ev-1", "bad id with spaces"))).andExpect(status().isBadRequest());
        assertThat(PUBLISHED).isEmpty();
    }

    @Test
    void conflictsAndOutagesAreReportedHonestly() throws Exception {
        String token = cabToken(C1, T1);
        mvc.perform(ingest("location", token, location(C1, TR1, O1.toString(), "conflict-1"))).andExpect(status().isConflict());
        mvc.perform(ingest("location", token, location(C1, TR1, O1.toString(), "down-1")))
                .andExpect(status().isServiceUnavailable())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.header().string("Retry-After", "1"));
        mvc.perform(ingest("location", token, location(C1, TR1, O1.toString(), "fine-1"))).andExpect(status().isAccepted());
        assertThat(PUBLISHED).hasSize(1);
    }

    // ------------------------------------------------------------------------------------ minting cab tokens

    private JsonNode mint(String token, UUID cab, int expectedStatus) throws Exception {
        MockHttpServletRequestBuilder b = post("/api/v1/auth/cab-token").contentType(MediaType.APPLICATION_JSON)
                .content("{\"cabId\":\"" + cab + "\"}");
        if (token != null) {
            b = b.header("Authorization", "Bearer " + token);
        }
        String body = mvc.perform(b).andExpect(status().is(expectedStatus)).andReturn().getResponse().getContentAsString();
        return body.isEmpty() ? null : new ObjectMapper().readTree(body);
    }

    @Test
    void onlyAdminsOfTheCabsOfficeAndOperatorsOfTheTenantCanMintCabTokens() throws Exception {
        assertThat(mint(userToken(UserRole.ADMIN, T1, O1), C1, 200).get("token").asText()).isNotBlank();
        mint(userToken(UserRole.ADMIN, T1, O1), C2, 403);                       // cab of another office
        mint(userToken(UserRole.ADMIN, T1, O1), C3, 403);                       // cab of another tenant
        mint(userToken(UserRole.ADMIN, T1, O1), UUID.randomUUID(), 403);        // unknown cab: same answer
        mint(userToken(UserRole.ADMIN, T1, null), C1, 403);                     // admin without office
        assertThat(mint(userToken(UserRole.SECURITY_OPERATOR, T1, null), C2, 200)).isNotNull();
        mint(userToken(UserRole.SECURITY_OPERATOR, T1, null), C3, 403);         // other tenant
        mint(userToken(UserRole.EMPLOYEE, T1, O1), C1, 403);
        mint(cabToken(C1, T1), C1, 403);                                        // a cab cannot mint tokens
        mint(null, C1, 401);
    }

    @Test
    void aMintedTokenWorksForTheCabAndOnlyThatCab() throws Exception {
        String minted = mint(userToken(UserRole.ADMIN, T1, O1), C1, 200).get("token").asText();
        mvc.perform(ingest("location", minted, location(C1, TR1, O1.toString()))).andExpect(status().isAccepted());
        mvc.perform(ingest("location", minted, location(C2, TR2, O2.toString()))).andExpect(status().isForbidden());
    }
}
