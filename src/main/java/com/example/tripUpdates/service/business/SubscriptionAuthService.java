package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.enums.UserRole;
import com.example.tripUpdates.service.business.AuthDirectory.CabInfo;
import com.example.tripUpdates.service.business.AuthDirectory.OfficeInfo;
import com.example.tripUpdates.service.business.AuthDirectory.TripInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Decides whether an authenticated user may subscribe to a topic. Called on every SUBSCRIBE and RESUME,
 * not just at login.
 *
 * <pre>
 *                 trip:{id}                          cab:{id}                       office:{id}
 *  EMPLOYEE       same tenant AND passenger          denied                         denied
 *  ADMIN          same tenant AND trip.office==own   same tenant AND cab.office==own  office==own AND same tenant
 *  SECURITY_OP    same tenant                        same tenant                    same tenant
 * </pre>
 *
 * The tenant of the <em>entity</em> is always compared with the tenant in the token, for every role,
 * so no role can reach another client's data. Every denial (malformed id, unknown topic type, unknown
 * entity, other tenant, wrong office, inactive user, backend error) returns plain {@code false}; callers
 * answer all of them identically, so the response never reveals whether an entity exists.
 *
 * Never throws for bad input. Logs ids (UUIDs) only: no names, emails or phone numbers.
 */
@Service
public class SubscriptionAuthService {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionAuthService.class);
    private static final Pattern UUID_PATTERN =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private final CachedDirectory directory;

    public SubscriptionAuthService(CachedDirectory directory) {
        this.directory = directory;
    }

    public boolean isAuthorized(String userId, String tenantId, UserRole role, String officeId, String requestedTopic) {
        try {
            if (role == null || requestedTopic == null) {
                return false;
            }
            UUID user = uuid(userId);
            UUID tenant = uuid(tenantId);
            UUID ownOffice = officeId == null ? null : uuid(officeId);
            if (user == null || tenant == null || (officeId != null && ownOffice == null)) {
                return deny("malformed claims", requestedTopic);
            }

            int colon = requestedTopic.indexOf(':');
            if (colon <= 0) {
                return deny("malformed topic", requestedTopic);
            }
            UUID id = uuid(requestedTopic.substring(colon + 1));
            if (id == null) {
                return deny("malformed topic id", requestedTopic);
            }

            if (!directory.isUserActive(user)) {
                return deny("inactive or unknown user", requestedTopic);
            }

            boolean allowed;
            switch (requestedTopic.substring(0, colon)) {
                case "trip":
                    allowed = tripAllowed(user, tenant, role, ownOffice, id);
                    break;
                case "cab":
                    allowed = cabAllowed(tenant, role, ownOffice, id);
                    break;
                case "office":
                    allowed = officeAllowed(tenant, role, ownOffice, id);
                    break;
                default:
                    return deny("unknown topic type", requestedTopic);
            }
            return allowed || deny("rule", requestedTopic);
        } catch (RuntimeException e) {
            logger.error("Authorization backend error: {}", e.getClass().getSimpleName());
            return false; // fail closed
        }
    }

    private boolean tripAllowed(UUID user, UUID tenant, UserRole role, UUID ownOffice, UUID tripId) {
        Optional<TripInfo> trip = directory.trip(tripId);
        if (trip.isEmpty() || !trip.get().tenantId().equals(tenant)) {
            return false;
        }
        switch (role) {
            case EMPLOYEE:
                return directory.isPassenger(tripId, user);
            case ADMIN:
                return ownOffice != null && trip.get().officeId().equals(ownOffice);
            case SECURITY_OPERATOR:
                return true;
            default:
                return false;
        }
    }

    private boolean cabAllowed(UUID tenant, UserRole role, UUID ownOffice, UUID cabId) {
        Optional<CabInfo> cab = directory.cab(cabId);
        if (cab.isEmpty() || !cab.get().tenantId().equals(tenant)) {
            return false;
        }
        switch (role) {
            case ADMIN:
                return ownOffice != null && ownOffice.equals(cab.get().officeId());
            case SECURITY_OPERATOR:
                return true;
            default:
                return false; // employees follow trips, not cabs
        }
    }

    private boolean officeAllowed(UUID tenant, UserRole role, UUID ownOffice, UUID officeId) {
        Optional<OfficeInfo> office = directory.office(officeId);
        if (office.isEmpty() || !office.get().tenantId().equals(tenant)) {
            return false;
        }
        switch (role) {
            case ADMIN:
                return ownOffice != null && ownOffice.equals(officeId);
            case SECURITY_OPERATOR:
                return true;
            default:
                return false;
        }
    }

    private boolean deny(String reason, String topic) {
        String safe = topic.length() > 80 ? topic.substring(0, 80) : topic;
        logger.debug("Subscription denied ({}): topic={}", reason, safe.replaceAll("[^A-Za-z0-9:_-]", "?"));
        return false;
    }

    private static UUID uuid(String value) {
        return value != null && UUID_PATTERN.matcher(value).matches() ? UUID.fromString(value) : null;
    }
}
