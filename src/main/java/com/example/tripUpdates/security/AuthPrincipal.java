package com.example.tripUpdates.security;

/**
 * Identity extracted from a verified JWT. User tokens (EMPLOYEE, ADMIN, SECURITY_OPERATOR) carry
 * {@code userId}; cab-device tokens (CAB_DEVICE) carry {@code cabId}. Only ids: no personal data.
 */
public record AuthPrincipal(String userId, String tenantId, String officeId, String role, String cabId) {

    public static final String CAB_DEVICE = "CAB_DEVICE";

    public boolean isCabDevice() {
        return CAB_DEVICE.equals(role);
    }
}
