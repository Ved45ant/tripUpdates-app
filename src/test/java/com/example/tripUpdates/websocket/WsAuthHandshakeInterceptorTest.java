package com.example.tripUpdates.websocket;

import com.example.tripUpdates.model.entity.User;
import com.example.tripUpdates.model.enums.UserRole;
import com.example.tripUpdates.security.JwtTokenProvider;
import org.junit.jupiter.api.Test;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WsAuthHandshakeInterceptorTest {

    private final JwtTokenProvider jwt = new JwtTokenProvider("handshake-secret-handshake-secret-handshake-secret");
    private final WsAuthHandshakeInterceptor interceptor = new WsAuthHandshakeInterceptor(jwt);

    private boolean handshake(String token, Map<String, Object> attributes) throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest();
        if (token != null) {
            req.setParameter("token", token);
        }
        return interceptor.beforeHandshake(new ServletServerHttpRequest(req),
                new ServletServerHttpResponse(new MockHttpServletResponse()), null, attributes);
    }

    private String userToken(UserRole role) {
        User u = new User();
        u.setId(UUID.randomUUID());
        u.setTenantId(UUID.randomUUID());
        u.setOfficeId(UUID.randomUUID());
        u.setRole(role);
        return jwt.generateToken(u);
    }

    @Test
    void userTokenIsAcceptedAndClaimsAndExpiryArePlacedOnTheSession() throws Exception {
        Map<String, Object> attrs = new HashMap<>();
        assertThat(handshake(userToken(UserRole.EMPLOYEE), attrs)).isTrue();
        assertThat(attrs).containsKeys("userId", "tenantId", "role", "officeId", "expiresAtMs");
        assertThat((Long) attrs.get("expiresAtMs")).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    void missingTamperedAndCabDeviceTokensAreRejected() throws Exception {
        assertThat(handshake(null, new HashMap<>())).isFalse();
        assertThat(handshake("garbage", new HashMap<>())).isFalse();
        String token = userToken(UserRole.ADMIN);
        int mid = token.lastIndexOf('.') + 10;                       // a character inside the signature
        char original = token.charAt(mid);
        String tampered = token.substring(0, mid) + (original == 'A' ? 'B' : 'A') + token.substring(mid + 1);
        assertThat(handshake(tampered, new HashMap<>())).isFalse();
        // a device token may publish but must never consume
        String cab = jwt.generateCabToken(UUID.randomUUID(), UUID.randomUUID());
        assertThat(handshake(cab, new HashMap<>())).isFalse();
    }
}
