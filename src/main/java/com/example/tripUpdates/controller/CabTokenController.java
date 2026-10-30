package com.example.tripUpdates.controller;

import com.example.tripUpdates.security.AuthPrincipal;
import com.example.tripUpdates.security.JwtTokenProvider;
import com.example.tripUpdates.service.business.AuthDirectory.CabInfo;
import com.example.tripUpdates.service.business.CachedDirectory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Issues cab-device tokens. An admin may mint one for a cab of their own office, a security
 * operator for any cab of their tenant. Anything else (unknown cab, other tenant, other office,
 * malformed id) gets the same 403.
 */
@RestController
@RequestMapping("/api/v1/auth")
public class CabTokenController {

    public static class CabTokenRequest {
        private String cabId;

        public String getCabId() {
            return cabId;
        }

        public void setCabId(String cabId) {
            this.cabId = cabId;
        }
    }

    private final CachedDirectory directory;
    private final JwtTokenProvider jwtTokenProvider;

    public CabTokenController(CachedDirectory directory, JwtTokenProvider jwtTokenProvider) {
        this.directory = directory;
        this.jwtTokenProvider = jwtTokenProvider;
    }

    @PostMapping("/cab-token")
    public ResponseEntity<Map<String, Object>> issue(@RequestBody CabTokenRequest request,
                                                     @AuthenticationPrincipal AuthPrincipal principal) {
        UUID cabId;
        UUID tenantId;
        UUID ownOffice;
        try {
            cabId = UUID.fromString(request.getCabId());
            tenantId = UUID.fromString(principal.tenantId());
            ownOffice = principal.officeId() == null ? null : UUID.fromString(principal.officeId());
        } catch (RuntimeException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        Optional<CabInfo> cab = directory.cab(cabId);
        boolean allowed = cab.isPresent() && cab.get().tenantId().equals(tenantId)
                && ("SECURITY_OPERATOR".equals(principal.role())
                || ("ADMIN".equals(principal.role()) && ownOffice != null && ownOffice.equals(cab.get().officeId())));
        if (!allowed) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        return ResponseEntity.ok(Map.of(
                "token", jwtTokenProvider.generateCabToken(cabId, tenantId),
                "expiresInSeconds", JwtTokenProvider.cabTokenLifetimeSeconds()));
    }
}
