package com.example.tripUpdates.security;

import io.jsonwebtoken.Claims;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Authenticates REST calls from the {@code Authorization: Bearer <jwt>} header. An invalid or missing
 * token simply leaves the request unauthenticated; the security chain then answers 401. The reason
 * is never reported to the caller.
 */
@Component
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private static final Set<String> ROLES = Set.of("EMPLOYEE", "ADMIN", "SECURITY_OPERATOR", AuthPrincipal.CAB_DEVICE);

    private final JwtTokenProvider jwtTokenProvider;

    public JwtAuthenticationFilter(JwtTokenProvider jwtTokenProvider) {
        this.jwtTokenProvider = jwtTokenProvider;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String header = request.getHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            AuthPrincipal principal = parse(header.substring(7).trim());
            if (principal != null) {
                SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(
                        principal, null, List.of(new SimpleGrantedAuthority("ROLE_" + principal.role()))));
            }
        }
        chain.doFilter(request, response);
    }

    private AuthPrincipal parse(String token) {
        try {
            Claims claims = jwtTokenProvider.validateAndGetClaims(token);
            String role = claims.get("role", String.class);
            String tenantId = claims.get("tenantId", String.class);
            if (role == null || tenantId == null || !ROLES.contains(role)) {
                return null;
            }
            if (AuthPrincipal.CAB_DEVICE.equals(role)) {
                String cabId = claims.get("cabId", String.class);
                return cabId == null ? null : new AuthPrincipal(null, tenantId, null, role, cabId);
            }
            String userId = claims.get("userId", String.class);
            return userId == null ? null
                    : new AuthPrincipal(userId, tenantId, claims.get("officeId", String.class), role, null);
        } catch (RuntimeException e) {
            return null; // bad signature, expired, malformed: unauthenticated
        }
    }
}
