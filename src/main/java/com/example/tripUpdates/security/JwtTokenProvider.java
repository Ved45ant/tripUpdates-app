package com.example.tripUpdates.security;

import com.example.tripUpdates.model.entity.User;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * JWT token provider for authentication and validation.
 * 
 * TOKEN STRUCTURE (JWT Claims):
 * - sub (subject): userId
 * - tenantId: Tenant isolation claim
 * - officeId: Office scope (optional, for ADMIN)
 * - role: User role (EMPLOYEE, ADMIN, SECURITY_OPERATOR)
 * - iat: Issued at timestamp
 * - exp: Expiration timestamp (1 hour)
 * 
 * USAGE:
 * 1. generateToken(User) → JWT string for login response
 * 2. validateAndGetClaims(token) → Claims for WebSocket auth
 * 
 * SECURITY:
 * - HMAC SHA-256 signing with server secret
 * - Tokens expire after 1 hour
 * - Signature verified before claim extraction
 * - Invalid/expired tokens raise JwtException
 */
@Component
public class JwtTokenProvider {

    private final SecretKey key;
    private final long expirationMs = 3600000; // 1 hour in milliseconds

    public JwtTokenProvider(@Value("${jwt.secret:your-256-bit-secret-key-here-that-is-very-long}") String secret) {
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Generates a new JWT token for the given user.
     * Token includes userId, tenantId, officeId, and role in claims.
     * 
     * @param user User entity
     * @return Signed JWT token string
     */
    public String generateToken(User user) {
        return Jwts.builder()
                .subject(user.getId().toString()) // Subject is userId (convert UUID to String)
                .claim("userId", user.getId().toString())
                .claim("tenantId", user.getTenantId().toString())
                .claim("officeId", user.getOfficeId() != null ? user.getOfficeId().toString() : null)
                .claim("role", user.getRole().name())
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + expirationMs))
                .signWith(key)
                .compact();
    }

    /** Cab-device tokens live 12 hours; they only allow publishing events for that one cab. */
    private static final long CAB_TOKEN_EXPIRATION_MS = 12L * 3600 * 1000;

    public static long cabTokenLifetimeSeconds() {
        return CAB_TOKEN_EXPIRATION_MS / 1000;
    }

    public String generateCabToken(java.util.UUID cabId, java.util.UUID tenantId) {
        return Jwts.builder()
                .subject("cab:" + cabId)
                .claim("cabId", cabId.toString())
                .claim("tenantId", tenantId.toString())
                .claim("role", "CAB_DEVICE")
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + CAB_TOKEN_EXPIRATION_MS))
                .signWith(key)
                .compact();
    }

    /**
     * Validates a JWT token and extracts its claims.
     * Raises JwtException if token is invalid, expired, or signature is invalid.
     * 
     * @param token JWT token string
     * @return Claims object containing token payload
     */
    public Claims validateAndGetClaims(String token) {
        return Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
}
