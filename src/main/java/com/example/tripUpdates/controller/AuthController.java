package com.example.tripUpdates.controller;

import com.example.tripUpdates.model.entity.User;
import com.example.tripUpdates.repository.UserRepository;
import com.example.tripUpdates.security.JwtTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Authentication controller for JWT token generation.
 * 
 * ENDPOINT:
 * POST /api/v1/auth/login
 * 
 * REQUEST:
 * {
 *   "username": "user-id-uuid",
 *   "password": "password123"
 * }
 * 
 * RESPONSE (Success):
 * HTTP 200
 * {
 *   "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
 *   "userId": "user-id-uuid",
 *   "role": "EMPLOYEE",
 *   "tenantId": "tenant-id-uuid"
 * }
 * 
 * RESPONSE (Failure):
 * HTTP 401
 * {
 *   "error": "Invalid credentials"
 * }
 * 
 * SECURITY:
 * - Validates password against BCrypt hash
 * - Logs authentication attempts (without exposing passwords)
 * - Returns generic error message (no user enumeration)
 * - Token valid for 1 hour
 */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    private static final Logger logger = LoggerFactory.getLogger(AuthController.class);

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenProvider jwtTokenProvider;
    private final com.example.tripUpdates.telemetry.MetricsService metrics;

    public AuthController(UserRepository userRepository,
                          PasswordEncoder passwordEncoder,
                          JwtTokenProvider jwtTokenProvider,
                          com.example.tripUpdates.telemetry.MetricsService metrics) {
        this.metrics = metrics;
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtTokenProvider = jwtTokenProvider;
    }

    /**
     * Authenticates user with username and password.
     * Returns JWT token on success, 401 on failure.
     * 
     * @param request Login request with username and password
     * @return JWT token response or error
     */
    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody LoginRequest request) {
        logger.debug("Login attempt: username={}", request.getUsername());

        // Validate request
        if (request.getUsername() == null || request.getUsername().isEmpty() ||
            request.getPassword() == null || request.getPassword().isEmpty()) {
            logger.warn("Login failed: Missing username or password");
            metrics.recordAuthRejection("login");
            return ResponseEntity
                    .status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "Invalid credentials"));
        }

        // Find user by ID (username is the user ID)
        UUID userId;
        try {
            userId = UUID.fromString(request.getUsername());
        } catch (IllegalArgumentException e) {
            metrics.recordAuthRejection("login");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Invalid credentials"));
        }
        Optional<User> userOpt = userRepository.findById(userId);
        if (userOpt.isEmpty()) {
            logger.warn("Login failed: User not found - username={}", request.getUsername());
            metrics.recordAuthRejection("login");
            return ResponseEntity
                    .status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "Invalid credentials"));
        }

        User user = userOpt.get();

        // Verify password
        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            logger.warn("Login failed: Invalid password - username={}", request.getUsername());
            metrics.recordAuthRejection("login");
            return ResponseEntity
                    .status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "Invalid credentials"));
        }

        // Check if user is active
        if (!user.isActive()) {
            logger.warn("Login failed: User inactive - username={}", request.getUsername());
            metrics.recordAuthRejection("login");
            return ResponseEntity
                    .status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "Invalid credentials"));
        }

        // Generate JWT token
        String token = jwtTokenProvider.generateToken(user);
        logger.info("Login successful: userId={}, role={}", user.getId(), user.getRole());

        // Return token response
        Map<String, String> response = new HashMap<>();
        response.put("token", token);
        response.put("userId", user.getId().toString());
        response.put("role", user.getRole().name());
        response.put("tenantId", user.getTenantId().toString());

        return ResponseEntity.ok(response);
    }

    /**
     * Login request DTO.
     */
    public static class LoginRequest {
        private String username;
        private String password;

        public LoginRequest() {}

        public LoginRequest(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
