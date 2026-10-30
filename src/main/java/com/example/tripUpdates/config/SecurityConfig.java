package com.example.tripUpdates.config;

import com.example.tripUpdates.security.JwtAuthenticationFilter;
import com.example.tripUpdates.telemetry.MetricsService;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Spring Security configuration for the TripUpdates application.
 * 
 * SECURITY MODEL:
 * - Stateless (JWT-based) - sessions disabled
 * - CSRF disabled (JWT is immune to CSRF)
 * - Password encoding via BCrypt (strength 12)
 * 
 * PUBLIC: /actuator/**, POST /api/v1/auth/login, /ws/** (handshake validates the JWT itself)
 * ROLE_CAB_DEVICE: /api/v1/events/**  (device token, one cab, one tenant)
 * ROLE_ADMIN / ROLE_SECURITY_OPERATOR: /api/v1/auth/cab-token
 * 
 * ALL OTHER ENDPOINTS:
 * - Require authentication (JWT token)
 * 
 * JWT FLOW:
 * 1. Client POST /api/v1/auth/login with username/password
 * 2. Server returns JWT token
 * 3. Client uses token in WebSocket query: /ws/updates?token=...
 * 4. WsAuthHandshakeInterceptor validates JWT before upgrade
 * 
 * STATELESS:
 * - No server-side session state
 * - Each request is independent
 * - Enables horizontal scaling
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    /**
     * BCryptPasswordEncoder bean for password hashing.
     * Strength 12 provides good balance between security and performance.
     * 
     * @return BCryptPasswordEncoder instance
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(12);
    }

    /**
     * Configures the security filter chain.
     * Permits public endpoints, disables CSRF, enables stateless auth.
     * 
     * @param http HttpSecurity builder
     * @return Configured SecurityFilterChain
     */
    /** Authentication is JWT only; an empty user store stops Spring Boot from generating a default password. */
    @Bean
    public UserDetailsService userDetailsService() {
        return new InMemoryUserDetailsManager();
    }

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, JwtAuthenticationFilter jwtFilter,
                                                   MetricsService metrics) throws Exception {
        http
                // Disable CSRF (JWT is immune)
                .csrf(csrf -> csrf.disable())
                
                // Permit specific endpoints
                .authorizeHttpRequests(authz -> authz
                        // Public endpoints
                        .requestMatchers("/actuator/**").permitAll()          // internal network only
                        .requestMatchers(HttpMethod.POST, "/api/v1/auth/login").permitAll()
                        .requestMatchers("/ws/**").permitAll()                // JWT checked in the handshake
                        // admins mint device tokens for cabs of their tenant
                        .requestMatchers("/api/v1/auth/cab-token").hasAnyRole("ADMIN", "SECURITY_OPERATOR")
                        // only cab devices publish events
                        .requestMatchers("/api/v1/events/**").hasRole("CAB_DEVICE")
                        
                        // All other endpoints require authentication
                        .anyRequest().authenticated()
                )
                
                // Stateless session management
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                )
                .addFilterBefore(jwtFilter, UsernamePasswordAuthenticationFilter.class)
                .exceptionHandling(ex -> ex.authenticationEntryPoint((request, response, authException) -> {
                    metrics.recordAuthRejection("rest");
                    response.setStatus(HttpStatus.UNAUTHORIZED.value());
                }));

        return http.build();
    }
}
