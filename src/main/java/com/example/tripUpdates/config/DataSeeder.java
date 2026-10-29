package com.example.tripUpdates.config;

import com.example.tripUpdates.model.entity.*;
import com.example.tripUpdates.model.enums.TripStatus;
import com.example.tripUpdates.model.enums.UserRole;
import com.example.tripUpdates.repository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Data seeder for initializing database with test data on application startup.
 * 
 * DETERMINISTIC SEED DATA:
 * - Tenant: tenant-1 (Alpha Corp)
 * - Office: office-1 (HQ Bangalore) → tenant-1
 * - Cab: cab-1 (Vehicle 001) → office-1
 * 
 * USERS:
 * - user-1 (EMPLOYEE) → tenant-1, office-1, password: password123
 * - user-2 (ADMIN) → tenant-1, office-1, password: password123
 * - user-3 (SECURITY_OPERATOR) → tenant-1, no office, password: password123
 * 
 * TRIP:
 * - trip-1 (ACTIVE) → tenant-1, office-1, cab-1
 * - Passenger: user-1 assigned to trip-1
 * 
 * IDEMPOTENT:
 * - Only runs if database is empty (userRepository.count() == 0)
 * - Prevents duplicate inserts on restart
 * 
 * PURPOSE:
 * - Enable automated k6 load testing without manual setup
 * - Provide consistent test data for development
 * - Quick local testing without database migration scripts
 * 
 * SECURITY NOTE:
 * - Hardcoded credentials are for testing ONLY
 * - For production, use proper Flyway/Liquibase migrations
 * - Never commit production seed data
 */
@Component
@Order(1)
public class DataSeeder implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DataSeeder.class);

    private final TenantRepository tenantRepository;
    private final OfficeRepository officeRepository;
    private final UserRepository userRepository;
    private final CabRepository cabRepository;
    private final TripRepository tripRepository;
    private final TripPassengerRepository tripPassengerRepository;
    private final PasswordEncoder passwordEncoder;

    public DataSeeder(TenantRepository tenantRepository,
                      OfficeRepository officeRepository,
                      UserRepository userRepository,
                      CabRepository cabRepository,
                      TripRepository tripRepository,
                      TripPassengerRepository tripPassengerRepository,
                      PasswordEncoder passwordEncoder) {
        this.tenantRepository = tenantRepository;
        this.officeRepository = officeRepository;
        this.userRepository = userRepository;
        this.cabRepository = cabRepository;
        this.tripRepository = tripRepository;
        this.tripPassengerRepository = tripPassengerRepository;
        this.passwordEncoder = passwordEncoder;
    }

    /**
     * Runs on application startup.
     * Populates database with deterministic test data if empty.
     * 
     * @param args Command line arguments
     */
    @Override
    public void run(String... args) {
        // Check if data already seeded (idempotent)
        if (userRepository.count() > 0) {
            logger.info("Database already seeded, skipping initialization");
            return;
        }

        logger.info("Initializing database with seed data");

        try {
            // 1. Create Tenant using constructor
            Tenant tenant = new Tenant("Alpha Corp");
            tenant.setId(UUID.fromString("12345678-1234-1234-1234-123456789012"));
            tenantRepository.save(tenant);
            logger.debug("Created tenant: tenant-1 (Alpha Corp)");

            // 2. Create Office
            Office office = new Office();
            office.setId(UUID.fromString("12345678-1234-1234-1234-123456789013"));
            office.setTenantId(UUID.fromString("12345678-1234-1234-1234-123456789012"));
            office.setName("HQ Bangalore");
            officeRepository.save(office);
            logger.debug("Created office: office-1 (HQ Bangalore)");

            // 3. Create Cab using constructor
            Cab cab = new Cab(
                    UUID.fromString("12345678-1234-1234-1234-123456789012").toString(),
                    "KA01AB0001"
            );
            cab.setId(UUID.fromString("12345678-1234-1234-1234-123456789014"));
            cab.setOfficeId(UUID.fromString("12345678-1234-1234-1234-123456789013"));
            cabRepository.save(cab);
            logger.debug("Created cab: cab-1");

            // 4. Create Users
            User employee = new User();
            employee.setId(UUID.fromString("12345678-1234-1234-1234-123456789015"));
            employee.setTenantId(UUID.fromString("12345678-1234-1234-1234-123456789012"));
            employee.setOfficeId(UUID.fromString("12345678-1234-1234-1234-123456789013"));
            employee.setRole(UserRole.EMPLOYEE);
            employee.setPassword(passwordEncoder.encode("password123"));
            employee.setActive(true);
            userRepository.save(employee);
            logger.debug("Created user: user-1 (EMPLOYEE)");

            User admin = new User();
            admin.setId(UUID.fromString("12345678-1234-1234-1234-123456789016"));
            admin.setTenantId(UUID.fromString("12345678-1234-1234-1234-123456789012"));
            admin.setOfficeId(UUID.fromString("12345678-1234-1234-1234-123456789013"));
            admin.setRole(UserRole.ADMIN);
            admin.setPassword(passwordEncoder.encode("password123"));
            admin.setActive(true);
            userRepository.save(admin);
            logger.debug("Created user: user-2 (ADMIN)");

            User securityOp = new User();
            securityOp.setId(UUID.fromString("12345678-1234-1234-1234-123456789017"));
            securityOp.setTenantId(UUID.fromString("12345678-1234-1234-1234-123456789012"));
            securityOp.setOfficeId(null); // Security operators span multiple offices
            securityOp.setRole(UserRole.SECURITY_OPERATOR);
            securityOp.setPassword(passwordEncoder.encode("password123"));
            securityOp.setActive(true);
            userRepository.save(securityOp);
            logger.debug("Created user: user-3 (SECURITY_OPERATOR)");

            // 5. Create Trip
            Trip trip = new Trip(
                    UUID.fromString("12345678-1234-1234-1234-123456789012").toString(),
                    UUID.fromString("12345678-1234-1234-1234-123456789013").toString(),
                    UUID.fromString("12345678-1234-1234-1234-123456789014").toString(),
                    TripStatus.ACTIVE
            );
            trip.setId(UUID.fromString("12345678-1234-1234-1234-123456789018"));
            tripRepository.save(trip);
            logger.debug("Created trip: trip-1 (ACTIVE)");

            // 6. Assign Passenger to Trip
            TripPassenger passenger = new TripPassenger(
                    UUID.fromString("12345678-1234-1234-1234-123456789018").toString(),
                    UUID.fromString("12345678-1234-1234-1234-123456789015").toString()
            );
            tripPassengerRepository.save(passenger);
            logger.debug("Assigned user-1 as passenger to trip-1");

            logger.info("Database seed completed successfully. Ready for testing.");
            logger.info("  Tenant: tenant-1 (Alpha Corp)");
            logger.info("  Office: office-1 (HQ Bangalore)");
            logger.info("  Users: user-1 (EMPLOYEE), user-2 (ADMIN), user-3 (SECURITY_OPERATOR)");
            logger.info("  Trip: trip-1 (ACTIVE, cab-1)");
            logger.info("  Passenger: user-1 on trip-1");
            logger.info("  All users password: password123");

        } catch (Exception e) {
            logger.error("Failed to seed database", e);
            throw new RuntimeException("Database seeding failed", e);
        }
    }
}
