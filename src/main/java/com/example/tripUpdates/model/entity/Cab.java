package com.example.tripUpdates.model.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.util.UUID;

/**
 * Represents a cab (vehicle) in the system.
 * Cabs are the publishers of location, status, and SOS events.
 */
@Entity
@Table(name = "cabs", indexes = {
        @Index(name = "idx_cab_tenant_id", columnList = "tenant_id"),
        @Index(name = "idx_cab_office_id", columnList = "office_id")
})
public class Cab {

    @Id
    @JdbcTypeCode(SqlTypes.UUID)
    @Column(columnDefinition = "UUID")
    private UUID id;

    @Column(name = "tenant_id", nullable = false, columnDefinition = "UUID")
    @JdbcTypeCode(SqlTypes.UUID)
    private UUID tenantId;

    @Column(name = "office_id", columnDefinition = "UUID")
    @JdbcTypeCode(SqlTypes.UUID)
    private UUID officeId;

    @Column(name = "license_plate")
    private String licensePlate;

    @Column(name = "registration")
    private String registration;

    public Cab() {}

    public Cab(String tenantId, String licensePlate) {
        this.id = UUID.randomUUID();
        this.tenantId = UUID.fromString(tenantId);
        this.licensePlate = licensePlate;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public void setTenantId(UUID tenantId) {
        this.tenantId = tenantId;
    }

    public UUID getOfficeId() {
        return officeId;
    }

    public void setOfficeId(UUID officeId) {
        this.officeId = officeId;
    }

    public String getLicensePlate() {
        return licensePlate;
    }

    public void setLicensePlate(String licensePlate) {
        this.licensePlate = licensePlate;
    }

    public String getRegistration() {
        return registration;
    }

    public void setRegistration(String registration) {
        this.registration = registration;
    }
}
