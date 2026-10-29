package com.example.tripUpdates.model.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.util.UUID;

/**
 * Represents an office (physical location) within a tenant.
 * Office admins are restricted to subscribing to trips linked to their office only.
 */
@Entity
@Table(name = "offices", indexes = {
        @Index(name = "idx_office_tenant_id", columnList = "tenant_id")
})
public class Office {

    @Id
    @JdbcTypeCode(SqlTypes.UUID)
    @Column(columnDefinition = "UUID")
    private UUID id;

    @Column(name = "tenant_id", nullable = false, columnDefinition = "UUID")
    @JdbcTypeCode(SqlTypes.UUID)
    private UUID tenantId;

    @Column(nullable = false)
    private String name;

    public Office() {}

    public Office(String tenantId, String name) {
        this.id = UUID.randomUUID();
        this.tenantId = UUID.fromString(tenantId);
        this.name = name;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
