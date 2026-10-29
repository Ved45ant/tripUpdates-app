package com.example.tripUpdates.model.entity;

import com.example.tripUpdates.model.enums.UserRole;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.util.UUID;

/**
 * Represents a user (employee, admin, or security operator) in the system.
 * Users are scoped to a tenant and optionally to an office.
 */
@Entity
@Table(name = "users", indexes = {
        @Index(name = "idx_user_tenant_id", columnList = "tenant_id"),
        @Index(name = "idx_user_office_id", columnList = "office_id")
})
public class User {

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

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private UserRole role;

    @Column(name = "password", nullable = false)
    private String password;

    @Column(name = "is_active", nullable = false)
    private boolean isActive;

    public User() {}

    public User(String tenantId, UserRole role, boolean isActive) {
        this.id = UUID.randomUUID();
        this.tenantId = UUID.fromString(tenantId);
        this.role = role;
        this.isActive = isActive;
    }

    public User(String id, String tenantId, String officeId, UserRole role, String password, boolean isActive) {
        this.id = UUID.fromString(id);
        this.tenantId = UUID.fromString(tenantId);
        this.officeId = officeId != null ? UUID.fromString(officeId) : null;
        this.role = role;
        this.password = password;
        this.isActive = isActive;
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

    public UserRole getRole() {
        return role;
    }

    public void setRole(UserRole role) {
        this.role = role;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
