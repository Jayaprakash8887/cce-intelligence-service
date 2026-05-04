package org.openphc.cce.intelligence.domain.entity;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "intelligence_delivery_audit_log")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IntelligenceDeliveryAuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "intelligence_delivery_id", nullable = false)
    private UUID intelligenceDeliveryId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "intelligence_delivery_id", insertable = false, updatable = false)
    private IntelligenceDelivery intelligenceDelivery;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false)
    @Builder.Default
    private String actor = "system";

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private JsonNode details;

    @Column(nullable = false)
    private OffsetDateTime timestamp;

    @PrePersist
    protected void onCreate() {
        if (timestamp == null) {
            timestamp = OffsetDateTime.now();
        }
    }
}
