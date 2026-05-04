package org.openphc.cce.intelligence.domain.entity;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.openphc.cce.intelligence.domain.enums.ActionType;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.openphc.cce.intelligence.domain.enums.IntelligenceSeverity;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "intelligence_delivery")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IntelligenceDelivery {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "intelligence_event_id", nullable = false)
    private UUID intelligenceEventId;

    @Column(name = "action_definition_id", nullable = false)
    private UUID actionDefinitionId;

    @Column(name = "destination_adaptor_mapping_id")
    private UUID destinationAdaptorMappingId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "destination_adaptor_mapping_id", insertable = false, updatable = false)
    private DestinationAdaptorMapping destinationAdaptorMapping;

    @Enumerated(EnumType.STRING)
    @Column(name = "action_type", nullable = false)
    private ActionType actionType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private IntelligenceDeliveryStatus status;

    @Column(nullable = false)
    private String subject;

    @Column(name = "protocol_canonical", nullable = false)
    private String protocolCanonical;

    @Column(name = "action_id", nullable = false)
    private String actionId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private IntelligenceSeverity severity;

    @Column(nullable = false)
    private String destination;

    @Column(name = "fhir_payload", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private JsonNode fhirPayload;

    @Column(name = "delivery_result", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private JsonNode deliveryResult;

    @Column(name = "attempt_count", nullable = false)
    @Builder.Default
    private int attemptCount = 0;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "delivered_at")
    private OffsetDateTime deliveredAt;

    @PrePersist
    protected void onCreate() {
        createdAt = OffsetDateTime.now();
        updatedAt = OffsetDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }
}
