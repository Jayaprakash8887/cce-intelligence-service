package org.openphc.cce.intelligence.domain.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "channel_subscription")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ChannelSubscription {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "protocol_definition_id", nullable = false)
    private UUID protocolDefinitionId;

    @Column(name = "action_id")
    private String actionId;

    @Column(nullable = false)
    private String channel;

    @Column(name = "receiver_adaptor_id", nullable = false)
    private UUID receiverAdaptorId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "receiver_adaptor_id", insertable = false, updatable = false)
    private ReceiverAdaptor receiverAdaptor;

    @Column(nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = OffsetDateTime.now();
        updatedAt = OffsetDateTime.now();
        if (status == null) {
            status = "ACTIVE";
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }
}
