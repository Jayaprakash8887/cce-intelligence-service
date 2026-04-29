package org.openphc.cce.intelligence.web.dto;

import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IntelligenceDeliverySummaryDto {
    private UUID id;
    private UUID intelligenceEventId;
    private String actionType;
    private String actionId;
    private String channel;
    private UUID channelSubscriptionId;
    private String status;
    private String subject;
    private String protocolCanonical;
    private String severity;
    private int attemptCount;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private OffsetDateTime deliveredAt;
}
