package org.openphc.cce.intelligence.web.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IntelligenceDeliveryDto {
    private UUID id;
    private UUID intelligenceEventId;
    private UUID actionDefinitionId;
    private UUID channelSubscriptionId;
    private String actionType;
    private String status;
    private String subject;
    private String protocolCanonical;
    private String actionId;
    private String severity;
    private String channel;
    private JsonNode fhirPayload;
    private JsonNode deliveryResult;
    private int attemptCount;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private OffsetDateTime deliveredAt;
}
