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
public class ChannelSubscriptionDto {
    private UUID id;
    private UUID protocolDefinitionId;
    private String actionId;
    private String channel;
    private UUID receiverAdaptorId;
    private String receiverAdaptorName;
    private String status;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
