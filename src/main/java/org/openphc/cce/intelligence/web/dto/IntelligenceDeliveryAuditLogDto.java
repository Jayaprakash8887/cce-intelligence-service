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
public class IntelligenceDeliveryAuditLogDto {
    private UUID id;
    private UUID intelligenceDeliveryId;
    private String eventType;
    private String actor;
    private JsonNode details;
    private OffsetDateTime timestamp;
}
