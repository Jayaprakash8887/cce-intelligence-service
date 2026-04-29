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
public class ReceiverAdaptorDto {
    private UUID id;
    private String name;
    private JsonNode definition;
    private String status;
    private JsonNode config;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
