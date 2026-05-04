package org.openphc.cce.intelligence.web.dto;

import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DestinationAdaptorMappingDto {
    private UUID id;
    private String destination;
    private UUID receiverAdaptorId;
    private String receiverAdaptorName;
    private String status;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
}
