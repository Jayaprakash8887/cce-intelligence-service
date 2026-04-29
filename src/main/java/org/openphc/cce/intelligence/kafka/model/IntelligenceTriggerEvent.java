package org.openphc.cce.intelligence.kafka.model;

import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IntelligenceTriggerEvent {

    private UUID id;
    private String subject;
    private UUID intelligenceEventId;
    private UUID actionDefinitionId;
    private UUID protocolDefinitionId;
    private String actionType;
    private String severity;
    private String intelligenceChannel;
    private String stepState;
    private String actionId;
    private String protocolCanonical;
    private OffsetDateTime detectedAt;
}
