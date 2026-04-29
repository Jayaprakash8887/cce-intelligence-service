package org.openphc.cce.intelligence.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreateChannelSubscriptionRequest {

    @NotNull
    private UUID protocolDefinitionId;

    private String actionId;

    @NotBlank
    private String channel;

    @NotNull
    private UUID receiverAdaptorId;
}
