package org.openphc.cce.intelligence.web.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateReceiverAdaptorRequest {
    private String name;
    private JsonNode definition;
    private JsonNode config;
    private String status;
}
