package org.openphc.cce.intelligence.webhook;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;

@Getter
public class WebhookResult {

    private final boolean success;
    private final int httpStatus;
    private final String errorMessage;
    private final int attempts;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WebhookResult(boolean success, int httpStatus, String errorMessage, int attempts) {
        this.success = success;
        this.httpStatus = httpStatus;
        this.errorMessage = errorMessage;
        this.attempts = attempts;
    }

    public static WebhookResult success(int httpStatus, int attempts) {
        return new WebhookResult(true, httpStatus, null, attempts);
    }

    public static WebhookResult failure(int httpStatus, String errorMessage, int attempts) {
        return new WebhookResult(false, httpStatus, errorMessage, attempts);
    }

    public JsonNode toJsonNode() {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("success", success);
        node.put("httpStatus", httpStatus);
        node.put("attempts", attempts);
        if (errorMessage != null) {
            node.put("errorMessage", errorMessage);
        }
        return node;
    }
}
