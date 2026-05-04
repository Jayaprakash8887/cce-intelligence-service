package org.openphc.cce.intelligence.webhook;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.intelligence.config.IntelligenceProperties;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebhookDeliveryClient {

    private final WebClient webClient;
    private final IntelligenceProperties properties;
    private final MeterRegistry meterRegistry;

    /**
     * Delivers a FHIR payload to the webhook endpoint with retry logic.
     * Supports per-adaptor overrides for timeout and retries.
     * Computes HMAC-SHA256 signature if webhookSecret is configured.
     */
    public WebhookResult deliver(JsonNode payload, String endpointUrl, UUID deliveryId,
                                  UUID intelligenceEventId, JsonNode adaptorConfig) {
        int maxAttempts = resolveMaxAttempts(adaptorConfig);
        int retryInterval = resolveRetryInterval(adaptorConfig);
        String adaptorName = resolveAdaptorName(endpointUrl);

        Timer.Sample timerSample = Timer.start(meterRegistry);
        String body = payload.toString();

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                WebClient.RequestBodySpec request = webClient.post()
                        .uri(endpointUrl)
                        .header(HttpHeaders.CONTENT_TYPE, "application/fhir+json")
                        .header("X-CCE-Intelligence-Delivery-Id", deliveryId.toString())
                        .header("X-CCE-Intelligence-Event-Id", intelligenceEventId.toString());

                // HMAC-SHA256 signature if webhookSecret configured
                if (adaptorConfig != null && adaptorConfig.has("webhookSecret")
                        && !adaptorConfig.get("webhookSecret").isNull()) {
                    String secret = adaptorConfig.get("webhookSecret").asText();
                    String signature = computeHmacSha256(secret, body);
                    request.header("X-CCE-Signature-256", "sha256=" + signature);
                }

                // Add adaptor auth headers from config
                if (adaptorConfig != null && adaptorConfig.has("authHeader") && adaptorConfig.has("authValue")) {
                    String headerName = adaptorConfig.get("authHeader").asText();
                    String headerValue = adaptorConfig.get("authValue").asText();
                    request.header(headerName, headerValue);
                }
                if (adaptorConfig != null && adaptorConfig.has("customHeaders") && adaptorConfig.get("customHeaders").isObject()) {
                    adaptorConfig.get("customHeaders").fields().forEachRemaining(entry ->
                            request.header(entry.getKey(), entry.getValue().asText()));
                }

                request.bodyValue(body)
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();

                return stopTimer(timerSample, adaptorName, WebhookResult.success(200, attempt));

            } catch (WebClientResponseException ex) {
                int status = ex.getStatusCode().value();

                if (status >= 400 && status < 500) {
                    log.warn("Non-retryable error from webhook: status={}, url={}, deliveryId={}",
                            status, endpointUrl, deliveryId);
                    return stopTimer(timerSample, adaptorName, WebhookResult.failure(status, ex.getResponseBodyAsString(), attempt));
                }

                log.warn("Retryable error from webhook: status={}, url={}, attempt={}/{}",
                        status, endpointUrl, attempt, maxAttempts);

                if (attempt < maxAttempts) {
                    sleep(retryInterval);
                } else {
                    return stopTimer(timerSample, adaptorName, WebhookResult.failure(status, ex.getResponseBodyAsString(), attempt));
                }

            } catch (Exception ex) {
                log.warn("Webhook delivery error: url={}, attempt={}/{}, error={}",
                        endpointUrl, attempt, maxAttempts, ex.getMessage());

                if (attempt < maxAttempts) {
                    sleep(retryInterval);
                } else {
                    return stopTimer(timerSample, adaptorName, WebhookResult.failure(0, ex.getMessage(), attempt));
                }
            }
        }

        return stopTimer(timerSample, adaptorName, WebhookResult.failure(0, "Max attempts exceeded", maxAttempts));
    }

    private int resolveMaxAttempts(JsonNode adaptorConfig) {
        if (adaptorConfig != null && adaptorConfig.has("retryOverride")) {
            JsonNode override = adaptorConfig.get("retryOverride");
            if (override.has("maxAttempts")) {
                return override.get("maxAttempts").asInt();
            }
        }
        return properties.getWebhook().getRetryAttempts();
    }

    private int resolveRetryInterval(JsonNode adaptorConfig) {
        if (adaptorConfig != null && adaptorConfig.has("retryOverride")) {
            JsonNode override = adaptorConfig.get("retryOverride");
            if (override.has("intervalMs")) {
                return override.get("intervalMs").asInt();
            }
        }
        return properties.getWebhook().getRetryIntervalMs();
    }

    private String resolveAdaptorName(String endpointUrl) {
        try {
            return new java.net.URI(endpointUrl).getHost();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private String computeHmacSha256(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(keySpec);
            byte[] hash = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            log.error("Failed to compute HMAC-SHA256 signature", e);
            throw new RuntimeException("HMAC computation failed", e);
        }
    }

    private WebhookResult stopTimer(Timer.Sample sample, String adaptorName, WebhookResult result) {
        sample.stop(Timer.builder("cce.intelligence.webhook.duration")
                .description("Webhook response time")
                .tag("adaptor_name", adaptorName)
                .register(meterRegistry));
        return result;
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
