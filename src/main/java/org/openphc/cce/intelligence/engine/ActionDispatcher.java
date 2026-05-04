package org.openphc.cce.intelligence.engine;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.intelligence.domain.entity.DestinationAdaptorMapping;
import org.openphc.cce.intelligence.domain.entity.IntelligenceDelivery;
import org.openphc.cce.intelligence.domain.enums.ActionType;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.openphc.cce.intelligence.domain.enums.IntelligenceSeverity;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryRepository;
import org.openphc.cce.intelligence.kafka.model.IntelligenceTriggerEvent;
import org.openphc.cce.intelligence.service.IntelligenceDeliveryAuditService;
import org.openphc.cce.intelligence.webhook.WebhookDeliveryClient;
import org.openphc.cce.intelligence.webhook.WebhookResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@Component
public class ActionDispatcher {

    private final IntelligenceDeliveryRepository intelligenceDeliveryRepository;
    private final FhirPayloadBuilder fhirPayloadBuilder;
    private final WebhookDeliveryClient webhookDeliveryClient;
    private final IntelligenceDeliveryAuditService auditService;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public ActionDispatcher(IntelligenceDeliveryRepository intelligenceDeliveryRepository,
                            FhirPayloadBuilder fhirPayloadBuilder,
                            WebhookDeliveryClient webhookDeliveryClient,
                            IntelligenceDeliveryAuditService auditService,
                            MeterRegistry meterRegistry,
                            TransactionTemplate transactionTemplate) {
        this.intelligenceDeliveryRepository = intelligenceDeliveryRepository;
        this.fhirPayloadBuilder = fhirPayloadBuilder;
        this.webhookDeliveryClient = webhookDeliveryClient;
        this.auditService = auditService;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = transactionTemplate;
    }

    public void dispatch(IntelligenceTriggerEvent event, DestinationAdaptorMapping mapping) {
        UUID mappingId = mapping.getId();

        // Phase 1: Create delivery record in transaction
        IntelligenceDelivery delivery = transactionTemplate.execute(status -> {
            // Idempotency guard
            if (intelligenceDeliveryRepository.existsByIntelligenceEventIdAndDestinationAdaptorMappingId(
                    event.getIntelligenceEventId(), mappingId)) {
                log.debug("Delivery already exists for eventId={}, mappingId={}",
                        event.getIntelligenceEventId(), mappingId);
                return null;
            }

            IntelligenceDelivery d = IntelligenceDelivery.builder()
                    .intelligenceEventId(event.getIntelligenceEventId())
                    .actionDefinitionId(event.getActionDefinitionId())
                    .destinationAdaptorMappingId(mappingId)
                    .actionType(resolveActionType(event))
                    .status(IntelligenceDeliveryStatus.PENDING)
                    .subject(event.getSubject())
                    .protocolCanonical(event.getProtocolCanonical())
                    .actionId(event.getActionId())
                    .severity(IntelligenceSeverity.valueOf(event.getSeverity().toUpperCase()))
                    .destination(event.getIntelligenceDestination())
                    .fhirPayload(fhirPayloadBuilder.buildPayload(event, null))
                    .build();

            d = intelligenceDeliveryRepository.save(d);
            auditService.logEvent(d.getId(), "CREATED", null);

            // Rebuild payload with actual delivery ID and set to EXECUTING
            JsonNode payload = fhirPayloadBuilder.buildPayload(event, d.getId());
            d.setFhirPayload(payload);
            d.setStatus(IntelligenceDeliveryStatus.EXECUTING);
            d = intelligenceDeliveryRepository.save(d);
            auditService.logEvent(d.getId(), "DISPATCHED", null);

            return d;
        });

        if (delivery == null) {
            return; // Already dispatched (idempotent)
        }

        UUID deliveryId = delivery.getId();
        ActionType actionType = delivery.getActionType();

        // Record dispatched metric
        Counter.builder("cce.intelligence.deliveries.dispatched")
                .description("Deliveries dispatched to adaptors")
                .tag("action_type", actionType.name())
                .tag("severity", delivery.getSeverity().name())
                .register(meterRegistry)
                .increment();

        // Phase 2: HTTP call OUTSIDE transaction
        String endpointUrl = mapping.getReceiverAdaptor().getDefinition().get("address").asText();
        JsonNode adaptorConfig = mapping.getReceiverAdaptor().getConfig();

        WebhookResult result = webhookDeliveryClient.deliver(
                delivery.getFhirPayload(), endpointUrl, deliveryId, event.getIntelligenceEventId(), adaptorConfig);

        // Phase 3: Update delivery with result in transaction
        transactionTemplate.executeWithoutResult(status -> {
            IntelligenceDelivery d = intelligenceDeliveryRepository.findById(deliveryId).orElseThrow();
            d.setAttemptCount(result.getAttempts());
            d.setDeliveryResult(result.toJsonNode());

            if (result.isSuccess()) {
                d.setStatus(IntelligenceDeliveryStatus.DELIVERED);
                d.setDeliveredAt(OffsetDateTime.now());
                auditService.logEvent(deliveryId, "DELIVERED", result.toJsonNode());
                Counter.builder("cce.intelligence.deliveries.delivered")
                        .description("Successful deliveries")
                        .tag("action_type", actionType.name())
                        .register(meterRegistry)
                        .increment();
                log.info("Delivery successful: deliveryId={}, endpoint={}", deliveryId, endpointUrl);
            } else {
                d.setStatus(IntelligenceDeliveryStatus.FAILED);
                auditService.logEvent(deliveryId, "FAILED", result.toJsonNode());
                Counter.builder("cce.intelligence.deliveries.failed")
                        .description("Failed deliveries")
                        .tag("action_type", actionType.name())
                        .register(meterRegistry)
                        .increment();
                log.warn("Delivery failed: deliveryId={}, endpoint={}, error={}", deliveryId, endpointUrl, result.getErrorMessage());
            }

            intelligenceDeliveryRepository.save(d);
        });
    }

    private ActionType resolveActionType(IntelligenceTriggerEvent event) {
        String fhirKind = event.getActionType();
        String severity = event.getSeverity();

        if ("Task".equals(fhirKind) || "ServiceRequest".equals(fhirKind)) {
            return ActionType.COORDINATION;
        }
        if ("HIGH".equalsIgnoreCase(severity) || "CRITICAL".equalsIgnoreCase(severity)) {
            return ActionType.ESCALATION;
        }
        return ActionType.NOTIFICATION;
    }
}
