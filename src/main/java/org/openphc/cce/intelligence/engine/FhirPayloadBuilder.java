package org.openphc.cce.intelligence.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.intelligence.kafka.model.IntelligenceTriggerEvent;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class FhirPayloadBuilder {

    private final ObjectMapper objectMapper;

    /**
     * Builds a FHIR R4-compliant payload from the trigger event.
     * CommunicationRequest → FHIR CommunicationRequest
     * Task → FHIR Task
     * ServiceRequest (with eventPayload) → passthrough
     */
    public JsonNode buildPayload(IntelligenceTriggerEvent event, UUID intelligenceDeliveryId) {
        String actionType = event.getActionType();

        // ServiceRequest: pass through the original event payload from the compliance service
        if ("ServiceRequest".equals(actionType) && event.getEventPayload() != null) {
            return event.getEventPayload();
        }
        if ("Task".equals(actionType)) {
            return buildTask(event, intelligenceDeliveryId);
        }
        return buildCommunicationRequest(event, intelligenceDeliveryId);
    }

    private JsonNode buildCommunicationRequest(IntelligenceTriggerEvent event, UUID deliveryId) {
        ObjectNode resource = objectMapper.createObjectNode();
        resource.put("resourceType", "CommunicationRequest");
        resource.put("status", "active");
        resource.put("priority", mapSeverityToFhirPriority(event.getSeverity()));

        // Identifier
        ArrayNode identifiers = resource.putArray("identifier");
        ObjectNode identifier = identifiers.addObject();
        identifier.put("system", "http://openphc.org/fhir/intelligence-delivery-id");
        identifier.put("value", deliveryId != null ? deliveryId.toString() : "pending");

        // Category
        ArrayNode categories = resource.putArray("category");
        ObjectNode category = categories.addObject();
        ArrayNode codings = category.putArray("coding");
        ObjectNode coding = codings.addObject();
        coding.put("system", "http://openphc.org/fhir/CodeSystem/cce-action-type");
        coding.put("code", resolveActionType(event));
        coding.put("display", resolveActionType(event));

        // Subject (FHIR identifier-based reference)
        ObjectNode subject = resource.putObject("subject");
        ObjectNode subjectIdentifier = subject.putObject("identifier");
        subjectIdentifier.put("system", "http://openphc.org/fhir/patient-upid");
        subjectIdentifier.put("value", event.getSubject());

        // About (protocol + action references)
        ArrayNode about = resource.putArray("about");
        ObjectNode aboutRef = about.addObject();
        aboutRef.put("reference", "PlanDefinition/" + event.getProtocolCanonical());
        ObjectNode aboutAction = about.addObject();
        aboutAction.put("display", event.getActionId());

        // Payload — human-readable summary
        ArrayNode payload = resource.putArray("payload");
        ObjectNode payloadContent = payload.addObject();
        payloadContent.put("contentString", buildSummary(event));

        // Recipient
        ArrayNode recipients = resource.putArray("recipient");
        ObjectNode recipient = recipients.addObject();
        recipient.put("display", event.getIntelligenceDestination());

        // AuthoredOn
        resource.put("authoredOn", OffsetDateTime.now().toString());

        // CCE Extensions
        addExtensions(resource, event);

        return resource;
    }

    private JsonNode buildTask(IntelligenceTriggerEvent event, UUID deliveryId) {
        ObjectNode resource = objectMapper.createObjectNode();
        resource.put("resourceType", "Task");
        resource.put("status", "requested");
        resource.put("intent", "order");
        resource.put("priority", mapSeverityToFhirPriority(event.getSeverity()));

        // Identifier
        ArrayNode identifiers = resource.putArray("identifier");
        ObjectNode identifier = identifiers.addObject();
        identifier.put("system", "http://openphc.org/fhir/intelligence-delivery-id");
        identifier.put("value", deliveryId != null ? deliveryId.toString() : "pending");

        // Code
        ObjectNode code = resource.putObject("code");
        ArrayNode codings = code.putArray("coding");
        ObjectNode coding = codings.addObject();
        coding.put("system", "http://openphc.org/fhir/CodeSystem/cce-action-type");
        coding.put("code", "Task");
        coding.put("display", "Task");

        // For (subject - FHIR identifier-based reference)
        ObjectNode forRef = resource.putObject("for");
        ObjectNode forIdentifier = forRef.putObject("identifier");
        forIdentifier.put("system", "http://openphc.org/fhir/patient-upid");
        forIdentifier.put("value", event.getSubject());

        // Description
        resource.put("description", buildSummary(event));

        // AuthoredOn
        resource.put("authoredOn", OffsetDateTime.now().toString());

        // CCE Extensions
        addExtensions(resource, event);

        return resource;
    }

    private JsonNode buildServiceRequest(IntelligenceTriggerEvent event, UUID deliveryId) {
        ObjectNode resource = objectMapper.createObjectNode();
        resource.put("resourceType", "ServiceRequest");
        resource.put("status", "active");
        resource.put("intent", "order");
        resource.put("priority", mapSeverityToFhirPriority(event.getSeverity()));

        // Identifier
        ArrayNode identifiers = resource.putArray("identifier");
        ObjectNode identifier = identifiers.addObject();
        identifier.put("system", "http://openphc.org/fhir/intelligence-delivery-id");
        identifier.put("value", deliveryId != null ? deliveryId.toString() : "pending");

        // Code
        ObjectNode code = resource.putObject("code");
        ArrayNode codings = code.putArray("coding");
        ObjectNode coding = codings.addObject();
        coding.put("system", "http://openphc.org/fhir/CodeSystem/cce-action-type");
        coding.put("code", "ServiceRequest");
        coding.put("display", "ServiceRequest");

        // Subject (FHIR identifier-based reference)
        ObjectNode subject = resource.putObject("subject");
        ObjectNode subjectIdentifier = subject.putObject("identifier");
        subjectIdentifier.put("system", "http://openphc.org/fhir/patient-upid");
        subjectIdentifier.put("value", event.getSubject());

        // Note / description
        ArrayNode notes = resource.putArray("note");
        ObjectNode note = notes.addObject();
        note.put("text", buildSummary(event));

        // AuthoredOn
        resource.put("authoredOn", OffsetDateTime.now().toString());

        // CCE Extensions
        addExtensions(resource, event);

        return resource;
    }

    private void addExtensions(ObjectNode resource, IntelligenceTriggerEvent event) {
        ArrayNode extensions = resource.putArray("extension");

        ObjectNode severityExt = extensions.addObject();
        severityExt.put("url", "http://openphc.org/fhir/StructureDefinition/cce-severity");
        severityExt.put("valueCode", event.getSeverity().toLowerCase());

        ObjectNode eventIdExt = extensions.addObject();
        eventIdExt.put("url", "http://openphc.org/fhir/StructureDefinition/cce-intelligence-event-id");
        eventIdExt.put("valueId", event.getIntelligenceEventId().toString());

        ObjectNode stepStateExt = extensions.addObject();
        stepStateExt.put("url", "http://openphc.org/fhir/StructureDefinition/cce-step-state");
        stepStateExt.put("valueCode", event.getStepState());
    }

    private String mapSeverityToFhirPriority(String severity) {
        return switch (severity.toUpperCase()) {
            case "LOW" -> "routine";
            case "MEDIUM", "HIGH" -> "urgent";
            case "CRITICAL" -> "asap";
            default -> "routine";
        };
    }

    private String resolveActionType(IntelligenceTriggerEvent event) {
        return event.getActionType();
    }

    private String buildSummary(IntelligenceTriggerEvent event) {
        return String.format("[%s] %s for patient %s — step %s (%s)",
                event.getSeverity(),
                resolveActionType(event),
                event.getSubject(),
                event.getActionId(),
                event.getProtocolCanonical());
    }
}
