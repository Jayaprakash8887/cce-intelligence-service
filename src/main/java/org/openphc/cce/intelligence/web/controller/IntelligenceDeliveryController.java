package org.openphc.cce.intelligence.web.controller;

import lombok.RequiredArgsConstructor;
import org.openphc.cce.intelligence.domain.entity.IntelligenceDelivery;
import org.openphc.cce.intelligence.domain.entity.IntelligenceDeliveryAuditLog;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.openphc.cce.intelligence.service.IntelligenceDeliveryAuditService;
import org.openphc.cce.intelligence.service.IntelligenceDeliveryService;
import org.openphc.cce.intelligence.web.dto.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/intelligence-deliveries")
@RequiredArgsConstructor
public class IntelligenceDeliveryController {

    private final IntelligenceDeliveryService deliveryService;
    private final IntelligenceDeliveryAuditService auditService;

    @GetMapping
    public ApiResponse<List<IntelligenceDeliverySummaryDto>> list(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String subject,
            @RequestParam(required = false) UUID intelligenceEventId,
            @RequestParam(required = false) UUID actionDefinitionId,
            @RequestParam(required = false) String actionType,
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) String destination,
            @RequestParam(required = false) UUID protocolDefinitionId,
            Pageable pageable) {

        Page<IntelligenceDelivery> page = deliveryService.findFiltered(
                status, subject, intelligenceEventId, actionDefinitionId,
                actionType, severity, destination, protocolDefinitionId, pageable);

        List<IntelligenceDeliverySummaryDto> data = page.getContent().stream()
                .map(this::toSummaryDto).toList();

        ApiResponse.PaginationInfo pagination = ApiResponse.PaginationInfo.builder()
                .page(page.getNumber())
                .size(page.getSize())
                .totalElements(page.getTotalElements())
                .totalPages(page.getTotalPages())
                .build();

        return ApiResponse.of(data, pagination);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<IntelligenceDeliveryDto>> getById(@PathVariable UUID id) {
        return deliveryService.findById(id)
                .map(this::toDto)
                .map(dto -> ResponseEntity.ok(ApiResponse.of(dto)))
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}/audit")
    public ResponseEntity<ApiResponse<List<IntelligenceDeliveryAuditLogDto>>> getAudit(@PathVariable UUID id) {
        if (deliveryService.findById(id).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        List<IntelligenceDeliveryAuditLogDto> audit = auditService.getAuditTrail(id).stream()
                .map(this::toAuditDto)
                .toList();
        return ResponseEntity.ok(ApiResponse.of(audit));
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<ApiResponse<IntelligenceDeliveryDto>> cancel(@PathVariable UUID id) {
        IntelligenceDelivery delivery = deliveryService.cancel(id);
        return ResponseEntity.ok(ApiResponse.of(toDto(delivery)));
    }

    private IntelligenceDeliveryDto toDto(IntelligenceDelivery d) {
        return IntelligenceDeliveryDto.builder()
                .id(d.getId())
                .intelligenceEventId(d.getIntelligenceEventId())
                .actionDefinitionId(d.getActionDefinitionId())
                .destinationAdaptorMappingId(d.getDestinationAdaptorMappingId())
                .actionType(d.getActionType().name())
                .status(d.getStatus().name())
                .subject(d.getSubject())
                .protocolCanonical(d.getProtocolCanonical())
                .actionId(d.getActionId())
                .severity(d.getSeverity().name())
                .destination(d.getDestination())
                .fhirPayload(d.getFhirPayload())
                .deliveryResult(d.getDeliveryResult())
                .attemptCount(d.getAttemptCount())
                .createdAt(d.getCreatedAt())
                .updatedAt(d.getUpdatedAt())
                .deliveredAt(d.getDeliveredAt())
                .build();
    }

    private IntelligenceDeliverySummaryDto toSummaryDto(IntelligenceDelivery d) {
        return IntelligenceDeliverySummaryDto.builder()
                .id(d.getId())
                .intelligenceEventId(d.getIntelligenceEventId())
                .actionType(d.getActionType().name())
                .actionId(d.getActionId())
                .destination(d.getDestination())
                .destinationAdaptorMappingId(d.getDestinationAdaptorMappingId())
                .status(d.getStatus().name())
                .subject(d.getSubject())
                .protocolCanonical(d.getProtocolCanonical())
                .severity(d.getSeverity().name())
                .attemptCount(d.getAttemptCount())
                .createdAt(d.getCreatedAt())
                .deliveredAt(d.getDeliveredAt())
                .build();
    }

    private IntelligenceDeliveryAuditLogDto toAuditDto(IntelligenceDeliveryAuditLog a) {
        return IntelligenceDeliveryAuditLogDto.builder()
                .id(a.getId())
                .intelligenceDeliveryId(a.getIntelligenceDeliveryId())
                .eventType(a.getEventType())
                .actor(a.getActor())
                .details(a.getDetails())
                .timestamp(a.getTimestamp())
                .build();
    }
}
