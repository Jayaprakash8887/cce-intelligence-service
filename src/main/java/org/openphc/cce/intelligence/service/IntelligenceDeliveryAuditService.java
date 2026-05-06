package org.openphc.cce.intelligence.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.intelligence.domain.entity.IntelligenceDeliveryAuditLog;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryAuditLogRepository;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class IntelligenceDeliveryAuditService {

    private final IntelligenceDeliveryAuditLogRepository auditLogRepository;

    public void logEvent(UUID intelligenceDeliveryId, String eventType, JsonNode details) {
        try {
            IntelligenceDeliveryAuditLog auditLog = IntelligenceDeliveryAuditLog.builder()
                    .intelligenceDeliveryId(intelligenceDeliveryId)
                    .eventType(eventType)
                    .actor("system")
                    .details(details)
                    .timestamp(OffsetDateTime.now())
                    .build();

            auditLogRepository.save(auditLog);
            log.debug("Audit log: deliveryId={}, event={}", intelligenceDeliveryId, eventType);
        } catch (Exception e) {
            log.warn("Failed to write audit log: deliveryId={}, event={}, error={}",
                    intelligenceDeliveryId, eventType, e.getMessage());
        }
    }

    public List<IntelligenceDeliveryAuditLog> getAuditTrail(UUID intelligenceDeliveryId) {
        return auditLogRepository.findByIntelligenceDeliveryIdOrderByTimestampDesc(intelligenceDeliveryId);
    }
}
