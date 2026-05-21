package org.openphc.cce.intelligence.service;

import lombok.RequiredArgsConstructor;
import org.openphc.cce.intelligence.domain.entity.IntelligenceDelivery;
import org.openphc.cce.intelligence.domain.enums.ActionType;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.openphc.cce.intelligence.domain.enums.IntelligenceSeverity;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class IntelligenceDeliveryService {

    private final IntelligenceDeliveryRepository deliveryRepository;
    private final IntelligenceDeliveryAuditService auditService;

    public Page<IntelligenceDelivery> findAll(Pageable pageable) {
        return deliveryRepository.findAll(pageable);
    }

    public Page<IntelligenceDelivery> findByStatus(IntelligenceDeliveryStatus status, Pageable pageable) {
        return deliveryRepository.findByStatus(status, pageable);
    }

    public Page<IntelligenceDelivery> findBySubject(String subject, Pageable pageable) {
        return deliveryRepository.findBySubject(subject, pageable);
    }

    public Page<IntelligenceDelivery> findFiltered(String status, String subject, UUID intelligenceEventId,
                                                    UUID actionDefinitionId, String actionType, String severity,
                                                    String destination, UUID protocolDefinitionId, Pageable pageable) {
        Specification<IntelligenceDelivery> spec = Specification.where(null);

        if (status != null) {
            IntelligenceDeliveryStatus deliveryStatus = IntelligenceDeliveryStatus.valueOf(status.toUpperCase());
            spec = spec.and((root, query, cb) -> cb.equal(root.get("status"), deliveryStatus));
        }
        if (subject != null) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("subject"), subject));
        }
        if (intelligenceEventId != null) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("intelligenceEventId"), intelligenceEventId));
        }
        if (actionDefinitionId != null) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("actionDefinitionId"), actionDefinitionId));
        }
        if (actionType != null) {
            ActionType type = ActionType.valueOf(actionType);
            spec = spec.and((root, query, cb) -> cb.equal(root.get("actionType"), type));
        }
        if (severity != null) {
            IntelligenceSeverity sev = IntelligenceSeverity.valueOf(severity.toUpperCase());
            spec = spec.and((root, query, cb) -> cb.equal(root.get("severity"), sev));
        }
        if (destination != null) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("destination"), destination));
        }
        if (protocolDefinitionId != null) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("destinationAdaptorMapping").get("destination"), protocolDefinitionId.toString()));
        }

        return deliveryRepository.findAll(spec, pageable);
    }

    public Optional<IntelligenceDelivery> findById(UUID id) {
        return deliveryRepository.findById(id);
    }

    @Transactional
    public IntelligenceDelivery cancel(UUID id) {
        IntelligenceDelivery delivery = deliveryRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Intelligence delivery not found: " + id));

        if (delivery.getStatus() != IntelligenceDeliveryStatus.PENDING
                && delivery.getStatus() != IntelligenceDeliveryStatus.FAILED) {
            throw new IllegalStateException(
                    "Only PENDING or FAILED deliveries can be cancelled. Current status: " + delivery.getStatus());
        }

        delivery.setStatus(IntelligenceDeliveryStatus.CANCELLED);
        delivery = deliveryRepository.save(delivery);
        auditService.logEvent(id, "CANCELLED", null);
        return delivery;
    }
}
