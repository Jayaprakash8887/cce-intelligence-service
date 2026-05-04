package org.openphc.cce.intelligence.domain.repository;

import org.openphc.cce.intelligence.domain.entity.IntelligenceDeliveryAuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface IntelligenceDeliveryAuditLogRepository extends JpaRepository<IntelligenceDeliveryAuditLog, UUID> {

    List<IntelligenceDeliveryAuditLog> findByIntelligenceDeliveryIdOrderByTimestampDesc(UUID intelligenceDeliveryId);
}
