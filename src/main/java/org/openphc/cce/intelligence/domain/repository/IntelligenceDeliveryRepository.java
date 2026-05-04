package org.openphc.cce.intelligence.domain.repository;

import org.openphc.cce.intelligence.domain.entity.IntelligenceDelivery;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface IntelligenceDeliveryRepository extends JpaRepository<IntelligenceDelivery, UUID>, JpaSpecificationExecutor<IntelligenceDelivery> {

    boolean existsByIntelligenceEventIdAndDestinationAdaptorMappingId(UUID intelligenceEventId, UUID destinationAdaptorMappingId);

    List<IntelligenceDelivery> findByIntelligenceEventId(UUID intelligenceEventId);

    Page<IntelligenceDelivery> findByStatus(IntelligenceDeliveryStatus status, Pageable pageable);

    Page<IntelligenceDelivery> findBySubject(String subject, Pageable pageable);

    boolean existsByDestinationAdaptorMappingIdAndStatusIn(UUID destinationAdaptorMappingId, List<IntelligenceDeliveryStatus> statuses);
}
