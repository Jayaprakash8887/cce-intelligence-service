package org.openphc.cce.intelligence.domain.repository;

import org.openphc.cce.intelligence.domain.entity.DestinationAdaptorMapping;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface DestinationAdaptorMappingRepository extends JpaRepository<DestinationAdaptorMapping, UUID> {

    Optional<DestinationAdaptorMapping> findByDestinationAndStatus(String destination, String status);

    List<DestinationAdaptorMapping> findByReceiverAdaptorId(UUID receiverAdaptorId);

    boolean existsByReceiverAdaptorIdAndStatus(UUID receiverAdaptorId, String status);

    boolean existsByDestination(String destination);

    long countByStatus(String status);

    @Query("""
        SELECT m FROM DestinationAdaptorMapping m
        WHERE (:destination IS NULL OR m.destination = :destination)
          AND (:receiverAdaptorId IS NULL OR m.receiverAdaptorId = :receiverAdaptorId)
          AND (:status IS NULL OR m.status = :status)
        """)
    List<DestinationAdaptorMapping> findByFilters(
            @Param("destination") String destination,
            @Param("receiverAdaptorId") UUID receiverAdaptorId,
            @Param("status") String status);
}
