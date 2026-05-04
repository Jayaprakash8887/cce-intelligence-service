package org.openphc.cce.intelligence.service;

import lombok.RequiredArgsConstructor;
import org.openphc.cce.intelligence.domain.entity.DestinationAdaptorMapping;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.openphc.cce.intelligence.domain.repository.DestinationAdaptorMappingRepository;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryRepository;
import org.openphc.cce.intelligence.domain.repository.ReceiverAdaptorRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DestinationAdaptorMappingService {

    private final DestinationAdaptorMappingRepository mappingRepository;
    private final ReceiverAdaptorRepository receiverAdaptorRepository;
    private final IntelligenceDeliveryRepository deliveryRepository;

    public List<DestinationAdaptorMapping> findAll() {
        return mappingRepository.findAll();
    }

    public List<DestinationAdaptorMapping> findFiltered(String destination, UUID receiverAdaptorId, String status) {
        if (destination == null && receiverAdaptorId == null && status == null) {
            return mappingRepository.findAll();
        }
        return mappingRepository.findByFilters(destination, receiverAdaptorId, status);
    }

    public Optional<DestinationAdaptorMapping> findById(UUID id) {
        return mappingRepository.findById(id);
    }

    @Transactional
    public DestinationAdaptorMapping create(String destination, UUID receiverAdaptorId) {
        if (!receiverAdaptorRepository.existsById(receiverAdaptorId)) {
            throw new IllegalArgumentException("Receiver adaptor not found: " + receiverAdaptorId);
        }

        if (mappingRepository.existsByDestination(destination)) {
            throw new IllegalArgumentException("Destination already mapped: " + destination);
        }

        DestinationAdaptorMapping mapping = DestinationAdaptorMapping.builder()
                .destination(destination)
                .receiverAdaptorId(receiverAdaptorId)
                .status("ACTIVE")
                .build();

        return mappingRepository.save(mapping);
    }

    @Transactional
    public DestinationAdaptorMapping update(UUID id, UUID receiverAdaptorId, String status) {
        DestinationAdaptorMapping mapping = mappingRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Destination adaptor mapping not found: " + id));

        if (receiverAdaptorId != null) {
            if (!receiverAdaptorRepository.existsById(receiverAdaptorId)) {
                throw new IllegalArgumentException("Receiver adaptor not found: " + receiverAdaptorId);
            }
            mapping.setReceiverAdaptorId(receiverAdaptorId);
        }

        if (status != null) {
            mapping.setStatus(status);
        }

        return mappingRepository.save(mapping);
    }

    @Transactional
    public void delete(UUID id) {
        DestinationAdaptorMapping mapping = mappingRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Destination adaptor mapping not found: " + id));

        boolean hasActiveDeliveries = deliveryRepository.existsByDestinationAdaptorMappingIdAndStatusIn(
                id, List.of(IntelligenceDeliveryStatus.PENDING, IntelligenceDeliveryStatus.EXECUTING));

        if (hasActiveDeliveries) {
            throw new IllegalStateException("Cannot delete mapping with active (PENDING/EXECUTING) intelligence deliveries");
        }

        mappingRepository.delete(mapping);
    }
}
