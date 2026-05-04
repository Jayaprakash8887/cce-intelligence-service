package org.openphc.cce.intelligence.engine;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.intelligence.domain.entity.DestinationAdaptorMapping;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryRepository;
import org.openphc.cce.intelligence.kafka.model.IntelligenceTriggerEvent;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class IntelligenceEngine {

    private final DestinationRouter destinationRouter;
    private final FhirPayloadBuilder fhirPayloadBuilder;
    private final ActionDispatcher actionDispatcher;
    private final IntelligenceDeliveryRepository intelligenceDeliveryRepository;

    public void processTrigger(IntelligenceTriggerEvent event) {
        UUID intelligenceEventId = event.getIntelligenceEventId();

        // 1. Resolve destination adaptor mapping (1:1)
        Optional<DestinationAdaptorMapping> mapping = destinationRouter.resolveAdaptor(
                event.getIntelligenceDestination());

        if (mapping.isEmpty()) {
            log.warn("No active adaptor mapping for trigger: eventId={}, destination={}",
                    intelligenceEventId, event.getIntelligenceDestination());
            return;
        }

        DestinationAdaptorMapping adaptorMapping = mapping.get();

        // 2. Idempotency check
        if (intelligenceDeliveryRepository.existsByIntelligenceEventIdAndDestinationAdaptorMappingId(
                intelligenceEventId, adaptorMapping.getId())) {
            log.info("Delivery already exists for eventId={}, mappingId={}",
                    intelligenceEventId, adaptorMapping.getId());
            return;
        }

        // 3. Single dispatch
        try {
            actionDispatcher.dispatch(event, adaptorMapping);
            log.info("Dispatch complete: eventId={}, destination={}", intelligenceEventId, event.getIntelligenceDestination());
        } catch (Exception e) {
            log.error("Dispatch failed for eventId={}, mappingId={}",
                    intelligenceEventId, adaptorMapping.getId(), e);
        }
    }
}
