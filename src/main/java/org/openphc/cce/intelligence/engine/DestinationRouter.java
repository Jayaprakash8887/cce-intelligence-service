package org.openphc.cce.intelligence.engine;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.intelligence.domain.entity.DestinationAdaptorMapping;
import org.openphc.cce.intelligence.domain.repository.DestinationAdaptorMappingRepository;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class DestinationRouter {

    private final DestinationAdaptorMappingRepository destinationAdaptorMappingRepository;

    /**
     * Resolves the active adaptor mapping for a given destination.
     * Returns a single mapping (1:1 relationship between destination and adaptor).
     */
    public Optional<DestinationAdaptorMapping> resolveAdaptor(String destination) {
        Optional<DestinationAdaptorMapping> mapping = destinationAdaptorMappingRepository
                .findByDestinationAndStatus(destination, "ACTIVE");

        if (mapping.isEmpty()) {
            log.warn("No active adaptor mapping for destination={}", destination);
        }

        return mapping;
    }
}
