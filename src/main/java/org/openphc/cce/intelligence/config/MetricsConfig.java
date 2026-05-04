package org.openphc.cce.intelligence.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.openphc.cce.intelligence.domain.repository.DestinationAdaptorMappingRepository;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    public MetricsConfig(MeterRegistry meterRegistry, DestinationAdaptorMappingRepository mappingRepository) {
        Gauge.builder("cce.intelligence.destinations.active", mappingRepository,
                        repo -> repo.countByStatus("ACTIVE"))
                .description("Number of active destination adaptor mappings")
                .register(meterRegistry);
    }
}
