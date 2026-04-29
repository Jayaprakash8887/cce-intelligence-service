package org.openphc.cce.intelligence.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.openphc.cce.intelligence.domain.repository.ChannelSubscriptionRepository;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    public MetricsConfig(MeterRegistry meterRegistry, ChannelSubscriptionRepository subscriptionRepository) {
        Gauge.builder("cce.intelligence.subscriptions.active", subscriptionRepository,
                        repo -> repo.countByStatus("ACTIVE"))
                .description("Number of active channel subscriptions")
                .register(meterRegistry);
    }
}
