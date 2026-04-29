package org.openphc.cce.intelligence.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "cce.intelligence")
@Getter
@Setter
public class IntelligenceProperties {

    private Webhook webhook = new Webhook();

    @Getter
    @Setter
    public static class Webhook {
        private int connectTimeoutMs = 5000;
        private int readTimeoutMs = 10000;
        private int retryAttempts = 3;
        private int retryIntervalMs = 2000;
    }
}
