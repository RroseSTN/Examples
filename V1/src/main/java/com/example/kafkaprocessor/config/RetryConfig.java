package com.example.kafkaprocessor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "app.processing.retry")
@Data
public class RetryConfig {
    private int attempts;
    private long maxDuration;
    private BackoffConfig backoff;
    private TopicConfig topic;

    @Data
    public static class BackoffConfig {
        private long initialInterval;
        private double multiplier;
        private long maxInterval;
    }

    @Data
    public static class TopicConfig {
        private String retrySuffix;
        private String dltSuffix;
        private ProvisioningConfig provisioning;
    }

    @Data
    public static class ProvisioningConfig {
        private boolean enabled;
        private int partitions;
        private int replicas;
    }
}