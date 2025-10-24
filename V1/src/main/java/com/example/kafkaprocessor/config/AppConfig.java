package com.example.kafkaprocessor.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "app.processing")
@Getter
@Setter
public class AppConfig {
    private MessageTrackingConfig messageTracking;
    private MetricsConfig metrics;
    
    @Getter
    @Setter
    public static class MessageTrackingConfig {
        private boolean enabled;
    }
    
    @Getter
    @Setter
    public static class MetricsConfig {
        private int reportingInterval;
    }
}