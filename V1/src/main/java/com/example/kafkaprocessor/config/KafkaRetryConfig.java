package com.example.kafkaprocessor.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@RequiredArgsConstructor
public class KafkaRetryConfig {
    
    private final RetryConfig retryConfig;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Bean
    public RetryTopicConfiguration retryTopicConfiguration() {
        return RetryTopicConfigurationBuilder
                .newInstance()
                .maxAttempts(retryConfig.getAttempts())
                .fixedBackOff(retryConfig.getBackoff().getInitialInterval())
                .retryTopicSuffix(retryConfig.getTopic().getRetrySuffix())
                .dltSuffix(retryConfig.getTopic().getDltSuffix())
                .useSingleTopicForSameIntervals()
                .doNotAutoCreateRetryTopics()
                .create(kafkaTemplate);
    }

    @Bean
    public JsonMessageConverter jsonMessageConverter() {
        return new JsonMessageConverter();
    }
}