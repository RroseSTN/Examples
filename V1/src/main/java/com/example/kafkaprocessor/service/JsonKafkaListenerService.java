package com.example.kafkaprocessor.service;

import com.example.kafkaprocessor.model.Command;
import com.example.kafkaprocessor.model.EventProcessorResult;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.apache.kafka.clients.consumer.Consumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;

@Service
@Log4j2
public class JsonKafkaListenerService extends AbstractSpringKafkaListener {
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @KafkaListener(
        topics = "#{@jsonKafkaTopicName}",
        containerFactory = "jsonKafkaListenerContainerFactory",
        concurrency = "#{@jsonKafkaListenerConcurrency}"
    )
    public void consumeMessage(ConsumerRecord<String, String> record, 
                             Acknowledgment acknowledgment, 
                             Consumer<?, ?> consumer) {
        processMessage(record, acknowledgment)
            .doOnError(e -> log.error("Error processing JSON message: {}", e.getMessage(), e))
            .subscribe(
                null, 
                error -> {
                    // Log the error but don't rethrow - let the error handling in processMessage handle retries
                    log.error("Unhandled error in JSON message processing: {}", error.getMessage(), error);
                }
            );
    }

    @Override
    public Mono<EventProcessorResult> receiveEvent(String applicationTraceId, String message) {
        try {
            Command command = objectMapper.readValue(message, Command.class);
            return Mono.just(EventProcessorResult.success(command));
        } catch (Exception e) {
            log.error("Failed to parse JSON message: {}", e.getMessage());
            return Mono.just(EventProcessorResult.failure("Failed to parse JSON message: " + e.getMessage()));
        }
    }

    @Override
    public Mono<EventProcessorResult> executeReceive(String applicationTraceId, Command command) {
        log.info("Executing receive for JSON message: {}", applicationTraceId);
        return Mono.just(EventProcessorResult.success(command));
    }

    @Override
    public Mono<EventProcessorResult> acceptEvent(String applicationTraceId, Command command) {
        log.info("Accepting JSON event: {}", applicationTraceId);
        return Mono.just(EventProcessorResult.success(command));
    }

    @Override
    public Mono<EventProcessorResult> processEvent(String applicationTraceId, Command command) {
        log.info("Processing JSON event: {}", applicationTraceId);
        return Mono.just(EventProcessorResult.success(command));
    }

    @Override
    public Mono<EventProcessorResult> validateEvent(String applicationTraceId, Command command) {
        log.info("Validating JSON event: {}", applicationTraceId);
        return Mono.just(EventProcessorResult.success(command));
    }
}