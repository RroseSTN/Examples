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
import org.springframework.beans.factory.annotation.Autowired;
import org.apache.avro.generic.GenericRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@Log4j2
public class AvroKafkaListenerService extends AbstractSpringKafkaListener {
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @KafkaListener(
        topics = "#{@avroKafkaTopicName}",
        containerFactory = "avroKafkaListenerContainerFactory",
        concurrency = "#{@avroKafkaListenerConcurrency}"
    )
    public void consumeMessage(ConsumerRecord<String, GenericRecord> record, 
                           Acknowledgment acknowledgment,
                           Consumer<?, ?> consumer) {
        // Convert AVRO record to JSON string for processing
        String message = record.value().toString();
        processMessage(
            new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                message
            ),
            acknowledgment
        )
        .doOnError(e -> log.error("Error processing AVRO message: {}", e.getMessage(), e))
        .subscribe();
    }

    @Override
    public Mono<EventProcessorResult> receiveEvent(String applicationTraceId, String message) {
        try {
            Command command = objectMapper.readValue(message, Command.class);
            log.debug("AVRO message received for processing: {}", applicationTraceId);
            return Mono.just(EventProcessorResult.success(command));
        } catch (Exception e) {
            log.error("Failed to parse AVRO message: {}", e.getMessage());
            return Mono.just(EventProcessorResult.failure("Failed to parse AVRO message: " + e.getMessage()));
        }
    }

    @Override
    public Mono<EventProcessorResult> executeReceive(String applicationTraceId, Command command) {
        log.info("Executing receive for AVRO message: {}", applicationTraceId);
        return Mono.just(EventProcessorResult.success(command));
    }

    @Override
    public Mono<EventProcessorResult> acceptEvent(String applicationTraceId, Command command) {
        log.info("Accepting AVRO event: {}", applicationTraceId);
        return Mono.just(EventProcessorResult.success(command));
    }

    @Override
    public Mono<EventProcessorResult> processEvent(String applicationTraceId, Command command) {
        log.info("Processing AVRO event: {}", applicationTraceId);
        return Mono.just(EventProcessorResult.success(command));
    }

    @Override
    public Mono<EventProcessorResult> validateEvent(String applicationTraceId, Command command) {
        log.info("Validating AVRO event: {}", applicationTraceId);
        return Mono.just(EventProcessorResult.success(command));
    }
}