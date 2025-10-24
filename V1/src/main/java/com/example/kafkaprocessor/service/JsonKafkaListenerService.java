package com.example.kafkaprocessor.service;

import com.example.kafkaprocessor.model.Command;
import com.example.kafkaprocessor.model.CommandEvent;
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
public class JsonKafkaListenerService extends AbstractSpringKafkaListener<CommandEvent<String>> {
    
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
        String message = record.value();
        try {
            CommandEvent<String> commandEvent = CommandEvent.<String>builder()
                .id(record.key())
                .eventData(message)
                .build();
            
            ConsumerRecord<String, CommandEvent<String>> wrappedRecord = new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                commandEvent
            );
            
            processMessage(wrappedRecord, acknowledgment)
                .doOnError(e -> log.error("Error processing JSON message: {}", e.getMessage(), e))
                .subscribe(
                    null, 
                    error -> {
                        log.error("Unhandled error in JSON message processing: {}", error.getMessage(), error);
                    }
                );
        } catch (Exception e) {
            log.error("Failed to wrap message into CommandEvent: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }

    @Override
    protected Mono<EventProcessorResult> receiveEvent(String applicationTraceId, CommandEvent<String> commandEvent) {
        log.info("Receiving JSON event for trace ID: {}", applicationTraceId);
        try {
            Command command = objectMapper.readValue(commandEvent.getEventData(), Command.class);
            log.debug("JSON message received for processing: {}", applicationTraceId);
            return Mono.just(EventProcessorResult.success(command));
        } catch (Exception e) {
            log.error("Error in receiveEvent for trace ID {}: {}", applicationTraceId, e.getMessage(), e);
            return Mono.just(EventProcessorResult.failure("Failed to parse JSON message: " + e.getMessage()));
        }
    }
}