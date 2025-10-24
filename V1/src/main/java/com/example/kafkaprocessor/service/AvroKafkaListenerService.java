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
import org.springframework.beans.factory.annotation.Autowired;
import org.apache.avro.generic.GenericRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@Log4j2
public class AvroKafkaListenerService extends AbstractSpringKafkaListener<CommandEvent<byte[]>> {
    
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
        try {
            byte[] avroBytes = record.value().toString().getBytes();
            CommandEvent<byte[]> commandEvent = CommandEvent.<byte[]>builder()
                .id(record.key())
                .eventData(avroBytes)
                .build();
            
            ConsumerRecord<String, CommandEvent<byte[]>> wrappedRecord = new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                commandEvent
            );
            
            processMessage(wrappedRecord, acknowledgment)
                .doOnError(e -> log.error("Error processing AVRO message: {}", e.getMessage(), e))
                .subscribe();
        } catch (Exception e) {
            log.error("Failed to wrap message into CommandEvent: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }

    @Override
    protected Mono<EventProcessorResult> receiveEvent(String applicationTraceId, CommandEvent<byte[]> commandEvent) {
        log.info("Receiving AVRO event for trace ID: {}", applicationTraceId);
        try {
            String message = new String(commandEvent.getEventData());
            Command command = objectMapper.readValue(message, Command.class);
            log.debug("AVRO message received for processing: {}", applicationTraceId);
            return Mono.just(EventProcessorResult.success(command));
        } catch (Exception e) {
            log.error("Error in receiveEvent for trace ID {}: {}", applicationTraceId, e.getMessage(), e);
            return Mono.just(EventProcessorResult.failure("Failed to parse AVRO message: " + e.getMessage()));
        }
    }
}