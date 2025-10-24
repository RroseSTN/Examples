package com.example.kafkaprocessor.service;

import com.example.kafkaprocessor.model.Command;
import com.example.kafkaprocessor.model.CommandEvent;
import com.example.kafkaprocessor.model.EventProcessorResult;
import org.springframework.stereotype.Service;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

@Service("jsonListenerJ")
@Log4j2
public class JsonListenerJ extends JsonKafkaListenerService {
    
    @Override
    protected Mono<EventProcessorResult> validateEvent(String applicationTraceId, Command command) {
        if (command.getPayload() == null || String.valueOf(command.getPayload()).trim().isEmpty()) {
            return Mono.just(EventProcessorResult.failure("Payload cannot be empty"));
        }
        
        if (command.getMetadata() == null || command.getMetadata().isEmpty()) {
            return Mono.just(EventProcessorResult.failure("Metadata cannot be empty"));
        }
        
        return Mono.just(EventProcessorResult.success(command));
    }

    @Override
    protected Mono<EventProcessorResult> processEvent(String applicationTraceId, Command command) {
        command.setMetadata(command.getMetadata() + "_processed_by_J");
        return Mono.just(EventProcessorResult.success(command));
    }
}