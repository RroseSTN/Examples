package com.example.kafkaprocessor.model;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventProcessorResult {
    private String eventProcessorResultStatus;
    private String message;
    private Command command;
    
    public static EventProcessorResult success(Command command) {
        return EventProcessorResult.builder()
                .eventProcessorResultStatus("Success")
                .command(command)
                .build();
    }
    
    public static EventProcessorResult failure(String message) {
        return EventProcessorResult.builder()
                .eventProcessorResultStatus("Failure")
                .message(message)
                .build();
    }
}