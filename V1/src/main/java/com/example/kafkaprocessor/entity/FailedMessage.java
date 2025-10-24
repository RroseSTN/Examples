package com.example.kafkaprocessor.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Column;
import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.time.LocalDateTime;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FailedMessage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String topic;
    private Integer partition;
    private Long offset;
    private String key;
    
    @Column(columnDefinition = "TEXT")
    private String value;
    
    @Column(length = 4000)
    private String errorMessage;
    
    private LocalDateTime failureTime;
    private Integer retryCount;
    private String applicationTraceId;
}