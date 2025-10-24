package com.example.kafkaprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaMessageProcessorApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaMessageProcessorApplication.class, args);
    }
}