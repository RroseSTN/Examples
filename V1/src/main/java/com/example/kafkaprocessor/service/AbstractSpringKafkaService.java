package com.example.kafkaprocessor.service;

import com.example.kafkaprocessor.model.Command;
import com.example.kafkaprocessor.model.EventProcessorResult;
import com.example.kafkaprocessor.repository.DBRepository;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public abstract class AbstractSpringKafkaService {
    
    @Value("${kafka.topic.partitions}")
    private int partitionCount;
    
    @Value("${kafka.topic.route-away.shutdown-timeout}")
    private int shutdownTimeoutMs;
    
    // Removed unused retry configuration fields since we're using network-level retry
    
    @Autowired
    private DBRepository dbRepository;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    
    private final AtomicInteger messageProcessingCount = new AtomicInteger(0);
    private final AtomicBoolean routeAwayFlag = new AtomicBoolean(false);
    
    @PostConstruct
    public void init() {
        // Check initial route away flag state
        dbRepository.getRouteAwayFlag().ifPresent(flag -> {
            routeAwayFlag.set(flag.isFlag());
            if (flag.isFlag()) {
                log.warn("Starting with RouteAwayFlag=true - Message consumption will be paused");
                pauseAllContainers();
            } else {
                log.info("Starting with RouteAwayFlag=false - Message consumption enabled");
            }
        });
        
        log.info("Kafka message processor initialized with:");
        log.info("- Shutdown timeout: {} ms", shutdownTimeoutMs);
        log.info("- Manual commit mode enabled");
        log.info("- Network-level retry enabled");
        log.info("- Initial RouteAwayFlag: {}", routeAwayFlag.get());
    }
    
    private void pauseAllContainers() {
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container -> {
            container.pause();
            log.info("Container {} paused due to RouteAwayFlag=true", container.getListenerId());
        });
    }
    
    private void resumeAllContainers() {
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container -> {
            container.resume();
            log.info("Container {} resumed due to RouteAwayFlag=false", container.getListenerId());
        });
    }
    
    // No RetryableTopic annotation as we handle network retries at the container level
    @KafkaListener(
            topics = "#{@kafkaTopicName}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "#{@kafkaListenerConcurrency}"
    )
    public Mono<Void> processMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        // Log partition assignment when processing starts
        log.debug("Processing message from Topic: {}, Partition: {}, Offset: {}", 
                record.topic(), record.partition(), record.offset());
                
        if (routeAwayFlag.get()) {
            log.info("Route away flag is true, skipping message processing");
            return Mono.empty();
        }

        return Mono.defer(() -> {
            messageProcessingCount.incrementAndGet();
            String applicationTraceId = record.key();
            String message = record.value();
            
            return receiveEvent(applicationTraceId, message)
                    .flatMap(result -> {
                        if ("Success".equals(result.getEventProcessorResultStatus())) {
                            return executeReceive(applicationTraceId, result.getCommand());
                        }
                        return Mono.just(result);
                    })
                    .flatMap(result -> {
                        if ("Success".equals(result.getEventProcessorResultStatus())) {
                            return acceptEvent(applicationTraceId, result.getCommand());
                        }
                        return Mono.just(result);
                    })
                    .flatMap(result -> {
                        if ("Success".equals(result.getEventProcessorResultStatus())) {
                            return processEvent(applicationTraceId, result.getCommand());
                        }
                        return Mono.just(result);
                    })
                    .flatMap(result -> {
                        if ("Success".equals(result.getEventProcessorResultStatus())) {
                            return validateEvent(applicationTraceId, result.getCommand());
                        }
                        return Mono.just(result);
                    })
                    .map(result -> {
                        if ("Success".equals(result.getEventProcessorResultStatus())) {
                            acknowledgment.acknowledge();
                            log.debug("Message acknowledged for partition {} offset {}", 
                                    record.partition(), record.offset());
                        }
                        return result;
                    })
                    .doFinally(signalType -> {
                        int remainingMessages = messageProcessingCount.decrementAndGet();
                        // If route away flag is false and this was the last message, resume consumption
                        if (remainingMessages == 0 && !routeAwayFlag.get()) {
                            log.info("All in-flight messages completed - Resuming consumers");
                            resumeAllContainers();
                        }
                    })
                    .onErrorResume(e -> {
                        messageProcessingCount.decrementAndGet();
                        log.error("Error in message processing: {}", e.getMessage(), e);
                        
                        // Store failed message in database after all retries are exhausted
                        dbRepository.saveFailedMessage(record, e.getMessage(), 
                            record.headers().lastHeader("kafka_retryCount") != null ? 
                                Integer.parseInt(new String(record.headers().lastHeader("kafka_retryCount").value())) : 0,
                            applicationTraceId);
                            
                        return Mono.empty();
                    })
                    .then();
        });
    }
    
    @Scheduled(fixedRateString = "${kafka.topic.route-away.check-interval}")
    public void checkRouteAwayFlag() {
        dbRepository.getRouteAwayFlag()
                .ifPresent(flag -> {
                    boolean oldValue = routeAwayFlag.get();
                    boolean newValue = flag.isFlag();
                    
                    if (oldValue != newValue) {
                        routeAwayFlag.set(newValue);
                        
                        log.info("Route away flag state change detected:");
                        log.info("  - Previous state: {}", oldValue);
                        log.info("  - New state: {}", newValue);
                        log.info("  - Last updated: {}", flag.getLastUpdated());
                        log.info("  - Current in-flight messages: {}", messageProcessingCount.get());
                        
                        if (newValue) {
                            log.warn("Route away flag changed to TRUE - Pausing all consumers");
                            pauseAllContainers();
                        } else {
                            log.info("Route away flag changed to FALSE - Resuming all consumers");
                            if (messageProcessingCount.get() == 0) {
                                resumeAllContainers();
                            } else {
                                log.warn("Delaying consumer resume until in-flight messages complete. Count: {}", 
                                        messageProcessingCount.get());
                            }
                        }
                    }
                });
    }
    
    private void logCurrentAssignment(MessageListenerContainer container) {
        log.info("Container {} status - ID: {}, Running: {}, Paused: {}", 
            container.getListenerId(),
            container.getGroupId(),
            container.isRunning(),
            container.isContainerPaused());
    }
    
    @Scheduled(fixedRateString = "${app.processing.metrics.reporting-interval}")
    public void reportKafkaMetrics() {
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container -> {
            log.info("=== Kafka Consumer Status Report ===");
            log.info("Container ID: {}", container.getListenerId());
            log.info("Consumer Group: {}", container.getGroupId());
            log.info("Status: {}", container.isRunning() ? "RUNNING" : "STOPPED");
            log.info("Paused: {}", container.isContainerPaused());
            log.info("Route Away Flag: {}", routeAwayFlag.get());
            log.info("In-flight Messages: {}", messageProcessingCount.get());
            logCurrentAssignment(container);
            log.info("=====================================");
        });
    }
    
    // Abstract methods to be implemented by concrete classes
    public abstract Mono<EventProcessorResult> receiveEvent(String applicationTraceId, String message);
    public abstract Mono<EventProcessorResult> executeReceive(String applicationTraceId, Command command);
    public abstract Mono<EventProcessorResult> acceptEvent(String applicationTraceId, Command command);
    public abstract Mono<EventProcessorResult> processEvent(String applicationTraceId, Command command);
    public abstract Mono<EventProcessorResult> validateEvent(String applicationTraceId, Command command);
}