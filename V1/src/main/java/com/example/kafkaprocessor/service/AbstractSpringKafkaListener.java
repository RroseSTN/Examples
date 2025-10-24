package com.example.kafkaprocessor.service;

import com.example.kafkaprocessor.model.Command;
import com.example.kafkaprocessor.model.CommandEvent;
import com.example.kafkaprocessor.model.EventProcessorResult;
import com.example.kafkaprocessor.repository.DBRepository;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public abstract class AbstractSpringKafkaListener<T> {

    @Value("${kafka.topic.partitions}")
    private int partitionCount;

    @Value("${kafka.topic.route-away.shutdown-timeout}")
    private int shutdownTimeoutMs;

    @Value("${kafka.topic.route-away.inflight-wait-time-mins}")
    private int inflightWaitTimeMins;

    @Value("${kafka.retry.max-attempts}")
    private int maxRetryAttempts;

    @Value("${kafka.listener.id}")
    private String listenerId;

    @Autowired
    protected DBRepository dbRepository;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

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
        log.info("- In-flight wait timeout: {} minutes", inflightWaitTimeMins);
        log.info("- Manual commit mode enabled");
        log.info("- Max retry attempts: {}", maxRetryAttempts);
        log.info("- Initial RouteAwayFlag: {}", routeAwayFlag.get());
    }

    private void pauseAllContainers() {
        registry.getListenerContainers().forEach(container -> {
            container.pause();
            log.info("Container {} paused due to RouteAwayFlag=true", container.getListenerId());
        });
    }

    private void resumeAllContainers() {
        registry.getListenerContainers().forEach(container -> {
            container.resume();
            log.info("Container {} resumed due to RouteAwayFlag=false", container.getListenerId());
        });
    }

    protected Mono<Void> processMessage(ConsumerRecord<String, T> record, Acknowledgment acknowledgment) {
        log.debug("Processing message from Topic: {}, Partition: {}, Offset: {}", 
                record.topic(), record.partition(), record.offset());

        if (routeAwayFlag.get()) {
            log.info("Route away flag is true, skipping message processing");
            return Mono.empty();
        }

        return Mono.defer(() -> {
            messageProcessingCount.incrementAndGet();
            String applicationTraceId = record.key();
            T message = record.value();
            
            return doReceiveEvent(applicationTraceId, message)
                .flatMap(result -> {
                    if (!"Success".equals(result.getEventProcessorResultStatus())) {
                        log.error("Failed to receive message for trace ID: {}: {}", applicationTraceId, result.getMessage());
                        return Mono.just(result);
                    }
                    return doValidateEvent(applicationTraceId, result.getCommand());
                })
                .flatMap(result -> {
                    if (!"Success".equals(result.getEventProcessorResultStatus())) {
                        log.error("Failed to validate message for trace ID: {}: {}", applicationTraceId, result.getMessage());
                        return Mono.just(result);
                    }
                    return doExecuteReceive(applicationTraceId, result.getCommand());
                })
                .flatMap(result -> {
                    if (!"Success".equals(result.getEventProcessorResultStatus())) {
                        log.error("Failed to execute message for trace ID: {}: {}", applicationTraceId, result.getMessage());
                        return Mono.just(result);
                    }
                    return doAcceptEvent(applicationTraceId, result.getCommand());
                })
                .flatMap(result -> {
                    if (!"Success".equals(result.getEventProcessorResultStatus())) {
                        log.error("Failed to accept message for trace ID: {}: {}", applicationTraceId, result.getMessage());
                        return Mono.just(result);
                    }
                    return doProcessEvent(applicationTraceId, result.getCommand());
                })
                .doOnNext(result -> {
                    if ("Success".equals(result.getEventProcessorResultStatus())) {
                        log.info("Message processed successfully for trace ID: {}", applicationTraceId);
                        acknowledgment.acknowledge();
                    } else {
                        log.error("Failed to process message for trace ID: {}: {}", applicationTraceId, result.getMessage());
                        dbRepository.saveFailedMessage(record, result.getMessage(), 0, applicationTraceId);
                        acknowledgment.acknowledge();
                    }
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
                    
                    dbRepository.saveFailedMessage(record, e.getMessage(), 
                        record.headers().lastHeader("kafka_retryCount") != null ? 
                            Integer.parseInt(new String(record.headers().lastHeader("kafka_retryCount").value())) : 0,
                        applicationTraceId);
                        
                    return Mono.empty();
                })
                .then();
        });
    }

    // Abstract methods to be implemented by concrete classes
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
        registry.getListenerContainers().forEach(container -> {
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

    @PreDestroy
    public void onDestroy() {
        log.info("Shutting down Kafka listener - Waiting for in-flight messages to complete...");
        routeAwayFlag.set(true);
        pauseAllContainers();

        // Wait for in-flight messages to complete or timeout after configured minutes
        long startTime = System.currentTimeMillis();
        long timeoutMs = inflightWaitTimeMins * 60 * 1000L; // Convert minutes to milliseconds
        
        while (messageProcessingCount.get() > 0 && (System.currentTimeMillis() - startTime) < timeoutMs) {
            try {
                log.info("Waiting for {} in-flight messages to complete...", messageProcessingCount.get());
                Thread.sleep(1000); // Check every second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Shutdown interrupted while waiting for in-flight messages");
                break;
            }
        }

        if (messageProcessingCount.get() > 0) {
            log.warn("Shutdown timeout reached with {} messages still in-flight", messageProcessingCount.get());
        } else {
            log.info("All in-flight messages completed successfully");
        }
        
        log.info("Kafka listener shutdown complete");
    }

    // Private implementation methods
    private Mono<EventProcessorResult> doReceiveEvent(String applicationTraceId, T message) {
        return receiveEvent(applicationTraceId, message);
    }

    private Mono<EventProcessorResult> doValidateEvent(String applicationTraceId, Command command) {
        return validateEvent(applicationTraceId, command);
    }

    private Mono<EventProcessorResult> doExecuteReceive(String applicationTraceId, Command command) {
        return executeReceive(applicationTraceId, command);
    }

    private Mono<EventProcessorResult> doAcceptEvent(String applicationTraceId, Command command) {
        return acceptEvent(applicationTraceId, command);
    }

    private Mono<EventProcessorResult> doProcessEvent(String applicationTraceId, Command command) {
        return processEvent(applicationTraceId, command);
    }

    // Protected methods with default implementations that can be overridden if needed
    protected Mono<EventProcessorResult> receiveEvent(String applicationTraceId, T message) {
        log.info("Receiving event for trace ID: {}", applicationTraceId);
        try {
            CommandEvent<T> commandEvent = (CommandEvent<T>) message;
            return Mono.just(EventProcessorResult.success(Command.builder()
                    .id(applicationTraceId)
                    .payload(commandEvent)
                    .build()));
        } catch (Exception e) {
            log.error("Error in receiveEvent for trace ID {}: {}", applicationTraceId, e.getMessage(), e);
            return Mono.just(EventProcessorResult.failure(e.getMessage()));
        }
    }

    protected Mono<EventProcessorResult> validateEvent(String applicationTraceId, Command command) {
        log.info("Validating event for trace ID: {}", applicationTraceId);
        try {
            return Mono.just(EventProcessorResult.success(command));
        } catch (Exception e) {
            log.error("Error in validateEvent for trace ID {}: {}", applicationTraceId, e.getMessage(), e);
            return Mono.just(EventProcessorResult.failure(e.getMessage()));
        }
    }

    protected Mono<EventProcessorResult> executeReceive(String applicationTraceId, Command command) {
        log.info("Executing receive for trace ID: {}", applicationTraceId);
        try {
            return Mono.just(EventProcessorResult.success(command));
        } catch (Exception e) {
            log.error("Error in executeReceive for trace ID {}: {}", applicationTraceId, e.getMessage(), e);
            return Mono.just(EventProcessorResult.failure(e.getMessage()));
        }
    }

    protected Mono<EventProcessorResult> acceptEvent(String applicationTraceId, Command command) {
        log.info("Accepting event for trace ID: {}", applicationTraceId);
        try {
            return Mono.just(EventProcessorResult.success(command));
        } catch (Exception e) {
            log.error("Error in acceptEvent for trace ID {}: {}", applicationTraceId, e.getMessage(), e);
            return Mono.just(EventProcessorResult.failure(e.getMessage()));
        }
    }

    protected Mono<EventProcessorResult> processEvent(String applicationTraceId, Command command) {
        log.info("Processing event for trace ID: {}", applicationTraceId);
        try {
            return Mono.just(EventProcessorResult.success(command));
        } catch (Exception e) {
            log.error("Error in processEvent for trace ID {}: {}", applicationTraceId, e.getMessage(), e);
            return Mono.just(EventProcessorResult.failure(e.getMessage()));
        }
    }
}