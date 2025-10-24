package com.example.kafkaprocessor.repository;

import com.example.kafkaprocessor.entity.RouteAwayFlag;
import com.example.kafkaprocessor.entity.FailedMessage;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
@Log4j2
public class DBRepository {
    @PersistenceContext
    private EntityManager entityManager;

    @PostConstruct
    public void init() {
        // Initialize RouteAwayFlag if it doesn't exist
        ensureRouteAwayFlagExists();
    }

    @Transactional
    protected void ensureRouteAwayFlagExists() {
        Optional<RouteAwayFlag> flag = getRouteAwayFlag();
        if (flag.isEmpty()) {
            RouteAwayFlag newFlag = new RouteAwayFlag();
            newFlag.setId(1L);
            newFlag.setFlag(true); // Initialize as true for safety
            newFlag.setLastUpdated(LocalDateTime.now());
            entityManager.persist(newFlag);
            log.info("Initialized RouteAwayFlag with default value: true");
        }
    }

    @Transactional(readOnly = true)
    public Optional<RouteAwayFlag> getRouteAwayFlag() {
        return Optional.ofNullable(entityManager.find(RouteAwayFlag.class, 1L));
    }

    @Transactional
    public boolean updateRouteAwayFlag(boolean flag) {
        RouteAwayFlag routeAwayFlag = entityManager.find(RouteAwayFlag.class, 1L);
        if (routeAwayFlag == null) {
            routeAwayFlag = new RouteAwayFlag();
            routeAwayFlag.setId(1L);
        }

        boolean valueChanged = routeAwayFlag.isFlag() != flag;
        routeAwayFlag.setFlag(flag);
        routeAwayFlag.setLastUpdated(LocalDateTime.now());
        entityManager.persist(routeAwayFlag);

        if (valueChanged) {
            log.info("RouteAwayFlag changed to {} at {}", flag, routeAwayFlag.getLastUpdated());
        } else {
            log.debug("RouteAwayFlag updated (no change) at {}", routeAwayFlag.getLastUpdated());
        }

        return valueChanged;
    }

    @Transactional(readOnly = true)
    public LocalDateTime getLastUpdateTime() {
        return getRouteAwayFlag()
                .map(RouteAwayFlag::getLastUpdated)
                .orElse(null);
    }

    @Transactional
    public void saveFailedMessage(ConsumerRecord<String, String> record, String errorMessage, int retryCount, String applicationTraceId) {
        FailedMessage failedMessage = new FailedMessage();
        failedMessage.setTopic(record.topic());
        failedMessage.setPartition(record.partition());
        failedMessage.setOffset(record.offset());
        failedMessage.setKey(record.key());
        failedMessage.setValue(record.value());
        failedMessage.setErrorMessage(errorMessage);
        failedMessage.setFailureTime(LocalDateTime.now());
        failedMessage.setRetryCount(retryCount);
        failedMessage.setApplicationTraceId(applicationTraceId);
        
        entityManager.persist(failedMessage);
        log.warn("Message saved to failed messages database. Topic: {}, Partition: {}, Offset: {}, Error: {}", 
            record.topic(), record.partition(), record.offset(), errorMessage);
    }
}