package com.example.kafkaprocessor.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import lombok.extern.log4j.Log4j2;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import jakarta.annotation.PostConstruct;

@Configuration
@EnableKafka
@Log4j2
public class AvroKafkaConfig {
    
    @Value("${kafka.topic.avro.partitions}")
    private int totalPartitions;
    
    @Value("${kafka.topic.avro.consumers-per-dc}")
    private int consumersPerDc;
    
    @Value("${spring.kafka.listener.concurrency}")
    private int listenerConcurrency;
    
    @PostConstruct
    public void validateConfiguration() {
        int expectedConcurrency = totalPartitions / consumersPerDc;
        if (listenerConcurrency != expectedConcurrency) {
            log.warn("WARNING: AVRO Listener concurrency misconfigured!");
            log.warn("Current concurrency: {}, Expected: {} ({} partitions / {} consumers per DC)", 
                    listenerConcurrency, expectedConcurrency, totalPartitions, consumersPerDc);
            log.warn("This may lead to suboptimal partition distribution!");
        } else {
            log.info("AVRO Kafka concurrency validated: {} threads for {} partitions with {} consumers per DC", 
                    listenerConcurrency, totalPartitions, consumersPerDc);
        }
    }

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.topic.avro.name}")
    private String topicName;

    @Bean
    public String avroKafkaTopicName() {
        return topicName;
    }
    
    @Bean
    public String avroKafkaListenerConcurrency() {
        return String.valueOf(listenerConcurrency);
    }

    private static final long RETRY_INTERVAL_MS = 30000L; // 30 seconds
    private static final int SESSION_TIMEOUT_MS = 60000; // 60 seconds
    private static final int HEARTBEAT_INTERVAL_MS = 20000; // 20 seconds

    @Autowired
    private VaultAuthenticationProvider vaultAuthenticationProvider;

    @Bean
    public ConsumerFactory<String, Object> avroConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(getAvroConsumerProperties());
    }

    private Map<String, Object> getAvroConsumerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-avro");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        
        // Network retry configuration - retry every 30 seconds indefinitely
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, RETRY_INTERVAL_MS);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, RETRY_INTERVAL_MS);
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, RETRY_INTERVAL_MS);
        
        // Connection timeouts and monitoring
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_INTERVAL_MS);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, RETRY_INTERVAL_MS);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        
        // Connection error handling
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, -1); // Disable idle timeout
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, RETRY_INTERVAL_MS);

        // AVRO specific configuration
        props.put("schema.registry.url", "${SCHEMA_REGISTRY_URL:*****}");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("specific.avro.reader", true);

        // Security settings
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        
        // Get current Vault credentials
        String currentUsername = vaultAuthenticationProvider.getU();
        String currentPassword = vaultAuthenticationProvider.getP();
        
        if (currentUsername != null && currentPassword != null) {
            props.put("sasl.jaas.config", 
                String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                             currentUsername,
                             currentPassword));
        } else {
            log.error("Failed to get Kafka credentials from Vault. Check Vault configuration and connectivity.");
            throw new IllegalStateException("Kafka credentials not available from Vault");
        }

        return props;
    }

    @Value("${kafka.topic.route-away.shutdown-timeout}")
    private int shutdownTimeoutMs;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> avroKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(avroConsumerFactory());
        
        // Configure container properties
        ContainerProperties containerProps = factory.getContainerProperties();
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProps.setShutdownTimeout(shutdownTimeoutMs);
        
        // Configure error handling and recovery
        factory.setAutoStartup(true);
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setSyncCommits(false);
        
        // Network recovery settings
        factory.getContainerProperties().setIdleEventInterval(RETRY_INTERVAL_MS);
        factory.setConcurrency(listenerConcurrency);
        
        // Set restart policy to always retry
        factory.getContainerProperties().setRestartAfterAuthExceptions(true);
        // Add rebalance listener for logging
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("AVRO Consumer group rebalancing - Partitions being revoked:");
                partitions.forEach(partition -> 
                    log.info("  - Topic: {}, Partition: {}", 
                        partition.topic(), partition.partition())
                );
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("AVRO Consumer group rebalancing - New partitions assigned:");
                partitions.forEach(partition -> 
                    log.info("  - Topic: {}, Partition: {}", 
                        partition.topic(), partition.partition())
                );
                log.info("Total partitions assigned: {}", partitions.size());
            }
        });
        return factory;
    }
}