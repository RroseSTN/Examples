package com.example.kafkaprocessor.config;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.time.LocalDateTime;

@Component
@Log4j2
public class VaultAuthenticationProvider {
    
    @Value("${kafka.auth.credentials-refresh-interval:30000}")
    private long credentialsRefreshInterval;

    @Value("${vault.secret.kafka-path}")
    private String vaultSecretPath;

    private final AtomicReference<String> currentUsername = new AtomicReference<>();
    private final AtomicReference<String> currentPassword = new AtomicReference<>();
    private LocalDateTime lastPasswordUpdate;
    
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private VaultTemplate vaultTemplate;

    public String getU() {
        return currentUsername.get();
    }

    public String getP() {
        return currentPassword.get();
    }

    @PostConstruct
    public void init() {
        // Initial password fetch
        refreshCredentials();
    }

    @Scheduled(fixedRateString = "${kafka.auth.credentials-refresh-interval:30000}")
    public void refreshCredentials() {
        try {
            String newPassword = fetchPasswordFromVault();
            String oldPassword = currentPassword.get();
            
            if (newPassword != null && !newPassword.equals(oldPassword)) {
                log.info("Kafka credentials have changed, updating connections...");
                currentPassword.set(newPassword);
                lastPasswordUpdate = LocalDateTime.now();
                
                // Restart all Kafka containers to apply new credentials
                restartKafkaContainers();
            }
        } catch (Exception e) {
            log.error("Failed to refresh Kafka credentials from vault: {}", e.getMessage(), e);
        }
    }

    private String fetchPasswordFromVault() {
        try {
            VaultResponse response = vaultTemplate.read(vaultSecretPath);
            if (response == null || response.getData() == null) {
                log.error("No credentials found in Vault at path: {}", vaultSecretPath);
                return null;
            }

            Map<String, Object> data = response.getData();
            if (data != null) {
                String username = (String) data.get("username");
                String password = (String) data.get("password");

                if (username == null || password == null) {
                    log.error("Missing username or password in Vault response");
                    return null;
                }

                currentUsername.set(username);
                return password;
            } else {
                log.error("No data found in Vault response");
                return null;
            }
        } catch (Exception e) {
            log.error("Failed to fetch credentials from Vault: {}", e.getMessage(), e);
            return null;
        }
    }

    private void restartKafkaContainers() {
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container -> {
            try {
                log.info("Restarting container {} to apply new credentials", container.getListenerId());
                container.stop();
                Thread.sleep(5000); // Give it time to properly shut down
                container.start();
            } catch (Exception e) {
                log.error("Error restarting container {}: {}", container.getListenerId(), e.getMessage(), e);
            }
        });
    }

    public Optional<LocalDateTime> getLastPasswordUpdate() {
        return Optional.ofNullable(lastPasswordUpdate);
    }
}