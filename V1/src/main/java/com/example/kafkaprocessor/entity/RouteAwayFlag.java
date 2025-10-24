package com.example.kafkaprocessor.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "route_away_flag")
@Data
public class RouteAwayFlag {
    @Id
    private Long id;
    private boolean flag;
    private LocalDateTime lastUpdated;
}