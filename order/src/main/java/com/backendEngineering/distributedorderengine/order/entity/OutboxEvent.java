package com.backendEngineering.distributedorderengine.order.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Data @NoArgsConstructor @AllArgsConstructor
public class OutboxEvent {
    @Id
    private UUID eventId;
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    @Column(columnDefinition = "TEXT")
    private String payload;
    private String status;
    private LocalDateTime createdAt;
}
