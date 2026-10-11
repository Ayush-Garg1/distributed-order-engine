package com.backendEngineering.distributedorderengine.payment.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "processed_events")
@Data @NoArgsConstructor @AllArgsConstructor
public class ProcessedEventLog {
    @Id
    private String eventKey;
    private LocalDateTime processedAt;
}
