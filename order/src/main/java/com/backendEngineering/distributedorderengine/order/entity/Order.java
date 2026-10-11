package com.backendEngineering.distributedorderengine.order.entity;

import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Data @NoArgsConstructor @AllArgsConstructor
public class Order {
    @Id
    private UUID orderId;
    private UUID userId;
    private BigDecimal totalAmount;
    private String status;
    private LocalDateTime createdAt;
}
