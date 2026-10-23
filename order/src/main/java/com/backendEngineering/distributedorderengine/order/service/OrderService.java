package com.backendEngineering.distributedorderengine.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.backendEngineering.distributedorderengine.order.entity.Order;
import com.backendEngineering.distributedorderengine.order.entity.OutboxEvent;
import com.backendEngineering.distributedorderengine.order.repository.OrderRepository;
import com.backendEngineering.distributedorderengine.order.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Service
public class OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final UserCacheComponent userCacheComponent; // Inject the component

    public OrderService(OrderRepository orderRepository, OutboxRepository outboxRepository,
                        ObjectMapper objectMapper, UserCacheComponent userCacheComponent) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.userCacheComponent = userCacheComponent;
    }

    @Transactional
    public Order placeOrder(UUID userId, BigDecimal amount) {
        // Metric Vector 1: Now securely routes through Spring's Redis caching proxy
        long cacheStart = System.nanoTime();
        userCacheComponent.verifyUserContext(userId);
        long cacheDurationMs = (System.nanoTime() - cacheStart) / 1_000_000;
        log.info("--> TELEMETRY: Redis Cache-Aside Verification Latency: {} ms", cacheDurationMs);

        // Metric Vector 2: Measure Neon Cloud DB Commit Latency
        long dbStart = System.nanoTime();
        Order order = new Order(UUID.randomUUID(), userId, amount, "PENDING", LocalDateTime.now());
        orderRepository.save(order);

        Map<String, Object> payloadMap = Map.of(
                "orderId", order.getOrderId().toString(),
                "userId", order.getUserId().toString(),
                "amount", order.getTotalAmount()
        );

        try {
            String jsonPayload = objectMapper.writeValueAsString(payloadMap);
            OutboxEvent outbox = new OutboxEvent(
                    UUID.randomUUID(), "ORDER", order.getOrderId().toString(),
                    "ORDER_CREATED", jsonPayload, "PENDING", LocalDateTime.now()
            );
            outboxRepository.save(outbox);
        } catch (Exception e) {
            throw new RuntimeException("Atomic database transaction write failed", e);
        }

        long dbDurationMs = (System.nanoTime() - dbStart) / 1_000_000;
        log.info("--> TELEMETRY: Neon Cloud DB Atomic Transaction Commit Latency: {} ms", dbDurationMs);

        return order;
    }
}
