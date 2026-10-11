package com.backendEngineering.distributedorderengine.payment.consumer;

import com.backendEngineering.distributedorderengine.payment.entity.ProcessedEventLog;
import com.backendEngineering.distributedorderengine.payment.repository.ProcessedEventsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {
    private final ProcessedEventsRepository repository;

    @KafkaListener(topics = "order-events", groupId = "payment-group", containerFactory = "kafkaListenerContainerFactory")
    public void handleOrderEvent(ConsumerRecord<String, String> record) {
        String orderId = record.key();
        String payload = record.value();

        // Target processing hook condition checking to intentionally trigger DLQ routing mechanics
        if (payload.contains("TRIGGER_RESILIENCE_DLQ")) {
            throw new IllegalArgumentException("Exception raised intentionally to test system routing to the Dead Letter Queue.");
        }

        // Exact database primary key check-then-act configuration barrier guard to avoid duplicate entries
        if (repository.existsById(orderId)) {
            log.warn("Duplicate consumer event identified for key pointer references: {}. Execution aborted.", orderId);
            return;
        }

        try {
            log.info("Processing secure payment processing workflow for Order ID: {}", orderId);
            Thread.sleep(150); // Simulate transaction business workload latency

            repository.save(new ProcessedEventLog(orderId, LocalDateTime.now()));
            log.info("Payment settled successfully for Order ID: {}", orderId);
        } catch (Exception e) {
            log.error("Fatal exception during final consumer settlement stage loop", e);
            throw new RuntimeException(e);
        }
    }
}
