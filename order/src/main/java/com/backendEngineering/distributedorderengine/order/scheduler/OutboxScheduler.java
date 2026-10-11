package com.backendEngineering.distributedorderengine.order.scheduler;

import com.backendEngineering.distributedorderengine.order.entity.OutboxEvent;
import com.backendEngineering.distributedorderengine.order.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxScheduler {
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 500)
    public void processOutbox() {
        List<OutboxEvent> events = outboxRepository.findByStatusOrderByCreatedAtAsc("PENDING");
        if (events.isEmpty()) return;

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (OutboxEvent event : events) {
                executor.submit(() -> {
                    try {
                        kafkaTemplate.send("order-events", event.getAggregateId(), event.getPayload()).get();
                        event.setStatus("PROCESSED");
                        outboxRepository.save(event);
                        log.info("Successfully shifted event execution map to Kafka for Order Reference ID: {}", event.getAggregateId());
                    } catch (Exception e) {
                        log.error("Network streaming failed for outbox tracking cluster node mapping", e);
                        event.setStatus("FAILED");
                        outboxRepository.save(event);
                    }
                });
            }
        }
    }
}
