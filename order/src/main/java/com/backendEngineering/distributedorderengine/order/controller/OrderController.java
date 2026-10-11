package com.backendEngineering.distributedorderengine.order.controller;

import com.backendEngineering.distributedorderengine.order.entity.Order;
import com.backendEngineering.distributedorderengine.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Map<String, Object> payload) {
        UUID userId = UUID.fromString(payload.get("userId").toString());
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());
        return ResponseEntity.status(HttpStatus.CREATED).body(orderService.placeOrder(userId, amount));
    }
}
