package com.backendEngineering.distributedorderengine.order.repository;
import com.backendEngineering.distributedorderengine.order.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {}
