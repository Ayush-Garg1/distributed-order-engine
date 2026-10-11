package com.backendEngineering.distributedorderengine.payment.repository;
import com.backendEngineering.distributedorderengine.payment.entity.ProcessedEventLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventsRepository extends JpaRepository<ProcessedEventLog, String> {}
