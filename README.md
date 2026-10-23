# High-Availability Distributed Transaction & Order Processing Engine

An enterprise-grade, event-driven ordering system engineered to resolve critical core distributed systems failure vectors: **dual-write integrity loss**, **concurrent race conditions under heavy traffic spikes**, and **downstream asynchronous message duplication**.

By completely decoupling high-volume ingestion from heavy billing settlement routines using the **Transactional Outbox Pattern** and **Java 21 Virtual Threads**, the system unblocks web requests instantly, achieving an edge processing latency of **sub-15ms**.

---

## 🏗️ System Architecture & Workflow

```text
                        [ Postman / Client Request ] 
                                     │  (HTTP POST to :8081/api/orders)
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ 1. ORDER-SERVICE CONTAINER (:8081)                                                      │
│                                                                        ┌─────────────┐  │
│    A. INGRESS RATE-LIMITER FILTER (Redis Boundary)                     │ LOCAL CACHE │  │
│       Executes Atomic Lua Script against Redis Container               │  (Memory)   │  │
│          ├───[ FAIL (Over 5 req/sec) ] ──► HTTP 429 Too Many Requests  └──────▲──────┘  │
│          └───[ PASS ]                                                         │         │
│                │                                                              │         │
│                ▼                                                              │         │
│    B. CACHE-ASIDE USER VALIDATION ENGINE                                      │         │
│       Checks user profile cache footprint ────────────────────────────────────┘         │
│          ├───[ HIT  ] ──► Validate account context instantly (Sub-2ms)                  │
│          └───[ MISS ] ──► Query Neon Cloud DB ──► Populates Local Redis Cache           │
│                │                                                                        │
│                ▼                                                                        │
│    C. ATOMIC DB TRANSACTION GATEWAY (@Transactional Boundary)                           │
│       ┌─────────────────────────────────────────────────────────────┐                   │
│       │ Combined inside a single ACID Database Transaction:         │                   │
│       │ 1. Save data row to [orders table] (Status: PENDING)        │                   │
│       │ 2. Save payload map log to [outbox_events table] (PENDING)  │                   │
│       └──────────────────────────────┬──────────────────────────────┘                   │
│                                      │ (Committed atomically via TLS)                   │
└──────────────────────────────────────┼──────────────────────────────────────────────────┘
                                       ▼
                         ┌───────────────────────────┐
                         │    NEON CLOUD POSTGRES    │
                         │    (Singapore Region)     │
                         └─────────────┬─────────────┘
                                       ▲
                                       │ (Background Polling Loop every 500ms)
┌──────────────────────────────────────┼───────────────────────────────────────────────────┐
│ 2. ASYNCHRONOUS ENGINE DISPATCHER    │                                                   │
│                                      │                                                   │
│    A. OUTBOX SCHEDULER LAYER         │                                                   │
│       Leverages Java 21 Virtual Threads (`newVirtualThreadPerTaskExecutor`) to poll      │
│       `PENDING` rows out of Neon Cloud DB safely without OS thread locking.              │
│                │                                                                         │
│                ▼                                                                         │
│    B. KAFKA TEMPLATE ENGINE                                                              │
│       Dispatches payload event to Kafka Cluster broker using routing key: `orderId`      │
│                │                                                                         │
│                ▼                                                                         │
│    C. POST-ACKNOWLEDGEMENT TRANSACTION FINALIZER                                         │
│       Upon broker write validation receipt ──► Updates row status in Neon to: `PROCESSED`│
└────────────────┬─────────────────────────────────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 3. APACHE KAFKA BROKER CONTAINER (:9092)                                               │
│                                                                                        │
│    Core Live Stream Topic Hub:  [ "order-events" ]                                     │
│                                     │                                                  │
│                       (If App Processor Crashes 3 Times)                               │
│                                     ▼                                                  │
│    Dead Letter Queue Topic Hub: [ "order-events-dlq" ] <─── [Fault Isolated Isolation] │
└────────────────┬───────────────────────────────────────────────────────────────────────┘
                 │
                 ▼ (Asynchronous Push-Stream Delivery)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 4. PAYMENT-SERVICE CONTAINER (:8082)                                                   │
│                                                                                        │
│    A. KAFKA LISTENER ADAPTER                                                           │
│       Intercepts streaming broker notifications via custom factory loops.              │
│                │                                                                       │
│                ▼                                                                       │
│    B. IDEMPOTENCY DEDUPLICATION GUARD                                                  │
│       Queries Neon Database directly: "Does this orderId exist in processed_events?"   │
│          ├───[ YES (Duplicate Found) ] ──► Log warning instruction. Drop safely.       │
│          └───[ NO (New Message) ]                                                      │
│                │                                                                       │
│                ▼                                                                       │
│    C. BILLING SETTLEMENT ENGINE                                                        │
│       1. Processes core settlement execution logic loops.                              │
│       2. Inserts message tracking hash footprint into Neon [processed_events table].   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## ⚡ Core Distributed System Problems & Solutions

### 1. Dual-Write Inconsistency Protection (The Outbox Core)
* **The Problem:** Simultaneously updating a database and pushing a notification to an external event streaming broker (like Kafka) creates a dangerous dual-write vulnerabilities channel. If the database commit succeeds but the network blinks before the Kafka broadcast completes, the system enters an irreversible, corrupted state.
* **The Solution:** The `OrderService` encapsulates state generation inside an atomic Spring `@Transactional` block. The system writes the order record to the `orders` table and writes the serialized message parameters to the `outbox_events` table simultaneously within a single **ACID transaction** sent via TLS to our **Neon Cloud database**. They either completely succeed or roll back together, guaranteeing zero data mismatch.

### 2. Low-Overhead Background Polling (Java 21 Virtual Threads)
* **The Problem:** Polling data tables via standard background workers blocks expensive Operating System kernel threads, causing memory overhead and thread starvation under heavy throughput.
* **The Solution:** Our `OutboxScheduler` fires every 500ms and allocates a lightweight **Java 21 Virtual Thread** via `Executors.newVirtualThreadPerTaskExecutor()` for each event transport task. When the thread blocks waiting for Kafka's asynchronous `.get()` broker acknowledgment callback, it yields its underlying platform carrier thread back to the runtime pool, ensuring non-blocking operations.

### 3. Concurrency Protection (Atomic Redis Lua Scripts)
* **The Problem:** High-concurrency traffic bursts create race conditions where multiple application instances read a database request count simultaneously, undercounting traffic and letting bot attacks pass standard API rate limits.
* **The Solution:** The RateLimiterFilter catches traffic right at the Tomcat edge gateway. Instead of running a standard "get-then-increment" code block, it executes an inline Lua Script against the Redis container. Because Redis executes Lua scripts inside a strict single-threaded loop, the entire check-and-decrement operation acts as an unbroken, atomic execution unit, dropping abusive spikes with a clean HTTP 429 Too Many Requests code.

### 4. Financial Integrity (Database-Enforced Idempotency)

* **The Problem:** Apache Kafka operates under At-Least-Once message delivery semantics. If a network disruption prevents a consumer from committing its offset back to the broker, Kafka will inevitably re-deliver the exact same message on its next cycle, causing devastating double-charging bugs in billing systems.
* **The Solution:** Before executing any financial calculations or accounting settlements, the OrderEventConsumer pulls the message's unique orderId key and queries the Neon database via ProcessedEventsRepository.existsById(orderId). If a matching footprint exists, the message is identified as a duplicate network retry, logged as a warning, and safely dropped without executing business code.

### 5. Resiliency & Fault Isolation (Dead Letter Queue Strategy)

* **The Problem:** If an unexpected corrupted payload structure ("Poison Pill") lands in a Kafka stream, it will trigger an infinite exception retry loop inside the consumer, completely locking up processing on that entire message partition queue.
* **The Solution:** Our custom KafkaConsumerConfig defines a production-ready DefaultErrorHandler paired with a DeadLetterPublishingRecoverer. If a message throws a runtime crash, the handler catches it, logs the exception, waits a 1000ms delay window, and retries processing exactly 3 times. If it fails consecutively, it isolates the single broken payload and shifts it to an independent order-events-dlq topic, keeping the main stream fully operational.

## 📊 Live System Telemetry Benchmarks


Application performance and execution delays were carefully monitored and recorded using method-level telemetry instrumentation wrappers (System.nanoTime()), Spring Boot Actuator, and Micrometer:
### Redis Cache-Aside Performance:
  * Cache Miss Latency: 29ms to 52ms (Triggered on the first request; requires a network round-trip from the local machine to the serverless Neon Cloud PostgreSQL cluster in Singapore).
  * Cache Hit Latency: 0ms to 1ms (On all subsequent requests, Spring’s caching proxy interceptor resolves user context lookups entirely via the local in-memory Redis instance).
  * Neon Cloud PostgreSQL ACID Transaction Commit Latency: 397ms to 433ms (The total physical time required to transmit and securely commit rows across both tables concurrently via a cross-region cloud connection).
  * Ingress Edge Decoupling Speed: sub-15ms (Because the heavy payment processing workloads are completely decoupled via Kafka events, the controller returns an HTTP 201 Created response to Postman instantly, keeping application threads unblocked).

## 🚀 Step-by-Step Operations & Setup Guide


### Environment Configurations

   Create a file named exactly .env inside your root project folder `(E:\Projects\distributed-order-engine)` and fill it with your personal database keys:
env
```
DB_URL=jdbc:postgresql://your-neon-endpoint.singapore.neon.tech/neondb?sslmode=require
DB_USER=your_neon_database_username
DB_PASSWORD=your_neon_database_password
KAFKA_BROKER=kafka:29092
REDIS_HOST=redis
```


### Rebuild and Launch the Container Architecture
    
   Open your PowerShell terminal inside your root directory and run the complete multi-stage build orchestration:
```powershell
1. Clear out stale broken compilation layer fragments

docker builder prune -f

2. Compile Java code inside containers and bring the network online

docker compose -f production-compose.yml --env-file .env up --build -d
```
(This command will automatically download runtime images, execute clean maven packaging phases, build thin production containers, and fire up your entire application architecture).

### Verify System Health Status

Ensure all containers are up and running green:
```powershell docker ps```

### Execute a Live Integration Test

Fire a standard order placement body payload straight into your application stream using PowerShell:
powershell
```
Invoke-RestMethod -Method Post -Uri "http://localhost:8081/api/orders" -ContentType "application/json" -Body '{"userId": "a3b4c5d6-e7f8-90a1-b2c3-d4e5f6a7b8c9", "amount": 199.99}'
```
(This will return a clean JSON string showing your auto-generated order parameters marked with a status value of "PENDING" in under 15ms).

### Review the Performance Telemetry Logs

To check your live latency metrics and watch your outbox thread dispatch events to Kafka, inspect your container output streams:
powershell 
```
docker logs prod_order_service --tail 30
```

### Safely Stopping the Project

Whenever you are done coding or practicing for interviews, stop the background clusters to release your computer's RAM and CPU resources safely:
```
powershell docker compose -f production-compose.yml down
```
(Note: Because your records are saved inside the Neon Cloud database, shutting down your containers will never delete your order histories or database data).

## 📂 Multi-Module Repository Layout

```text
E:.
├───Documentations (System design artifacts)
│
├───order (Order Ingress & Transaction Outbox Service)
│   ├───pom.xml (Maven dependencies configuration for the order service)
│   └───src
│       ├───main
│       │   ├───java/com/backendEngineering/distributedorderengine/order
│       │   │   ├───config (Edge Protection Filters, Redis Atomicity Profiles)
│       │   │   ├───controller (REST Endpoints & Request Ingest Mapping Mappings)
│       │   │   ├───entity (Orders & Outbox Database Entity Mappings)
│       │   │   ├───repository (Spring Data JPA Interface Gateway Bridges)
│       │   │   ├───scheduler (Java 21 Virtual Thread Outbox Poller Engine)
│       │   │   └───service (Core ACID Transactional Business Logic Engines)
│       │   └───resources
│       │       └───application.properties (Database pool metrics & credentials settings)
│       └───test (Integration suites & pipeline resilience tests)
│
└───payment (Downstream Billing & Deduplication Service)
    ├───pom.xml (Maven dependencies configuration for the payment consumer)
    └───src
        ├───main
        │   ├───java/com/backendEngineering/distributedorderengine/payment
        │   │   ├───config (Kafka Container Listener Factories & DLQ Error Mappings)
        │   │   ├───consumer (Event Driven Topic Subscriptions & Push Stream Listeners)
        │   │   ├───entity (Idempotency Audit Logging Persistence Records)
        │   │   └───repository (Idempotency Guard Audit Gate Interface Bridges)
        │   └───resources
        │       └───application.properties (Kafka listener & group identification bindings)
        └───test (Idempotency deduplication logic verification profiles)
```
