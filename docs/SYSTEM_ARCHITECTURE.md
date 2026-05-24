# Payment Hub — System Architecture

**Document version:** 1.0  
**Last updated:** May 2026  
**Status:** Active  

---

## 1. System Overview

### Purpose

Payment Hub is a centralized payment processing service that accepts payment requests over HTTP, validates and executes them synchronously, persists the outcome, and publishes domain events for downstream consumers. It is designed as a reference implementation of patterns commonly found in enterprise payment platforms: idempotency guarantees, explicit payment lifecycle management, transactional persistence, and event-driven notification of external systems.

The system targets **real-time payment initiation** — callers receive a definitive status (`COMPLETED` or `FAILED`) in the same HTTP response — while decoupling post-payment side effects (notifications, analytics, reconciliation) through Apache Kafka.

### Centralized Payment Hub Model

Rather than embedding payment logic in every client application, all payment traffic flows through a single hub. Clients submit structured payment requests; the hub owns:

- **Identity and deduplication** — each payment receives a unique `paymentId` and is keyed by client-supplied `idempotencyKey`.
- **Business rule enforcement** — amount limits, account validation, and status transitions are applied in one place.
- **Authoritative state** — PostgreSQL holds the system of record for payment records and their lifecycle.
- **Event broadcast** — successful and failed payments are published to Kafka topics so other services can react without tight coupling.

This model simplifies compliance, auditing, and operational monitoring by concentrating payment behavior in a bounded, well-defined service boundary.

---

## 2. High-Level Architecture

### Architecture Diagram

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                              Client Layer                               │
│                    (Mobile apps, web frontends, B2B APIs)                 │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │ HTTP/JSON
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         API Layer (Spring Web)                          │
│              PaymentController — REST endpoints, validation             │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Service Layer (Spring)                           │
│  PaymentService — orchestration, transactions, idempotency            │
│  PaymentProcessorService — synchronous business rules                   │
└───────┬─────────────────────────────────┬───────────────────────────────┘
        │                                 │
        ▼                                 ▼
┌───────────────────┐           ┌─────────────────────────────────────────┐
│  Persistence      │           │  Messaging Layer (Spring Kafka)         │
│  PostgreSQL       │           │  PaymentEventProducer / Consumer        │
│  PaymentRepository│           │  Topics: payment.completed,             │
│  (Spring Data JPA)│           │          payment.failed                 │
└───────────────────┘           └─────────────────┬───────────────────────┘
                                                  │
                                                  ▼
                                    ┌─────────────────────────────┐
                                    │  Downstream Consumers       │
                                    │  (notifications, analytics) │
                                    └─────────────────────────────┘
```

### Major Components

| Component | Technology | Role |
|-----------|------------|------|
| Payment Hub API | Spring Boot 3, Java 21 | HTTP gateway and application orchestration |
| Database | PostgreSQL 16 | System of record for payments |
| Message broker | Apache Kafka (Confluent 7.6) | Asynchronous event distribution |
| Container runtime | Docker / Docker Compose | Local and portable deployment |
| Cloud target | AWS ECS Fargate, ECR, MSK (planned) | Production deployment |

### Layer Responsibilities

**API layer** (`PaymentController`)  
Exposes REST endpoints, applies Jakarta Bean Validation on inbound DTOs, maps HTTP semantics (201 Created, 404 Not Found), and delegates all business logic to the service layer. Contains no persistence or messaging code.

**Service layer** (`PaymentService`, `PaymentProcessorService`)  
Owns the payment workflow: idempotency checks, entity creation, synchronous processing, status updates, and event publication. `PaymentService` defines the transactional boundary; `PaymentProcessorService` encapsulates domain validation rules.

**Persistence layer** (`PaymentRepository`, `Payment` entity)  
Provides CRUD and lookup by `paymentId` and `idempotencyKey`. Schema is managed via Hibernate `ddl-auto: update` in development; unique constraints enforce idempotency at the database level.

**Messaging layer** (`PaymentEventProducer`, `PaymentEventConsumer`)  
Publishes `PaymentEvent` payloads to topic-per-outcome (`payment.completed`, `payment.failed`). The in-process consumer simulates a downstream notification service.

**Cross-cutting** (`GlobalExceptionHandler`)  
Centralizes error mapping to HTTP status codes and structured JSON error bodies.

### Data Flow — Create Payment

```text
1. Client POST /payments with CreatePaymentRequest
2. Controller validates request (@Valid)
3. PaymentService.createPayment():
   a. Check idempotencyKey → 409 Conflict if duplicate
   b. Persist payment with status INITIATED
   c. PaymentProcessorService.process() → COMPLETED or FAILED
   d. Update payment status in database
   e. PaymentEventProducer.publishPaymentEvent() → Kafka
   f. Return PaymentResponse to client
4. PaymentEventConsumer (async) receives event → simulated notification
```

### Data Flow — Get Payment

```text
1. Client GET /payments/{paymentId}
2. PaymentService loads from PostgreSQL by paymentId
3. Returns PaymentResponse or 404 Not Found
```

### Payment Lifecycle

```text
INITIATED  →  PROCESSING  →  COMPLETED
                         ↘
                          FAILED
```

Terminal states (`COMPLETED`, `FAILED`) trigger Kafka publication. Intermediate states are persisted but not published.

---

## 3. Component Breakdown

### 3.1 PaymentController

| Aspect | Detail |
|--------|--------|
| **Responsibility** | HTTP interface for payment operations and health checks |
| **Inputs** | `CreatePaymentRequest` (JSON body), `paymentId` (path variable) |
| **Outputs** | `PaymentResponse` (JSON), plain-text health message |
| **Internal behavior** | Delegates to `PaymentService`; relies on `@Valid` for request validation before service invocation |
| **Interactions** | Calls `PaymentService`; errors bubble to `GlobalExceptionHandler` |

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/payments/health` | Liveness indicator |
| `POST` | `/payments` | Create and process a payment (201 Created) |
| `GET` | `/payments/{paymentId}` | Retrieve payment by business ID |

---

### 3.2 PaymentService

| Aspect | Detail |
|--------|--------|
| **Responsibility** | Orchestrates the full create-and-settle workflow |
| **Inputs** | `CreatePaymentRequest`, `paymentId` (for lookup) |
| **Outputs** | `PaymentResponse` |
| **Internal behavior** | Runs within `@Transactional`. Generates `PAY-{UUID8}` identifiers. Performs idempotency lookup, two-phase persistence (INITIATED then final status), invokes processor, publishes Kafka event, maps entity to DTO |
| **Interactions** | `PaymentRepository`, `PaymentProcessorService`, `PaymentEventProducer` |

Idempotency is enforced by querying `findByIdempotencyKey` before insert. A duplicate key raises `IllegalStateException`, mapped to HTTP 409 by the global handler.

---

### 3.3 PaymentProcessorService

| Aspect | Detail |
|--------|--------|
| **Responsibility** | Synchronous payment validation and settlement simulation |
| **Inputs** | `Payment` entity (amount, accounts) |
| **Outputs** | `PaymentStatus` — `COMPLETED` or `FAILED` |
| **Internal behavior** | Sets in-memory status to `PROCESSING`, then evaluates rules in order |

**Failure rules (returns `FAILED`):**

- `amount` is null
- `amount` ≤ 0
- `amount` > 10,000
- `fromAccount` equals `toAccount`

If all rules pass, returns `COMPLETED`. This component is intentionally stateless and contains no I/O, making it a natural extraction point for a future rules engine or external payment gateway adapter.

---

### 3.4 PaymentRepository & Payment Entity

| Aspect | Detail |
|--------|--------|
| **Responsibility** | Persistence and retrieval of payment records |
| **Inputs** | `Payment` entity, lookup keys |
| **Outputs** | `Optional<Payment>`, saved `Payment` |
| **Internal behavior** | Spring Data JPA repository; Hibernate maps to `payments` table |

**Schema highlights:**

| Column | Constraint | Purpose |
|--------|------------|---------|
| `payment_id` | UNIQUE, NOT NULL | Business identifier exposed to clients |
| `idempotency_key` | UNIQUE, NOT NULL | Client-supplied deduplication key |
| `status` | NOT NULL | Lifecycle state (enum string) |
| `amount` | DECIMAL(19,2) | Monetary value |
| `created_at`, `updated_at` | NOT NULL | Audit timestamps |

---

### 3.5 PaymentEventProducer

| Aspect | Detail |
|--------|--------|
| **Responsibility** | Publish payment outcome events to Kafka |
| **Inputs** | `Payment` with terminal status |
| **Outputs** | Kafka message on `payment.completed` or `payment.failed` |
| **Internal behavior** | Builds `PaymentEvent` DTO, resolves topic by status, sends with `paymentId` as message key for partition affinity |
| **Interactions** | `KafkaTemplate<String, PaymentEvent>`, Kafka cluster |

Message key = `paymentId` ensures ordered processing per payment if multiple events were ever published for the same payment.

---

### 3.6 PaymentEventConsumer

| Aspect | Detail |
|--------|--------|
| **Responsibility** | Consume payment outcome events (simulated notification service) |
| **Inputs** | `PaymentEvent` from Kafka |
| **Outputs** | Console log / simulated notification |
| **Internal behavior** | Two `@KafkaListener` methods, one per topic, consumer group `paymenthub-notification-service` |
| **Interactions** | Kafka broker; in production this would be a separate deployable service |

---

### 3.7 PaymentEvent

| Aspect | Detail |
|--------|--------|
| **Responsibility** | Kafka message payload contract |
| **Fields** | `paymentId`, `fromAccount`, `toAccount`, `amount`, `currency`, `status`, `eventTime` |
| **Serialization** | JSON via Spring Kafka `JsonSerializer` / `JsonDeserializer` |

---

### 3.8 GlobalExceptionHandler

| Aspect | Detail |
|--------|--------|
| **Responsibility** | Consistent HTTP error responses |
| **Mappings** | `IllegalArgumentException` → 404, `IllegalStateException` → 409, validation errors → 400, unhandled → 500 |
| **Output shape** | `{ timestamp, status, error, message/details }` |

---

### 3.9 DTOs & Validation

**CreatePaymentRequest** — inbound contract with `@NotBlank`, `@NotNull`, `@DecimalMin("0.01")` constraints. Note: API-level validation requires amount > 0, while `PaymentProcessorService` additionally rejects amounts > 10,000 and same-account transfers.

**PaymentResponse** — outbound contract exposing payment identity, accounts, amount, currency, status, and timestamps. Does not expose internal `id` or `idempotencyKey`.

---

## 4. Failure Handling

### Business Failures

Payment business rules are evaluated synchronously in `PaymentProcessorService`. Failures are **not** thrown as exceptions; they result in `FAILED` status, persisted to the database, and published to `payment.failed`. The client receives HTTP 201 with `status: "FAILED"` — the request was accepted and processed, but the payment did not settle.

This distinction matters: a failed payment is a valid, completed operation from the API's perspective, not a server error.

### Idempotency Conflicts

Duplicate requests sharing the same `idempotencyKey` are rejected before processing with HTTP **409 Conflict**. The error message includes the existing `paymentId`, allowing clients to reconcile without creating a second payment.

Database-level unique constraint on `idempotency_key` provides a secondary guard against race conditions between concurrent duplicate requests.

### Validation Failures

Malformed requests (missing fields, invalid amount format, amount below minimum) fail at the controller with HTTP **400 Bad Request** and field-level error details. These never reach the service layer or database.

### Not Found

Lookup of a non-existent `paymentId` raises `IllegalArgumentException`, mapped to HTTP **404 Not Found**.

### Infrastructure Failures

| Failure | Current behavior | Production consideration |
|---------|-------------------|-------------------------|
| PostgreSQL unavailable | Transaction rolls back; client receives 500 | Connection pooling, retries, circuit breakers |
| Kafka publish fails after DB commit | Payment saved but event may be lost (dual-write gap) | Outbox pattern or transactional messaging |
| Kafka consumer failure | Spring Kafka default retry; no DLQ configured | Dead-letter queue, alerting |
| Application crash mid-transaction | JPA transaction rollback; payment not committed | Idempotent retries from client |

### Unsupported Status for Events

If `PaymentEventProducer` receives a non-terminal status (`INITIATED`, `PROCESSING`), it throws `IllegalStateException` → HTTP 500. Under normal flow this cannot occur because only terminal statuses are passed after processing.

---

## 5. Scalability Considerations

### Horizontal Scaling — API Tier

The Payment Hub application is **stateless** (session state lives in PostgreSQL). Multiple instances can run behind a load balancer (e.g., AWS ALB → ECS Fargate tasks). Health checks run independently via `/payments/health`.

### Database

PostgreSQL is the primary scaling bottleneck. Strategies for growth:

- Read replicas for `GET /payments/{id}` queries
- Connection pooling (HikariCP, default in Spring Boot)
- Indexing on `payment_id` and `idempotency_key` (enforced by unique constraints)

Write throughput is bounded by synchronous processing per request. For very high volumes, consider async initiation (accept → queue → process) — a significant architectural shift.

### Kafka

Kafka provides natural decoupling and horizontal scale for consumers:

- **Partitioning** — messages keyed by `paymentId` distribute load across partitions
- **Consumer groups** — multiple notification/analytics consumers can scale independently
- **Topic isolation** — `payment.completed` and `payment.failed` allow targeted scaling and retention policies

Local development uses a single broker with replication factor 1. Production (Amazon MSK) should use multi-AZ clusters with appropriate replication.

### Synchronous Processing Limit

Because clients block until processing completes, API latency and throughput are tied to `PaymentProcessorService` execution time. Current rules are in-memory and fast; integrating external payment networks would require timeout budgets, async patterns, or webhook callbacks.

---

## 6. Trade-Offs

| Decision | Benefit | Cost |
|----------|---------|------|
| **Synchronous processing** | Simple client contract; immediate status in response | Limits throughput; ties API latency to processing time |
| **Monolithic deployment** | Low operational complexity; easy local development | Cannot scale processor and API independently without decomposition |
| **Topic-per-outcome** (`completed` / `failed`) | Consumers subscribe only to relevant events | More topics to manage; cross-cutting analytics need multiple subscriptions |
| **In-process Kafka consumer** | Demonstrates end-to-end flow in one JAR | Not representative of production microservice boundaries |
| **Hibernate ddl-auto: update** | Fast iteration in development | Unsuitable for production schema management; use Flyway/Liquibase instead |
| **Idempotency via DB unique key** | Strong guarantee, simple implementation | Requires clients to generate and persist keys; no TTL on keys |
| **Kafka publish inside @Transactional** | Simple ordering (save then publish) | Risk of inconsistency if Kafka succeeds but transaction rolls back, or vice versa |
| **Console logging for notifications** | Zero external dependencies in demo | No delivery guarantees, observability, or retry semantics |

---

## 7. Future Extensibility

The current architecture defines clear extension points:

### PaymentProcessorService → Payment Gateway Adapter

Replace in-memory rules with calls to card networks, bank APIs, or internal ledger services. The interface boundary (`Payment` → `PaymentStatus`) remains stable.

### Extract Notification Consumer

Move `PaymentEventConsumer` to a separate service subscribed to the same topics. The hub publishes events only; notification, email, and SMS logic live elsewhere.

### Additional Kafka Topics

New topics (e.g., `payment.initiated`, `payment.refunded`) can be added without changing the REST contract. Event schema evolution should use backward-compatible JSON or adopt Avro/Schema Registry.

### Cross-Cutting Enhancements (from roadmap)

| Enhancement | Purpose |
|-------------|---------|
| Redis caching | Hot-path payment lookups |
| Fraud scoring service | Pre-processing risk evaluation |
| Dead-letter queue | Poison message isolation |
| Retry mechanism | Transient failure recovery |
| Distributed tracing | End-to-end request correlation |
| OpenAPI / Swagger | API contract documentation |
| Authentication / authorization | Client identity and access control |
| Outbox pattern | Reliable DB + Kafka consistency |
| Amazon MSK + IAM auth | Managed Kafka in AWS (`application-cloud.properties` prepared) |

### Multi-Service Decomposition

Natural service boundaries if the system grows:

```text
payment-api          → HTTP + orchestration
payment-processor    → rules + external integrations
payment-ledger       → account balances
notification-service → Kafka consumer
```

---

## 8. Deployment Model

### Local Development

Docker Compose orchestrates the full stack:

| Service | Container | Port |
|---------|-----------|------|
| PostgreSQL 16 | `paymenthub-postgres` | 5432 |
| Zookeeper | `paymenthub-zookeeper` | 2181 |
| Kafka | `paymenthub-kafka` | 9092 |
| Kafka UI | `paymenthub-kafka-ui` | 8081 |
| Payment Hub | `paymenthub-app` | 8080 |

The application can also run via `./mvnw spring-boot:run` against locally exposed infrastructure.

### Container Build

Multi-stage `Dockerfile`:

1. **Build stage** — Eclipse Temurin 21 JDK, Maven wrapper, `./mvnw clean package -DskipTests`
2. **Runtime stage** — Eclipse Temurin 21 JRE, fat JAR on port 8080

Environment variables override datasource and Kafka bootstrap servers when running in Compose:

```text
SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/paymenthub
SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

### Planned Cloud Deployment (AWS)

```text
Source → Docker build → Amazon ECR → ECS Fargate (behind ALB)
                              ↓
                    Amazon RDS (PostgreSQL)
                    Amazon SSM Parameter Store / Secrets Manager
                    Amazon MSK (Kafka, SASL_SSL + IAM)
```

`application-cloud.properties` configures MSK IAM authentication for production Kafka connectivity. ECS tasks receive secrets via environment injection; no credentials are baked into the image.

### Configuration Profiles

| Profile | Use case |
|---------|----------|
| Default (`application.yaml`) | Local development, plaintext Kafka |
| Cloud (`application-cloud.properties`) | AWS deployment, MSK IAM, externalized secrets |

---

## 9. Testing Strategy

### Current Test Coverage

The project includes a **Spring Boot context load test** (`PaymenthubApplicationTests`) that verifies the application starts and all beans wire correctly. This guards against configuration regressions, missing dependencies, and classpath issues.

`spring-kafka-test` is on the test classpath, enabling future embedded Kafka integration tests without external infrastructure.

### Recommended Test Layers

| Layer | What to test | Why |
|-------|--------------|-----|
| **Unit — PaymentProcessorService** | Each business rule (null amount, zero, over limit, same account, success) | Core domain logic; fast, no I/O |
| **Unit — PaymentEventProducer** | Topic resolution for COMPLETED vs FAILED; unsupported status | Ensures correct event routing |
| **Integration — PaymentService** | Full create flow with `@DataJpaTest` or Testcontainers PostgreSQL | Validates idempotency, persistence, status transitions |
| **Integration — Kafka** | Producer publishes expected payload; consumer receives and deserializes | Contract verification with `@EmbeddedKafka` |
| **API — PaymentController** | `@WebMvcTest` with MockMvc: validation errors, 201/404/409 responses | HTTP contract and status code mapping |
| **End-to-end** | POST payment → verify DB row → verify Kafka message → GET payment | Confidence in full pipeline; suitable for CI with Testcontainers |

### What Is Deliberately Not Unit-Tested in Isolation

- **GlobalExceptionHandler** — thin mapping layer; covered adequately by controller/API tests
- **DTO getters/setters** — no behavior; validation annotations tested via controller tests
- **Payment entity** — JPA mapping verified by integration tests

### Manual & Observability Testing

- **Postman / curl** — ad-hoc API verification during development
- **Kafka UI** (`localhost:8081`) — inspect topic messages, offsets, and consumer lag
- **Hibernate SQL logging** (`show-sql: true`) — trace persistence during development (disabled in production)

### CI Recommendations

Run `./mvnw verify` on every pull request. Extend the pipeline with Testcontainers-based integration tests so CI does not depend on a pre-running Docker Compose stack. Skip tests in the production Docker build (`-DskipTests`) is acceptable for image creation; CI must run tests before push to ECR.

---

## Appendix A — Technology Stack

| Category | Choice | Version |
|----------|--------|---------|
| Language | Java | 21 |
| Framework | Spring Boot | 3.5.14 |
| ORM | Spring Data JPA / Hibernate | (Boot managed) |
| Messaging | Spring Kafka | (Boot managed) |
| Database | PostgreSQL | 16 |
| Broker | Apache Kafka (Confluent Platform) | 7.6.1 |
| Build | Maven | Wrapper included |
| Container base | Eclipse Temurin | 21 |

## Appendix B — Kafka Topic Reference

| Topic | Published when | Message key | Consumer group |
|-------|----------------|-------------|----------------|
| `payment.completed` | Status = COMPLETED | `paymentId` | `paymenthub-notification-service` |
| `payment.failed` | Status = FAILED | `paymentId` | `paymenthub-notification-service` |

## Appendix C — API Error Reference

| HTTP Status | Trigger |
|-------------|---------|
| 400 Bad Request | Validation failure on `CreatePaymentRequest` |
| 404 Not Found | Unknown `paymentId` |
| 409 Conflict | Duplicate `idempotencyKey` |
| 500 Internal Server Error | Unexpected exception; unsupported status for Kafka publish |
