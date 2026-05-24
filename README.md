# Real-Time Payment Hub

A simplified real-time payment processing system built using:

- Java 21
- Spring Boot 3
- Apache Kafka
- PostgreSQL
- Docker
- AWS ECS Fargate (planned deployment)

This project demonstrates key concepts commonly used in enterprise-grade payment systems:

- synchronous payment processing
- event-driven architecture
- Kafka producer/consumer
- idempotency handling
- payment lifecycle management
- asynchronous downstream processing
- Docker-based infrastructure

---

# Architecture

## High-Level Flow

```text
Client
   ↓
Spring Boot Payment API
   ↓
Synchronous payment processing
   ↓
PostgreSQL persistence
   ↓
Kafka event publication
   ↓
Kafka consumers
   ↓
Notification / downstream processing
```

---

# Payment Lifecycle

```text
INITIATED
   ↓
PROCESSING
   ↓
COMPLETED

or

INITIATED
   ↓
PROCESSING
   ↓
FAILED
```

---

# Kafka Topics

| Topic | Description |
|---|---|
| payment.completed | Published when payment succeeds |
| payment.failed | Published when payment fails |

---

# Project Structure

```text
real-time-payment-hub/
│
├── src/main/java/com/demo/paymenthub/
│   ├── PaymentHubApplication.java
│   │
│   ├── controller/
│   │   └── PaymentController.java
│   │
│   ├── service/
│   │   ├── PaymentService.java
│   │   └── PaymentProcessorService.java
│   │
│   ├── kafka/
│   │   ├── PaymentEventProducer.java
│   │   ├── PaymentEventConsumer.java
│   │   └── PaymentEvent.java
│   │
│   ├── entity/
│   │   └── Payment.java
│   │
│   ├── repository/
│   │   └── PaymentRepository.java
│   │
│   ├── dto/
│   │   ├── CreatePaymentRequest.java
│   │   └── PaymentResponse.java
│   │
│   ├── enums/
│   │   └── PaymentStatus.java
│   │
│   └── exception/
│       └── GlobalExceptionHandler.java
│
├── src/main/resources/
│   └── application.yml
│
├── docker-compose.yml
├── Dockerfile
├── pom.xml
└── README.md
```

---

# Technologies Used

| Component | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3 |
| Messaging | Apache Kafka |
| Database | PostgreSQL |
| ORM | Spring Data JPA |
| Containerization | Docker |
| API Testing | Postman |
| Cloud Deployment | AWS ECS Fargate |
| Container Registry | AWS ECR |

---

# Running Locally

## Prerequisites

Install:

- Java 21
- Docker Desktop
- VS Code
- Maven (optional if using Maven Wrapper)

---

# Start Infrastructure

Run:

```bash
docker compose up -d
```

This starts:

- PostgreSQL
- Kafka
- Zookeeper
- Kafka UI

---

# Verify Docker Containers

```bash
docker ps
```

Expected containers:

```text
paymenthub-postgres
paymenthub-zookeeper
paymenthub-kafka
paymenthub-kafka-ui
```

---

# Run Spring Boot Application

```bash
.\mvnw.cmd spring-boot:run
```

Expected:

```text
Tomcat started on port 8080
Started PaymenthubApplication
```

---

# API Endpoints

## Health Check

```http
GET /payments/health
```

Response:

```text
Payment Hub is running
```

---

# Create Payment

```http
POST /payments
```

Request:

```json
{
  "fromAccount": "A1001",
  "toAccount": "B2001",
  "amount": 250.00,
  "currency": "MYR",
  "idempotencyKey": "demo-success-001"
}
```

Response:

```json
{
  "paymentId": "PAY-12345678",
  "fromAccount": "A1001",
  "toAccount": "B2001",
  "amount": 250.00,
  "currency": "MYR",
  "status": "COMPLETED"
}
```

---

# Get Payment

```http
GET /payments/{paymentId}
```

---

# Kafka Event Flow

## Successful Payment

```text
Payment API
   ↓
Payment Processor
   ↓
Database updated
   ↓
Kafka event published:
payment.completed
   ↓
Kafka consumer receives event
   ↓
Notification simulated
```

---

# Kafka UI

Open:

```text
http://localhost:8081
```

View:

- topics
- messages
- partitions
- offsets

---

# Idempotency Handling

Duplicate payment requests are prevented using:

```text
idempotencyKey
```

Repeated requests with the same key will return:

```text
409 Conflict
```

---

# Failure Scenarios

The demo simulates payment failures when:

- amount <= 0
- amount > 10000
- source and destination accounts are the same

These publish:

```text
payment.failed
```

---

# Docker-Based Development

Infrastructure is containerized using Docker Compose.

Benefits:

- consistent local environment
- simplified Kafka setup
- portable development workflow
- production-like infrastructure

---

# Planned AWS Deployment

## Deployment Architecture

```text
Docker Image
   ↓
Amazon ECR
   ↓
Amazon ECS Fargate
   ↓
Application Load Balancer
```

---

# Future Improvements

Potential enhancements:

- Redis caching
- fraud scoring service
- dead-letter queue
- retry mechanism
- distributed tracing
- metrics and observability
- OpenAPI / Swagger
- authentication / authorization
- multi-service decomposition
- Amazon MSK integration
- CI/CD pipeline

---

# Engineering Concepts Demonstrated

This project demonstrates:

- layered architecture
- synchronous + asynchronous processing
- event-driven systems
- Kafka producer/consumer patterns
- transactional persistence
- REST API design
- containerization
- cloud-native deployment patterns
- resilient payment processing concepts

---

# Author

Ray Chow