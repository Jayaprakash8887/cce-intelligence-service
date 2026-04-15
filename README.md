# CCE Intelligence Service

The **delivery engine** of the CCE platform. Consumes intelligence trigger events from the Compliance Service via Kafka, builds **FHIR R4-compliant payloads** (`CommunicationRequest` / `Task`), resolves routing via **target subscriptions**, and delivers actions to registered **Receiver Adaptors** via webhook.

> The Compliance Service evaluates *when* and *what* to act on. This service handles *where* (routing) and *how* (FHIR payload + webhook delivery).

## Architecture

```
Compliance Service → Kafka → Intelligence Consumer → Intelligence Engine
  → Load ActionRun + ActionDefinition
  → Build FHIR Payload (CommunicationRequest or Task)
  → Resolve Target Subscriptions
  → Fan-out Webhook Delivery → Receiver Adaptors
```

## Tech Stack

| Concern | Technology |
|---------|------------|
| Language | Java 21 (LTS) |
| Framework | Spring Boot 3.4.x |
| Build | Gradle 8.x |
| Database | PostgreSQL 16+ (shared `cce_collector`) |
| Messaging | Apache Kafka 3.7+ (KRaft) |
| HTTP Client | Spring WebClient |
| Observability | Micrometer + Prometheus |

## Quick Start

```bash
# Prerequisites: JDK 21+, Docker (for PostgreSQL + Kafka)
docker compose up -d
./gradlew bootRun
```

Health check: `http://localhost:8083/actuator/health`

## Database

The service owns 4 tables and reads 2 from the Compliance Service:

| Table | Owner | Purpose |
|-------|-------|---------|
| `delivery_run` | Intelligence | Delivery lifecycle per (action_run × adaptor) |
| `receiver_adaptor` | Intelligence | Registered webhook endpoints |
| `target_subscription` | Intelligence | Many-to-many routing map |
| `delivery_audit_log` | Intelligence | Audit trail |
| `action_run` | Compliance (read-only) | FK anchor for delivery tracking |
| `action_definition` | Compliance (read-only) | Action type, severity, target |

## Kafka Topics

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `cce.intelligence.triggers` | Inbound | Trigger events from Compliance Service |
| `cce.intelligence.triggers.dlq` | Outbound | Dead-letter queue for failed processing |

## REST API

All requests arrive via the **CCE Gateway Service** (pre-authenticated).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/delivery-runs` | List delivery runs (filtered) |
| `GET` | `/v1/delivery-runs/{id}` | Get delivery run by ID |
| `POST` | `/v1/delivery-runs/{id}/cancel` | Cancel a delivery run |
| `GET` | `/v1/receiver-adaptors` | List receiver adaptors |
| `POST` | `/v1/receiver-adaptors` | Register a receiver adaptor |
| `PUT` | `/v1/receiver-adaptors/{id}` | Update a receiver adaptor |
| `DELETE` | `/v1/receiver-adaptors/{id}` | Delete a receiver adaptor |
| `GET` | `/v1/target-subscriptions` | List target subscriptions |
| `POST` | `/v1/target-subscriptions` | Create a target subscription |
| `PUT` | `/v1/target-subscriptions/{id}` | Update a target subscription |
| `DELETE` | `/v1/target-subscriptions/{id}` | Delete a target subscription |

## Project Structure

~30 source files across 11 packages. Key components:

- **`engine/IntelligenceEngine`** — Core orchestrator (trigger → payload → route → deliver)
- **`engine/FhirPayloadBuilder`** — Builds FHIR CommunicationRequest or Task
- **`engine/SubscriptionRouter`** — Resolves (protocol, target) → adaptors
- **`engine/ActionDispatcher`** — Fan-out webhook delivery

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture Overview](docs/architecture-overview.md) | System context, pipeline, FHIR payloads, routing model |
| [Flow Diagrams](docs/flow-diagrams.md) | Mermaid diagrams for all processing flows |
| [API Reference](docs/api-reference.md) | REST API endpoints and request/response schemas |
| [Data Dictionary](docs/data-dictionary.md) | Database schema and entity relationships |
| [Kafka Events](docs/kafka-events.md) | Kafka topic contracts and event schemas |
| [Developer Setup](docs/developer-setup.md) | Local development environment setup |