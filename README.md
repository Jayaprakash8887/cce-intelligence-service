# CCE Intelligence Service

The **delivery engine** of the CCE platform. Consumes **self-contained** intelligence trigger events from the Compliance Service via Kafka, builds **FHIR R4-compliant payloads** (`CommunicationRequest` / `Task`), resolves routing via **channel subscriptions** (with step-level granularity), and delivers actions to registered **Receiver Adaptors** via webhook.

> The Compliance Service evaluates *when* and *what* to act on, resolving all metadata into a self-contained trigger event. This service handles *where* (step-level routing via channel subscriptions) and *how* (FHIR payload + webhook delivery) — with **zero Compliance table reads** on the hot path.

## Architecture

```
Compliance Service → Kafka → Intelligence Consumer → Intelligence Engine
  → Build FHIR Payload from trigger event (CommunicationRequest or Task)
  → Resolve Channel Subscriptions (protocol × action_id × channel)
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

The service owns 4 tables (zero read-only Compliance dependencies at runtime):

| Table | Owner | Purpose |
|-------|-------|--------|
| `intelligence_delivery` | Intelligence | Delivery lifecycle per (intelligence_event × adaptor) |
| `receiver_adaptor` | Intelligence | Registered webhook endpoints |
| `channel_subscription` | Intelligence | Many-to-many routing map (protocol × action_id × channel → adaptors) |
| `intelligence_delivery_audit_log` | Intelligence | Audit trail |

## Kafka Topics

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `cce.intelligence.triggers` | Inbound | Trigger events from Compliance Service |
| `cce.intelligence.triggers.dlq` | Outbound | Dead-letter queue for failed processing |

## REST API

All requests arrive via the **CCE Gateway Service** (pre-authenticated).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/intelligence-deliveries` | List intelligence deliveries (filtered) |
| `GET` | `/v1/intelligence-deliveries/{id}` | Get intelligence delivery by ID |
| `POST` | `/v1/intelligence-deliveries/{id}/cancel` | Cancel a intelligence delivery |
| `GET` | `/v1/receiver-adaptors` | List receiver adaptors |
| `POST` | `/v1/receiver-adaptors` | Register a receiver adaptor |
| `PUT` | `/v1/receiver-adaptors/{id}` | Update a receiver adaptor |
| `DELETE` | `/v1/receiver-adaptors/{id}` | Delete a receiver adaptor |
| `GET` | `/v1/channel-subscriptions` | List channel subscriptions |
| `POST` | `/v1/channel-subscriptions` | Create a channel subscription |
| `PUT` | `/v1/channel-subscriptions/{id}` | Update a channel subscription |
| `DELETE` | `/v1/channel-subscriptions/{id}` | Delete a channel subscription |

## Project Structure

~26 source files across 10 packages. Key components:

- **`engine/IntelligenceEngine`** — Core orchestrator (trigger → payload → route → deliver)
- **`engine/FhirPayloadBuilder`** — Builds FHIR CommunicationRequest or Task from trigger event fields
- **`engine/SubscriptionRouter`** — Resolves (protocol, actionId, channel) → adaptors with step-level precedence
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