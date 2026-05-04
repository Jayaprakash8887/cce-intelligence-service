# CCE Intelligence Service — AI Agent Instructions

## Architecture

Spring Boot 3.4.x / Java 21 microservice that consumes **self-contained intelligence triggers** from the Compliance Service via Kafka, resolves routing via **channel subscriptions** (many-to-many: protocol × action_id × channel → adaptors), builds FHIR R4-compliant payloads, and delivers actions to **Receiver Adaptors** via webhook. This is the **routing and delivery engine** of the CCE platform.

> **Naming:** This service is referred to as "Intelligence Engine" or "Intelligence Engine & Action Execution" in the CCE Solution Design v0.3. Implementation name: **Intelligence Service** (`cce-intelligence-service`).

**Core pipeline:** Kafka trigger (fat event) → idempotency check → resolve channel subscriptions (step-level routing) → fan-out: for each subscribed adaptor: build FHIR payload → create IntelligenceDelivery → webhook POST → track outcome.

> **Fat event design:** The Compliance Service resolves **all** metadata at publish time (action type, severity, intelligence channel, protocol definition ID, action definition ID). The Intelligence Service requires **zero Compliance table reads** on the hot path — no `@Immutable` entities, no read-only repositories.

## Key Conventions

- **Package:** `org.openphc.cce.intelligence` — ~26 source files across 11 packages
- **Entities:** 4 owned JPA entities (`IntelligenceDelivery`, `ReceiverAdaptor`, `ChannelSubscription`, `IntelligenceDeliveryAuditLog`) — **no read-only entities**
- **Enums:** Value-based enums (never ordinals) — `IntelligenceDeliveryStatus`, `ActionType`, `IntelligenceSeverity`
- **DTOs:** Separate DTOs in `web/dto/`, mapped via `DtoMapper` — never expose entities in REST responses
- **All timestamps:** `OffsetDateTime` in UTC (`hibernate.jdbc.time_zone=UTC`)
- **IDs:** `UUID` for all entity primary keys
- **Kafka:** Consumes `IntelligenceTriggerEvent` from `cce.intelligence.triggers`; does not produce to any topic
- **Schema migrations:** Flyway only (`spring.jpa.hibernate.ddl-auto=validate`) — never let Hibernate modify schema
- **Build tool:** Gradle 8.x
- **Response envelope:** `{ "data": ... }` for success, `{ "error": { "code": "...", "message": "..." } }` for errors

## Critical Design Patterns

- **Self-contained trigger events (fat event):** The Compliance Service publishes `IntelligenceTriggerEvent` with all metadata pre-resolved (`actionType`, `severity`, `intelligenceChannel`, `protocolDefinitionId`, `actionDefinitionId`). The Intelligence Service does **not** read any Compliance tables at runtime — no `action_definition`, `action_run`, `protocol_definition`, `protocol_instance`, `step_instance`. This eliminates all cross-service runtime dependencies.
- **Channel subscription routing (step-level):** Routing uses a many-to-many `channel_subscription` table mapping `(protocol_definition_id, action_id, channel)` → `List<ReceiverAdaptor>`. Channel names are protocol-scoped. The `action_id` column enables **step-level routing** — when set, the subscription applies only to that specific step; when `NULL`, it acts as a wildcard. Step-specific subscriptions take precedence over wildcards.
- **FHIR Endpoint for Receiver Adaptors:** The `receiver_adaptor` table stores a FHIR R4 **Endpoint** resource in the `definition` JSONB column (address, connection type, payload types). The `config` JSONB stores operational concerns (auth headers, retry overrides). This separates FHIR-standard adaptor identity from implementation-specific configuration.
- **Fan-out delivery:** One intelligence trigger fires → resolve channel subscriptions → find all subscribed adaptors → create one `IntelligenceDelivery` per adaptor → dispatch webhooks.
- **IntelligenceDelivery lifecycle:** `PENDING → EXECUTING → DELIVERED | FAILED | CANCELLED`. Each fan-out delivery creates a separate IntelligenceDelivery record.
- **Idempotency:** `(actionRunId, channelSubscriptionId)` compound key on `intelligence_delivery` prevents duplicate processing. Re-delivered Kafka messages produce no duplicate deliveries.
- **Table naming:** Intelligence Service owned tables use distinct names (`intelligence_delivery`, `intelligence_delivery_audit_log`, `channel_subscription`, `receiver_adaptor`) to avoid conflicts with Compliance Service tables in the shared `cce_collector` database.

## IntelligenceDelivery State Machine

`PENDING → EXECUTING → DELIVERED` (success path)
`PENDING → EXECUTING → FAILED` (delivery failure after retries)
`PENDING → CANCELLED` (manual cancellation via API)
`FAILED → CANCELLED` (manual cancellation via API)
Terminal states: `DELIVERED`, `CANCELLED`.

## Database Access

### Owned Tables (Read-Write) — 4 tables

| Table | Purpose |
|---|---|
| `receiver_adaptor` | FHIR Endpoint definition (`definition` JSONB) + operational config (`config` JSONB) |
| `channel_subscription` | Step-level routing: (protocol_definition_id, action_id, channel) → receiver_adaptor |
| `intelligence_delivery` | Delivery lifecycle per (action_run × adaptor); `action_run_id` stored for traceability only |
| `intelligence_delivery_audit_log` | Audit trail for delivery operations |

### No Read-Only Tables

The Intelligence Service does **not** read any Compliance Service tables at runtime. All metadata is carried in the fat trigger event. The `action_run_id` and `action_definition_id` columns on `intelligence_delivery` are stored for traceability and cross-service correlation only — not as runtime FKs.

### Shared Database Model

The Intelligence Service connects to the **same PostgreSQL database** (`cce_collector`) as all other CCE services. Infrastructure (PostgreSQL on port 5433, Kafka on port 9092) is deployed by the **CCE Collector Service**. The Intelligence Service's Flyway migration creates its 4 owned tables.

**Flyway configuration:** Use a dedicated migration prefix or `flyway.table` to avoid conflicts with other services' migration histories.

## Kafka Integration

### Topic Consumed

| Topic | Key | Consumer Group | Purpose |
|---|---|---|---|
| `cce.intelligence.triggers` | `actionRunId` | `cce-intelligence-service` | Self-contained intelligence triggers from Compliance Service |
| `cce.intelligence.triggers.dlq` | — | — | Dead letter queue for failed triggers |

### IntelligenceTriggerEvent Schema (Inbound)

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440099",
  "subject": "260225-0002-5501",
  "actionRunId": "990e8400-e29b-41d4-a716-446655440010",
  "actionDefinitionId": "aad-0001-0001-0001-000000000001",
  "protocolDefinitionId": "ppd-0001-0001-0001-000000000001",
  "actionType": "ESCALATION",
  "severity": "HIGH",
  "intelligenceChannel": "supervisor",
  "stepState": "overdue",
  "actionId": "anc-visit-2",
  "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
  "detectedAt": "2026-03-25T00:00:05Z"
}
```

> **Trigger type derivation:** Derived from `stepState` alone: `overdue` → `deviation.overdue`, `missed` → `deviation.missed`, `completed` → `step.completed.late`.

## API Endpoints

All endpoints prefixed with `/v1/`. Authentication handled by Gateway.

### Intelligence Deliveries (`intelligence-deliveries:read|write`)

| Method | Path | Description |
|---|---|---|
| GET | `/v1/intelligence-deliveries` | List intelligence deliveries (with filters + pagination) |
| GET | `/v1/intelligence-deliveries/{id}` | Get intelligence delivery by ID (full detail) |
| GET | `/v1/intelligence-deliveries/{id}/audit` | Get audit trail for run |
| POST | `/v1/intelligence-deliveries/{id}/cancel` | Cancel a PENDING/FAILED intelligence delivery |

### Channel Subscriptions (`channel-subscriptions:read|write`)

| Method | Path | Description |
|---|---|---|
| POST | `/v1/channel-subscriptions` | Create a channel subscription |
| GET | `/v1/channel-subscriptions` | List subscriptions (filter by protocol, channel, actionId, adaptor) |
| GET | `/v1/channel-subscriptions/{id}` | Get subscription by ID |
| PUT | `/v1/channel-subscriptions/{id}` | Update subscription |
| DELETE | `/v1/channel-subscriptions/{id}` | Delete subscription |

### Receiver Adaptors (`admin`)

| Method | Path | Description |
|---|---|---|
| POST | `/v1/receiver-adaptors` | Register a receiver adaptor (FHIR Endpoint definition + config) |
| GET | `/v1/receiver-adaptors` | List registered adaptors |
| GET | `/v1/receiver-adaptors/{id}` | Get adaptor by ID |
| PUT | `/v1/receiver-adaptors/{id}` | Update adaptor (definition + config) |
| DELETE | `/v1/receiver-adaptors/{id}` | Deregister adaptor |

### Response Envelope

```json
{ "data": { ... } }          // Success
{ "error": { "code": "...", "message": "..." } }  // Error
```

## Build & Run

```bash
./gradlew build -x test                # Fast build
./gradlew build                         # Build + all tests
cd /path/to/cce-collector-service && docker compose up -d  # Start shared PostgreSQL + Kafka
./gradlew bootRun                       # Run app (port 8085)
curl localhost:8085/actuator/health     # Health check
```

## Testing

- Unit tests: mocked dependencies — `src/test/java`
- Integration tests: Testcontainers (PostgreSQL + Kafka) — `src/integrationTest/java`
- API tests: MockMvc with `@WebMvcTest`
- Run unit tests: `./gradlew test`
- Run integration tests: `./gradlew integrationTest`

## Key Files to Read First

- `docs/architecture-overview.md` — core pipeline, channel subscription routing (step-level), state machines
- `docs/kafka-events.md` — IntelligenceTriggerEvent schema (fat event), consumer config, DLQ
- `docs/data-dictionary.md` — intelligence_delivery, channel_subscription, receiver_adaptor (FHIR Endpoint) schemas
- `docs/api-reference.md` — all REST endpoints with request/response examples
- `docs/developer-setup.md` — local setup, shared database requirement

## What's NOT in Scope (Release 1.0.0)

- **Action Definition CRUD** — owned by Compliance Service; Intelligence Service does not access it at runtime
- **Retry with exponential backoff** — failed webhook deliveries use fixed-interval retry. Exponential backoff deferred.
- **Receiver Adaptor health monitoring** — no circuit breaker on adaptor endpoints in 1.0.0
- **Coordination action tracking** — coordination actions are delivered but completion is tracked by the Compliance Service's normal event matching
- **Batch intelligence processing** — triggers processed individually (no batching optimization)
