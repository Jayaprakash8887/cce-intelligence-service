# Architecture & Design

## 1. System Context

The **CCE Intelligence Service** is the delivery engine of the CCE platform. It consumes **self-contained** intelligence trigger events published by the Compliance Service via Kafka, resolves routing via a many-to-many **channel subscription** model (with step-level granularity), builds FHIR-compliant payloads, and delivers actions to registered **Receiver Adaptors** via webhook.

> All REST requests arrive via the **CCE Gateway Service**, which validates OAuth tokens and enforces scopes (`delivery-runs:read|write`, `channel-subscriptions:read|write`, `admin`). The Intelligence Service does not handle authentication or authorization.

```mermaid
graph TB
    subgraph Dependent Services
        GATEWAY["CCE Gateway Service<br/>(Auth & Routing)"]
        RECEIVER_WH["Receiver Adaptors<br/>(Webhook endpoints)"]
    end

    subgraph CCE Intelligence Service
        CONSUMER["Intelligence Trigger<br/>Consumer"]
        ENGINE["Intelligence Engine<br/>(Core Orchestrator)"]
        BUILDER["FHIR Payload Builder<br/>(CommunicationRequest / Task)"]
        ROUTER["Subscription Router<br/>(channel_subscription)"]
        DISPATCHER["Action Dispatcher<br/>(fan-out)"]
        TRACKER["Delivery Run Tracker"]
        API["REST API<br/>(Spring MVC)"]
    end

    subgraph Shared Infrastructure
        DB[("PostgreSQL 16<br/>(cce_collector)")]
        KAFKA["Apache Kafka"]
    end

    subgraph CCE Compliance Service
        COMPLIANCE["Compliance Engine<br/>(publishes triggers)"]
    end

    COMPLIANCE --> KAFKA
    KAFKA -->|"cce.intelligence.triggers"| CONSUMER
    CONSUMER --> ENGINE
    ENGINE --> BUILDER
    ENGINE --> ROUTER
    ROUTER --> DB
    ROUTER --> DISPATCHER
    DISPATCHER --> RECEIVER_WH
    DISPATCHER --> TRACKER
    TRACKER --> DB
    API --> DB
    GATEWAY -->|"Authenticated Requests"| API

    classDef service fill:#4A90D9,stroke:#2C5F8A,color:white
    classDef external fill:#7B8D8E,stroke:#566573,color:white
    classDef data fill:#27AE60,stroke:#1E8449,color:white
    classDef broker fill:#E67E22,stroke:#D35400,color:white

    class CONSUMER,ENGINE,BUILDER,ROUTER,DISPATCHER,TRACKER,API service
    class GATEWAY,RECEIVER_WH external
    class DB data
    class KAFKA,COMPLIANCE broker
```

**This service does NOT handle:** event ingestion (Collector), protocol matching (Compliance), time-based transitions (Scheduler), analytics dashboards (Insights), or authentication/authorization (Gateway).

---

## 1.1 Compliance Service Contract

The **CCE Compliance Service** (v1.1.0+) is the upstream publisher. When a step's status changes, the Compliance Service's `IntelligenceActionEvaluator` evaluates PlanDefinition intelligence actions, resolves all metadata (action type, severity, intelligence channel, protocol definition), creates `IntelligenceEvent` records, and publishes a **self-contained** `IntelligenceTriggerEvent` to `cce.intelligence.triggers`.

```mermaid
sequenceDiagram
    participant CS as Compliance Service
    participant Kafka as Apache Kafka
    participant IS as Intelligence Service
    participant RA as Receiver Adaptors

    CS->>CS: Step state change detected (due, overdue, missed, or completed)
    CS->>CS: Evaluate intelligence action conditions
    CS->>CS: Resolve actionType, severity, intelligenceChannel, protocolDefinitionId
    CS->>CS: Create IntelligenceEventLog (published=false → true)
    CS->>Kafka: Publish IntelligenceTriggerEvent (fat event)
    Note over CS,Kafka: Event carries all metadata — no<br/>Compliance table reads needed by IS
    Kafka->>IS: Deliver trigger event
    IS->>IS: Resolve routing, build FHIR payload
    IS->>RA: Fan-out webhook delivery via channel subscriptions
```

#### Ownership Boundaries

| Aspect | Owner | Details |
|---|---|---|
| **Intelligence action evaluation** | Compliance Service | Evaluates PlanDefinition conditions, resolves all metadata, publishes self-contained triggers to Kafka |
| **`action_definition` table** | Compliance Service | FHIR ActivityDefinition resources — **not accessed** by Intelligence Service at runtime |
| **`intelligence_event_log` table** | Compliance Service | Tracks intelligence action execution and evaluation context. `intelligence_event_log.id` maps to `intelligenceEventId` in the trigger event — stored in `delivery_run` for traceability only |
| **Trigger consumption & routing** | Intelligence Service | Consumes self-contained triggers, resolves channel subscriptions (with step-level routing), fan-out delivery |
| **`receiver_adaptor` table** | Intelligence Service | Registered webhook endpoints (FHIR Endpoint resource in `definition` column) |
| **`channel_subscription` table** | Intelligence Service | Many-to-many routing map (protocol × action_id × channel → adaptors) |
| **`delivery_run` table** | Intelligence Service | Delivery lifecycle per (intelligence_event × adaptor); `intelligence_event_id` stored for traceability (maps to `intelligence_event_log.id`, not a runtime FK) |
| **`delivery_audit_log` table** | Intelligence Service | Audit trail for delivery operations |

> **Key invariant:** The Compliance Service evaluates *when* to act, *what* action to take, and publishes a self-contained trigger event with all resolved metadata. The Intelligence Service decides *where* to deliver it (step-level routing via channel subscriptions) and *how* (FHIR payload + webhook to receiver adaptors). This separation allows routing to evolve independently of clinical logic, and the fat event design eliminates all cross-service runtime dependencies.

---

## 2. Technology Stack

| Concern | Technology | Version |
|---------|------------|---------|
| Language | Java | 21 (LTS) |
| Framework | Spring Boot | 3.4.x |
| Build tool | Gradle | 8.x |
| Database | PostgreSQL | 16+ (shared `cce_collector` database) |
| Message broker | Apache Kafka | 3.7+ (KRaft mode) |
| DB access | Spring Data JPA + Hibernate | (Spring Boot managed) |
| DB migration | Flyway | (Spring Boot managed) |
| Connection pool | HikariCP | (Spring Boot default) |
| Expression evaluation | ~~Apache Johnzon JsonLogic~~ | ~~2.0.2~~ | *Removed — evaluation handled by Compliance Service* |
| HTTP client | Spring WebClient (reactive, non-blocking) | (Spring Boot managed) |
| Observability | Micrometer + Prometheus | (Spring Boot managed) |
| Testing | JUnit 5, Testcontainers, MockMvc | |

### Key Gradle Dependencies

```groovy
// Spring Boot starters
implementation 'org.springframework.boot:spring-boot-starter-web'
implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
implementation 'org.springframework.boot:spring-boot-starter-actuator'
implementation 'org.springframework.boot:spring-boot-starter-validation'
implementation 'org.springframework.boot:spring-boot-starter-webflux'  // WebClient for webhook delivery
implementation 'org.springframework.kafka:spring-kafka'

// Database
runtimeOnly 'org.postgresql:postgresql'
implementation 'org.flywaydb:flyway-core'
implementation 'org.flywaydb:flyway-database-postgresql'

// Expression evaluation — removed (evaluation handled by Compliance Service)
// implementation 'org.apache.johnzon:johnzon-jsonlogic:2.0.2'

// Observability
implementation 'io.micrometer:micrometer-registry-prometheus'

// Testing
testImplementation 'org.springframework.boot:spring-boot-starter-test'
testImplementation 'org.springframework.kafka:spring-kafka-test'
testImplementation 'org.testcontainers:postgresql'
testImplementation 'org.testcontainers:kafka'
testImplementation 'org.testcontainers:junit-jupiter'
testImplementation 'com.squareup.okhttp3:mockwebserver'  // Mock webhook endpoints
```


---

## 3. Package Structure

```
src/main/java/org/openphc/cce/intelligence/
├── IntelligenceServiceApplication.java           # @SpringBootApplication
├── config/
│   ├── KafkaConsumerConfig.java                   # Consumer factory, error handler, DLQ
│   ├── WebClientConfig.java                       # WebClient for webhook delivery
│   ├── JpaConfig.java                             # JPA/Hibernate settings
│   ├── AsyncConfig.java                           # @EnableAsync for audit writes
│   └── ObservabilityConfig.java                   # Custom metrics
├── domain/
│   ├── entity/
│   │   ├── DeliveryRun.java                       # Delivery lifecycle per (intelligence_event × adaptor)
│   │   ├── ReceiverAdaptor.java                   # Registered webhook endpoint
│   │   ├── ChannelSubscription.java               # Many-to-many routing map (with step-level action_id)
│   │   └── DeliveryAuditLog.java                  # Audit trail entry
│   ├── enums/
│   │   ├── DeliveryRunStatus.java                 # PENDING, EXECUTING, DELIVERED, FAILED, CANCELLED
│   │   ├── ActionType.java                        # NOTIFICATION, ESCALATION, COORDINATION
│   │   └── IntelligenceSeverity.java              # LOW, MEDIUM, HIGH, CRITICAL
│   └── repository/
│       ├── DeliveryRunRepository.java
│       ├── ReceiverAdaptorRepository.java
│       ├── ChannelSubscriptionRepository.java
│       └── DeliveryAuditLogRepository.java
├── engine/
│   ├── IntelligenceEngine.java                    # Core orchestrator — trigger → build payload → route → deliver
│   ├── FhirPayloadBuilder.java                    # Builds FHIR CommunicationRequest or Task from trigger event
│   ├── SubscriptionRouter.java                    # Resolve (protocol_definition_id, action_id, channel) → List<ReceiverAdaptor>
│   └── ActionDispatcher.java                      # Fan-out webhook delivery to subscribed adaptors
├── kafka/
│   ├── config/                                    # Consumer factory, topic bindings
│   ├── consumer/
│   │   └── IntelligenceTriggerConsumer.java       # @KafkaListener for cce.intelligence.triggers
│   └── model/
│       └── IntelligenceTriggerEvent.java          # Inbound Kafka message record
├── service/
│   ├── DeliveryRunService.java                    # Delivery run lifecycle management
│   ├── ReceiverAdaptorService.java                # Adaptor registration and lookup
│   ├── ChannelSubscriptionService.java            # Subscription management
│   └── DeliveryAuditService.java                  # @Async audit logging
├── webhook/
│   └── WebhookDeliveryClient.java                 # WebClient-based HTTP POST to adaptor endpoint
└── web/
    ├── controller/
    │   ├── DeliveryRunController.java
    │   ├── ReceiverAdaptorController.java
    │   └── ChannelSubscriptionController.java
    ├── dto/
    │   ├── DeliveryRunDto.java
    │   ├── ReceiverAdaptorDto.java
    │   ├── ChannelSubscriptionDto.java
    │   └── DtoMapper.java
    └── GlobalExceptionHandler.java

src/main/resources/
├── application.yml
├── application-docker.yml
└── db/migration/
    └── V1__create_intelligence_tables.sql

src/test/java/org/openphc/cce/intelligence/           # Unit tests
src/integrationTest/java/org/openphc/cce/intelligence/ # Integration tests
```

**Total:** ~26 source files across 10 packages.

---

## 4. Core Pipeline — IntelligenceEngine

The `IntelligenceEngine` is the central orchestrator. The pipeline is deliberately simple — the Compliance Service has already evaluated *when* and *what* to act on, and the trigger event carries all metadata. This service only handles *where* (routing) and *how* (FHIR payload + webhook delivery), with **zero Compliance table reads** on the hot path.

```mermaid
flowchart TD
    START["IntelligenceTriggerEvent received<br/>from cce.intelligence.triggers"] --> S1

    S1["Step 1: Idempotency Check<br/>(intelligenceEventId + subscriptions already delivered?)"]
    S1 -->|"All subscriptions delivered"| DUP["Return early — no-op"]
    S1 -->|"New or partial"| S2

    S2["Step 2: Resolve Channel Subscriptions<br/>(protocolDefinitionId, actionId, intelligenceChannel)<br/>→ subscribed adaptors"] --> S3

    S3{"Step 3: Fan-Out Delivery<br/>For each subscribed adaptor:"}
    S3 --> S4

    S4["Create DeliveryRun — PENDING<br/>→ Build FHIR Payload<br/>→ Dispatch Webhook<br/>→ Track Outcome"]
    S4 --> S5

    S5{"Delivery Result"}
    S5 -->|"Success"| DELIVERED["DeliveryRun → DELIVERED"]
    S5 -->|"Failure"| RETRY{"Retries remaining?"}
    RETRY -->|"Yes"| S4
    RETRY -->|"No"| FAILED["DeliveryRun → FAILED"]
```

### 4.1 Data Flow: Trigger Event → FHIR Payload

The trigger event is **self-contained** — all fields needed for routing and FHIR payload construction are carried directly in the event. No Compliance table lookups required.

```
IntelligenceTriggerEvent
  ├── actionType ───────────► FHIR kind (CommunicationRequest / Task / ServiceRequest) → mapped to Intelligence ActionType (NOTIFICATION / ESCALATION / COORDINATION)
  ├── severity ─────────────► FHIR priority mapping + extension; also used in actionType mapping (CommunicationRequest + HIGH/CRITICAL = ESCALATION, otherwise NOTIFICATION)
  ├── intelligenceChannel ───► FHIR recipient + routing lookup
  ├── protocolDefinitionId ─► channel_subscription routing key
  ├── actionId ─────────────► channel_subscription routing key (step-level) + FHIR about[1].display
  ├── subject ──────────────► FHIR subject.identifier
  ├── protocolCanonical ────► FHIR about[0].reference
  ├── stepState ────────────► FHIR extension (cce-step-state)
  ├── detectedAt ───────────► FHIR authoredOn
  ├── intelligenceEventId ───────► delivery_run.intelligence_event_id (traceability)
  └── actionDefinitionId ──► delivery_run.action_definition_id (traceability)
```

### 4.2 FHIR Payload Generation

The `FhirPayloadBuilder` constructs **FHIR R4-compliant payloads** directly from the trigger event fields. All structured data the receiver needs is in standard FHIR fields and CCE extensions. A default human-readable summary is generated for `payload.contentString` / `description`.

#### ActionType Mapping

The trigger event's `actionType` carries the FHIR `ActivityDefinition.kind` value (`CommunicationRequest`, `Task`, `ServiceRequest`). The Intelligence Service maps this to its own semantic `ActionType` at consumption time:

| Trigger Event `actionType` | Severity | Intelligence `ActionType` | FHIR Payload Resource |
|---|---|---|---|
| `CommunicationRequest` | `HIGH` or `CRITICAL` | `ESCALATION` | `CommunicationRequest` |
| `CommunicationRequest` | `LOW` or `MEDIUM` | `NOTIFICATION` | `CommunicationRequest` |
| `Task` | any | `COORDINATION` | `Task` |
| `ServiceRequest` | any | `COORDINATION` | `Task` |

The mapped `ActionType` is stored in `delivery_run.action_type` and used in the FHIR payload's `category` / `code` coding, the `contentString` summary, and the REST API response.

#### Payload Resource Types

| Action Type | FHIR Resource | Rationale |
|---|---|---|
| `NOTIFICATION` | `CommunicationRequest` | Standard FHIR resource for "send this message" semantics |
| `ESCALATION` | `CommunicationRequest` | Same structure, differentiated by `priority` and `category` |
| `COORDINATION` | `Task` | Standard FHIR resource for "perform this action" semantics |

#### CommunicationRequest Payload (NOTIFICATION / ESCALATION)

```json
{
  "resourceType": "CommunicationRequest",
  "identifier": [{
    "system": "http://openphc.org/fhir/delivery-run-id",
    "value": "aaaa-bbbb-cccc-dddd"
  }],
  "status": "active",
  "priority": "urgent",
  "category": [{
    "coding": [{
      "system": "http://openphc.org/fhir/CodeSystem/cce-action-type",
      "code": "ESCALATION",
      "display": "Escalation"
    }]
  }],
  "subject": {
    "identifier": { "system": "http://openphc.org/fhir/patient-upid", "value": "260225-0002-5501" }
  },
  "about": [
    { "reference": "PlanDefinition/anc-high-risk|2.1" },
    { "display": "anc-visit-2" }
  ],
  "payload": [{
    "contentString": "[HIGH] ESCALATION for patient 260225-0002-5501 — step anc-visit-2 overdue (PlanDefinition/anc-high-risk|2.1)"
  }],
  "recipient": [{ "display": "supervisor" }],
  "authoredOn": "2026-04-15T00:00:05Z",
  "extension": [
    {
      "url": "http://openphc.org/fhir/StructureDefinition/cce-severity",
      "valueCode": "high"
    },
    {
      "url": "http://openphc.org/fhir/StructureDefinition/cce-intelligence-event-id",
      "valueId": "intelligence-event-uuid"
    },
    {
      "url": "http://openphc.org/fhir/StructureDefinition/cce-step-state",
      "valueCode": "overdue"
    }
  ]
}
```

#### Task Payload (COORDINATION)

```json
{
  "resourceType": "Task",
  "identifier": [{
    "system": "http://openphc.org/fhir/delivery-run-id",
    "value": "aaaa-bbbb-cccc-dddd"
  }],
  "status": "requested",
  "intent": "order",
  "priority": "urgent",
  "code": {
    "coding": [{
      "system": "http://openphc.org/fhir/CodeSystem/cce-action-type",
      "code": "COORDINATION",
      "display": "Coordination"
    }]
  },
  "description": "[CRITICAL] COORDINATION for patient 260225-0002-5501 — step anc-visit-2 overdue (PlanDefinition/anc-high-risk|2.1)",
  "for": {
    "identifier": { "system": "http://openphc.org/fhir/patient-upid", "value": "260225-0002-5501" }
  },
  "authoredOn": "2026-04-15T00:00:05Z",
  "extension": [
    {
      "url": "http://openphc.org/fhir/StructureDefinition/cce-severity",
      "valueCode": "critical"
    },
    {
      "url": "http://openphc.org/fhir/StructureDefinition/cce-protocol-canonical",
      "valueCanonical": "PlanDefinition/anc-high-risk|2.1"
    },
    {
      "url": "http://openphc.org/fhir/StructureDefinition/cce-action-id",
      "valueString": "anc-visit-2"
    }
  ]
}
```

#### FHIR Field Mapping

| FHIR Field | CommunicationRequest | Task | Source |
|---|---|---|---|
| `identifier` | Delivery run ID | Delivery run ID | `delivery_run.id` |
| `status` | `active` | `requested` | Fixed per resource type |
| `priority` | Mapped from severity | Mapped from severity | `trigger.severity` |
| `category` / `code` | Action type coding | Action type coding | `trigger.actionType` |
| `subject` / `for` | Patient UPID | Patient UPID | `trigger.subject` |
| `about` | Protocol + action ID | — | `trigger.protocolCanonical`, `trigger.actionId` |
| `payload.contentString` / `description` | Auto-generated summary (includes stepState) | Auto-generated summary (includes stepState) | `FhirPayloadBuilder` |
| `recipient` | Channel name | — | `trigger.intelligenceChannel` |
| `authoredOn` | Detection time | Detection time | `trigger.detectedAt` |
| `extension.*` | Step state | Same | Trigger event fields |

#### Severity → FHIR Priority Mapping

| CCE Severity | FHIR `priority` |
|---|---|
| `LOW` | `routine` |
| `MEDIUM` | `urgent` |
| `HIGH` | `urgent` |
| `CRITICAL` | `asap` |

---

## 5. Channel Subscription Routing

### 5.1 Many-to-Many Routing Model

The Intelligence Service uses a **channel subscription** model that maps `(protocol_definition_id, action_id, channel)` → `List<ReceiverAdaptor>`. The `action_id` column is optional — when `NULL`, the subscription acts as a wildcard for any step in the protocol. Step-specific subscriptions take precedence over wildcards. This enables:

- **Step-level routing:** A single protocol can route the same `supervisor` channel to different adaptors depending on which step triggered the action (e.g., ANC visit alerts → CHW team lead; lab alerts → lab coordinator).
- **Multiple adaptors per channel:** A single channel (e.g., `supervisor`) in protocol A can deliver to both an SMS gateway and an in-app notification system.
- **One adaptor across protocols:** An adaptor can subscribe to channels across multiple protocols.
- **Protocol-scoped channel names:** Channel names like `supervisor`, `patient-reminder`, or `chw-alert` are meaningful within a protocol definition — different protocols can reuse the same channel name with different adaptor subscriptions.

```mermaid
erDiagram
    PROTOCOL_DEFINITION ||--o{ CHANNEL_SUBSCRIPTION : "scopes"
    RECEIVER_ADAPTOR ||--o{ CHANNEL_SUBSCRIPTION : "subscribes"
    CHANNEL_SUBSCRIPTION ||--o{ DELIVERY_RUN : "routes to"

    PROTOCOL_DEFINITION {
        uuid id PK
        varchar url
        varchar version
    }

    CHANNEL_SUBSCRIPTION {
        uuid id PK
        uuid protocol_definition_id FK
        varchar action_id
        varchar channel
        uuid receiver_adaptor_id FK
        varchar status
    }

    RECEIVER_ADAPTOR {
        uuid id PK
        varchar name
        jsonb definition
    }

    DELIVERY_RUN {
        uuid id PK
        uuid intelligence_event_id
        uuid channel_subscription_id FK
        varchar status
    }
```

### 5.2 Routing Flow

```mermaid
flowchart TD
    ACTION["Intelligence Action Fires<br/>channel = 'supervisor', actionId = 'anc-visit-2'"] --> LOOKUP["Query channel_subscription<br/>WHERE protocol_definition_id = :pdId<br/>AND channel = 'supervisor'<br/>AND action_id = 'anc-visit-2' OR action_id IS NULL<br/>AND status = 'ACTIVE'<br/>ORDER BY action_id NULLS LAST"]
    LOOKUP --> DEDUP["Deduplicate: step-specific<br/>subscriptions override wildcards"]
    DEDUP --> RESULT{"Subscriptions found?"}
    RESULT -->|"None"| FAIL["DeliveryRun → FAILED<br/>'No active subscription'"]
    RESULT -->|"1+ found"| FANOUT["Fan-out: create one DeliveryRun<br/>per subscribed adaptor"]
    FANOUT --> D1["DeliveryRun #1<br/>→ CHW Team Lead SMS"]
    FANOUT --> D2["DeliveryRun #2<br/>→ Dashboard Adaptor"]
```

### 5.3 Example: Step-Level Channel Subscriptions

| Protocol | action_id | Channel | Receiver Adaptor | Purpose |
|---|---|---|---|---|
| ANC High-Risk v2.1 | `anc-visit-2` | `supervisor` | CHW Team Lead SMS Gateway | Step-specific: ANC visit alerts to CHW lead |
| ANC High-Risk v2.1 | `lab-test-1` | `supervisor` | Lab Coordinator Dashboard | Step-specific: lab alerts to lab coordinator |
| ANC High-Risk v2.1 | `NULL` | `supervisor` | CCE Dashboard Adaptor | Wildcard: all other steps' supervisor alerts |
| ANC High-Risk v2.1 | `NULL` | `patient-reminder` | WhatsApp Bot Adaptor | Wildcard: all patient reminders |
| HIV Treatment v1.0 | `NULL` | `supervisor` | Musanze District Adaptor | Different adaptor for different protocol |
| HIV Treatment v1.0 | `NULL` | `lab-coordinator` | Lab System Adaptor | Lab-specific routing |
| Child Immunization v1.0 | `NULL` | `supervisor` | Kigali South SMS Gateway | Same adaptor, different protocol |

> **Step-level routing:** The `supervisor` channel in ANC High-Risk routes to the CHW Team Lead for `anc-visit-2`, to the Lab Coordinator for `lab-test-1`, and falls back to the CCE Dashboard for all other steps. This granularity is essential because a generic `ActivityDefinition/send-escalation` can be reused across rules with different routing needs.

---

## 6. State Machines

### 6.1 Delivery Run

```mermaid
stateDiagram-v2
    [*] --> PENDING : Created by engine
    PENDING --> EXECUTING : Dispatched to adaptor
    PENDING --> CANCELLED : Cancelled via API
    EXECUTING --> DELIVERED : HTTP 2xx received
    EXECUTING --> FAILED : Max retries exceeded or non-retryable error
    FAILED --> CANCELLED : Cancelled via API
    DELIVERED --> [*]
    CANCELLED --> [*]
```

**Status transitions:**

| From | To | Trigger |
|---|---|---|
| `PENDING` | `EXECUTING` | Webhook dispatch initiated |
| `PENDING` | `CANCELLED` | Manual cancellation via REST API |
| `EXECUTING` | `DELIVERED` | HTTP 2xx response from adaptor |
| `EXECUTING` | `FAILED` | Non-retryable error (4xx) or max retries exceeded (5xx/timeout) |
| `FAILED` | `CANCELLED` | Manual cancellation via REST API |

Terminal states: `DELIVERED`, `CANCELLED`.

### 6.2 Idempotency

- **Per-intelligence_event:** `(intelligence_event_id, channel_subscription_id)` unique constraint on `delivery_run` prevents duplicate processing for the same intelligence event + adaptor combination.
- **Cross-trigger:** If the Compliance Service publishes duplicate trigger events for the same `intelligence_event_log` record, the Intelligence Service's idempotency guard ensures each adaptor gets exactly one `delivery_run`.
- **Per-adaptor:** Within a single intelligence event's fan-out, each subscribed adaptor gets exactly one `delivery_run`.

---

## 7. Security

- **Authentication & Authorization:** Handled by the **CCE API Gateway**. This service does not implement security directly — all requests arrive pre-authenticated.
- **Webhook credentials:** Stored in `receiver_adaptor.config` JSONB (separate from the FHIR Endpoint in `definition`). The **external Receiver Adaptor operator** generates and manages their own auth credentials (API keys, bearer tokens, mTLS certs). A CCE admin registers the adaptor via `POST /v1/receiver-adaptors`, placing the operator-provided credentials into `config`. The `WebhookDeliveryClient` reads `authHeader` + `authValue` at dispatch time and injects them into the outbound HTTP request. The Intelligence Service never *issues* tokens — it only *stores and presents* credentials that the receiving system expects.
- **Credential protection:** `authValue` in `receiver_adaptor.config` should be encrypted at rest in production (e.g., via PostgreSQL pgcrypto or application-level encryption). Credentials are **never logged** — the `WebhookDeliveryClient` masks them in all log output. `authValue` is **never returned** in REST API responses — DTOs mask it (e.g., `sk-***123`).
- **Webhook request signing (HMAC):** Each Receiver Adaptor can configure a `webhookSecret` in `config`. When present, the `WebhookDeliveryClient` computes `HMAC-SHA256(webhookSecret, requestBody)` and sends it as the `X-CCE-Signature-256` header. The receiving system verifies the signature to ensure the request is authentically from the CCE platform. This prevents spoofing — critical for healthcare delivery endpoints.
- Actuator endpoints are publicly accessible for health checks and monitoring.

---

## 8. Observability

### 8.1 Metrics

| Metric | Type | Tags | Description |
|---|---|---|---|
| `cce.intelligence.triggers.received` | Counter | `step_state` | Triggers received from Kafka |
| `cce.intelligence.deliveries.dispatched` | Counter | `action_type`, `severity` | Deliveries dispatched to adaptors |
| `cce.intelligence.deliveries.delivered` | Counter | `action_type` | Successful deliveries |
| `cce.intelligence.deliveries.failed` | Counter | `action_type` | Failed deliveries |
| `cce.intelligence.webhook.duration` | Timer | `adaptor_name` | Webhook response time |
| `cce.intelligence.consumer.errors` | Counter | — | Consumer processing errors |
| `cce.intelligence.subscriptions.active` | Gauge | — | Active channel subscriptions |

### 8.2 Logging & Tracing

- **Format:** `timestamp [thread] [correlationId] level logger - message`
- **MDC fields:** `correlationId`, `intelligenceEventId`, `deliveryRunId`, `subject`, `protocolCanonical`
- **Tracing:** OpenTelemetry (OTLP), correlation propagated via trigger event metadata
- **Health:** `/actuator/health` (liveness + readiness), `/actuator/prometheus`

---

## 9. Error Handling

### 9.1 REST API

| Error Type | HTTP Status |
|---|---|
| Resource not found | 404 |
| Invalid input | 400 |
| State conflict | 409 |
| Invalid state transition | 422 |
| Internal error | 500 |

### 9.2 Kafka Consumer

- **Consumer errors:** Exception propagates to `DefaultErrorHandler` → retries with 1-second fixed backoff (up to 3 attempts) → routes to DLQ topic (`cce.intelligence.triggers.dlq`)
- **Deserialization errors:** `ErrorHandlingDeserializer` wraps errors gracefully, routes to DLQ

### 9.3 Webhook Delivery

| Response | Behavior |
|---|---|
| HTTP 2xx | Success → `DELIVERED` |
| HTTP 4xx | Non-retryable failure → `FAILED` immediately |
| HTTP 5xx / timeout | Retryable → retry up to `cce.intelligence.webhook.retry-attempts` (default: 3) with fixed delay |

---

## 10. Scaling

| Dimension | Strategy |
|---|---|
| **Horizontal** | Kafka consumer group enables multi-instance; partition assignment is automatic (25 partitions) |
| **Database** | Connection pool per instance (10 max) |
| **Kafka** | 3 concurrent listener threads per instance |
| **API** | Stateless — any instance serves any request |
| **Fan-out** | Delivery runs are created per-adaptor; concurrent webhook calls via WebClient's non-blocking I/O |
| **Data retention** | `delivery_run` and `delivery_audit_log` are high-growth tables. Partition by `created_at` using `pg_partman` or native PostgreSQL range partitioning. Archive partitions older than the configured retention period (default: 90 days) to cold storage. Regulatory retention requirements may extend this — consult compliance policy |
