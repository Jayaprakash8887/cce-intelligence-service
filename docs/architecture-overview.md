# Architecture & Design

## 1. System Context

The **CCE Intelligence Service** is the delivery engine of the CCE platform. It consumes intelligence trigger events published by the Compliance Service via Kafka, re-evaluates PlanDefinition intelligence actions, resolves routing via a many-to-many **target subscription** model, renders message templates, and delivers actions to registered **Receiver Adaptors** via webhook.

> All REST requests arrive via the **CCE Gateway Service**, which validates OAuth tokens and enforces scopes (`delivery-runs:read|write`, `target-subscriptions:read|write`, `admin`). The Intelligence Service does not handle authentication or authorization.

```mermaid
graph TB
    subgraph External
        GATEWAY["CCE Gateway Service<br/>(Auth & Routing)"]
        RECEIVER_WH["Receiver Adaptors<br/>(Webhook endpoints)"]
    end

    subgraph CCE Intelligence Service
        CONSUMER["Intelligence Trigger<br/>Consumer"]
        ENGINE["Intelligence Engine<br/>(Core Orchestrator)"]
        EVALUATOR["Intelligence Action<br/>Evaluator (JSONLogic)"]
        RESOLVER["Action Definition<br/>Resolver"]
        RENDERER["Template Renderer"]
        ROUTER["Subscription Router<br/>(target_subscription)"]
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
    ENGINE --> EVALUATOR
    ENGINE --> RESOLVER
    RESOLVER --> DB
    ENGINE --> RENDERER
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

    class CONSUMER,ENGINE,EVALUATOR,RESOLVER,RENDERER,ROUTER,DISPATCHER,TRACKER,API service
    class GATEWAY,RECEIVER_WH external
    class DB data
    class KAFKA,COMPLIANCE broker
```

**This service does NOT handle:** event ingestion (Collector), protocol matching (Compliance), time-based transitions (Scheduler), analytics dashboards (Insights), or authentication/authorization (Gateway).

---

## 1.1 Compliance Service Contract

The **CCE Compliance Service** (v1.1.0+) is the upstream publisher. When a step deviation is detected (OVERDUE/MISSED) or a step is completed with certain conditions, the Compliance Service's `IntelligenceActionEvaluator` evaluates PlanDefinition intelligence actions, creates `ActionRun` records (TRIGGERED → PUBLISHED), and publishes `IntelligenceTriggerEvent` messages to `cce.intelligence.triggers`.

```mermaid
sequenceDiagram
    participant CS as Compliance Service
    participant Kafka as Apache Kafka
    participant IS as Intelligence Service
    participant RA as Receiver Adaptors

    CS->>CS: Deviation detected or step completed
    CS->>CS: Evaluate intelligence action conditions (JSONLogic/FHIRPath)
    CS->>CS: Create ActionRun (TRIGGERED → PUBLISHED)
    CS->>Kafka: Publish IntelligenceTriggerEvent<br/>(key: protocolInstanceId)
    Kafka->>IS: Deliver trigger event
    IS->>IS: Load context, re-evaluate rules, resolve routing
    IS->>RA: Fan-out webhook delivery via target subscriptions
```

#### Ownership Boundaries

| Aspect | Owner | Details |
|---|---|---|
| **Intelligence action evaluation** | Compliance Service | Evaluates PlanDefinition conditions, publishes triggers to Kafka |
| **`action_definition` table** | Compliance Service | FHIR ActivityDefinition resources (message templates, action type, severity, target) |
| **`action_run` table** | Compliance Service | Tracks trigger lifecycle (TRIGGERED → PUBLISHED) |
| **`action_run_context` table** | Compliance Service | Evaluation context (why an action fired) — available as read-only |
| **Trigger consumption & routing** | Intelligence Service | Consumes triggers, resolves target subscriptions, fan-out delivery |
| **`receiver_adaptor` table** | Intelligence Service | Registered webhook endpoints |
| **`target_subscription` table** | Intelligence Service | Many-to-many routing map (protocol × target → adaptors) |
| **`delivery_run` table** | Intelligence Service | Delivery lifecycle per (action_run × adaptor); FK to `action_run.id` |
| **`delivery_audit_log` table** | Intelligence Service | Audit trail for delivery operations |

> **Key invariant:** The Compliance Service evaluates *when* to act and *what* action to take. The Intelligence Service decides *where* to deliver it (routing via target subscriptions) and *how* (webhook to receiver adaptors). This separation allows routing to evolve independently of clinical logic.

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
| Expression evaluation | Apache Johnzon JsonLogic | 2.0.2 |
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

// Expression evaluation
implementation 'org.apache.johnzon:johnzon-jsonlogic:2.0.2'

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

**Not included:** HAPI FHIR (PlanDefinition intelligence actions are parsed as JSONB — no FHIR R4 runtime), Redis (no caching in 1.0.0).

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
│   │   ├── DeliveryRun.java                       # Delivery lifecycle per (action_run × adaptor)
│   │   ├── ReceiverAdaptor.java                   # Registered webhook endpoint
│   │   ├── TargetSubscription.java                # Many-to-many routing map
│   │   └── DeliveryAuditLog.java                  # Audit trail entry
│   ├── readonly/
│   │   ├── ActionDefinition.java                  # Read-only (@Immutable) — compliance-owned
│   │   ├── ActionRun.java                         # Read-only (@Immutable) — FK anchor for delivery_run
│   │   ├── ProtocolDefinition.java                # Read-only (@Immutable)
│   │   ├── ProtocolInstance.java                  # Read-only (@Immutable)
│   │   └── StepInstance.java                      # Read-only (@Immutable)
│   ├── enums/
│   │   ├── DeliveryRunStatus.java                 # PENDING, EXECUTING, DELIVERED, FAILED, CANCELLED
│   │   ├── ActionType.java                        # NOTIFICATION, ESCALATION, COORDINATION
│   │   ├── DeliveryMode.java                      # WEBHOOK (1.0.0); TOPIC_SUBSCRIPTION (future)
│   │   └── IntelligenceSeverity.java              # LOW, MEDIUM, HIGH, CRITICAL
│   └── repository/
│       ├── DeliveryRunRepository.java
│       ├── ReceiverAdaptorRepository.java
│       ├── TargetSubscriptionRepository.java
│       ├── DeliveryAuditLogRepository.java
│       ├── ActionDefinitionRepository.java        # Read-only
│       ├── ActionRunRepository.java               # Read-only — lookup by id for delivery anchoring
│       ├── ProtocolDefinitionRepository.java      # Read-only
│       ├── ProtocolInstanceRepository.java        # Read-only
│       └── StepInstanceRepository.java            # Read-only
├── engine/
│   ├── IntelligenceEngine.java                    # Core orchestrator — trigger → evaluate → route → deliver
│   ├── IntelligenceActionEvaluator.java                         # JSONLogic condition evaluation against step runtime
│   ├── ActionEvaluationContext.java                            # Record: stepState, daysOverdue, etc.
│   ├── IntelligenceAction.java                      # Record: actionId, condition, definitionCanonical, severity, target
│   ├── ActionDefinitionResolver.java              # Resolve definitionCanonical → ActionDefinition entity
│   ├── TemplateRenderer.java                      # Variable substitution in message templates
│   ├── SubscriptionRouter.java                    # Resolve (protocol_definition_id, target) → List<ReceiverAdaptor>
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
│   ├── TargetSubscriptionService.java             # Subscription management
│   └── DeliveryAuditService.java                  # @Async audit logging
├── webhook/
│   └── WebhookDeliveryClient.java                 # WebClient-based HTTP POST to adaptor endpoint
└── web/
    ├── controller/
    │   ├── DeliveryRunController.java
    │   ├── ReceiverAdaptorController.java
    │   └── TargetSubscriptionController.java
    ├── dto/
    │   ├── DeliveryRunDto.java
    │   ├── ReceiverAdaptorDto.java
    │   ├── TargetSubscriptionDto.java
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

**Total:** ~40 source files across 14 packages.

---

## 4. Core Pipeline — IntelligenceEngine

The `IntelligenceEngine` is the central orchestrator. All intelligence trigger processing flows through it:

```mermaid
flowchart TD
    START["IntelligenceTriggerEvent received<br/>from cce.intelligence.triggers"] --> S1

    S1["Step 1: Idempotency Check<br/>(actionRunId already processed?)"]
    S1 -->|"All subscriptions already delivered"| DUP["Return early (no-op)"]
    S1 -->|"New or partial"| S2

    S2["Step 2: Load Context<br/>action_run, step_instance,<br/>protocol_instance, protocol_definition"] --> S3

    S3["Step 3: Extract Intelligence Actions<br/>from PlanDefinition nested sub-actions<br/>matching trigger's actionId"] --> S4

    S4["Step 4: Build ActionEvaluationContext<br/>(stepState, daysOverdue, etc.)"] --> S5

    S5{"Step 5: Evaluate Each Action<br/>(JSONLogic condition)"}
    S5 -->|"true"| S6
    S5 -->|"false"| SKIP["Skip action"]

    S6["Step 6: Resolve Action Definition<br/>(definitionCanonical → ActionDefinition)<br/>Extract target + message template"] --> S7

    S7["Step 7: Resolve Target Subscriptions<br/>(protocol_definition_id, target)<br/>→ List&lt;ReceiverAdaptor&gt;"] --> S8

    S8{"Step 8: Fan-Out Delivery<br/>For each subscribed adaptor:"}
    S8 --> S9

    S9["Create DeliveryRun (PENDING)<br/>→ Render Template<br/>→ Dispatch Webhook<br/>→ Track Outcome"]
    S9 --> S10

    S10{"Delivery Result"}
    S10 -->|"Success"| DELIVERED["DeliveryRun → DELIVERED"]
    S10 -->|"Failure"| RETRY{"Retries remaining?"}
    RETRY -->|"Yes"| S9
    RETRY -->|"No"| FAILED["DeliveryRun → FAILED"]
```

### 4.1 Intelligence Actions in PlanDefinition

Intelligence rules are modeled as **nested sub-actions** within a PlanDefinition step action (`action.action[]`). The Compliance Service evaluates these conditions and publishes triggers when they match. The Intelligence Service re-evaluates the same rules to determine which specific sub-action fired and to resolve the `definitionCanonical` needed for routing and template rendering.

```json
{
  "id": "anc-visit-2",
  "title": "Second ANC Visit",
  "action": [
    {
      "id": "anc-visit-2-overdue-alert",
      "title": "Alert — overdue notification",
      "condition": [{
        "kind": "applicability",
        "expression": {
          "language": "text/jsonlogic",
          "expression": "{\"==\": [{\"var\": \"stepState\"}, \"overdue\"]}"
        }
      }],
      "definitionCanonical": "ActivityDefinition/anc-overdue-alert|1.0",
      "extension": [
        {
          "url": "http://openphc.org/fhir/StructureDefinition/intelligence-severity",
          "valueCode": "high"
        },
        {
          "url": "http://openphc.org/fhir/StructureDefinition/intelligence-target",
          "valueCode": "supervisor"
        }
      ]
    },
    {
      "id": "anc-visit-2-missed-escalation",
      "title": "Escalation — missed step",
      "condition": [{
        "kind": "applicability",
        "expression": {
          "language": "text/jsonlogic",
          "expression": "{\"and\": [{\"==\": [{\"var\": \"deviationType\"}, \"missed\"]}, {\"==\": [{\"var\": \"requiredBehavior\"}, \"must\"]}]}"
        }
      }],
      "definitionCanonical": "ActivityDefinition/anc-missed-escalation|1.0",
      "extension": [
        {
          "url": "http://openphc.org/fhir/StructureDefinition/intelligence-severity",
          "valueCode": "critical"
        },
        {
          "url": "http://openphc.org/fhir/StructureDefinition/intelligence-target",
          "valueCode": "supervisor"
        }
      ]
    }
  ]
}
```

### 4.2 ActionEvaluationContext Variable Binding

| Variable | Type | Source | Available On |
|---|---|---|---|
| `stepState` | String | `step_instance.state` (lowercase) | Both |
| `deviationType` | String | `trigger.deviationType` (lowercase) | When present |
| `daysOverdue` | Long | `ChronoUnit.DAYS.between(dueDate, now)` (≥ 0) | Deviation only |
| `daysPastMissedDate` | Long | `ChronoUnit.DAYS.between(missedDate, now)` (≥ 0) | MISSED only |
| `actionId` | String | `step_instance.action_id` | Both |
| `repeatIndex` | Integer | `step_instance.repeat_index` | Both |
| `requiredBehavior` | String | `step_instance.required_behavior` | Both |
| `completionStatus` | String | `step_instance.completion_status` (lowercase) | Completion only |
| `dueDate` | OffsetDateTime | `step_instance.due_date` | Both |
| `completedAt` | OffsetDateTime | `step_instance.completed_at` | Completion only |

### 4.3 Template Rendering

Message templates are stored in the Compliance Service's `action_definition.definition` JSONB (FHIR `ActivityDefinition` resource) as a FHIR extension:

```json
{
  "url": "http://openphc.org/fhir/StructureDefinition/cce-message-template",
  "valueString": "Patient {patient_id} step {action_id} is {days_overdue} days overdue at facility {facility_id}"
}
```

The `TemplateRenderer` extracts this extension, replaces `{variable}` placeholders with values sourced from the trigger event and read-only database tables (`step_instance`, `action_definition`), and produces the rendered payload sent to each Receiver Adaptor. Variables are computed at evaluation time from the database for accuracy, not from event metadata snapshots.

**Template Variables:**

| Variable | Source | Description |
|---|---|---|
| `{patient_id}` | `trigger.subject` | Patient UPID |
| `{action_id}` | `trigger.actionId` | PlanDefinition action ID |
| `{protocol_canonical}` | `trigger.protocolCanonical` | Protocol `url\|version` |
| `{facility_id}` | Extracted from `trigger.subject` UPID (positions 7-10: `260225-XXXX-5501`) | Source FOSA facility code |
| `{days_overdue}` | `ChronoUnit.DAYS.between(step_instance.due_date, now())`, clamped ≥ 0 | Days since due date |
| `{days_past_missed}` | `ChronoUnit.DAYS.between(step_instance.missed_date, now())`, clamped ≥ 0 | Days since missed date |
| `{severity}` | `action_definition.severity` (or PlanDefinition extension override) | Intelligence severity |
| `{deviation_type}` | `trigger.deviationType` | Deviation type (overdue/missed/null) |
| `{step_state}` | `trigger.stepState` | Current step state (lowercase) |
| `{due_date}` | `step_instance.due_date` | Step due date |
| `{overdue_date}` | `step_instance.overdue_date` | Overdue threshold date |
| `{missed_date}` | `step_instance.missed_date` | Missed threshold date |
| `{completed_at}` | `step_instance.completed_at` | Step completion timestamp |
| `{required_behavior}` | `step_instance.required_behavior` | Step requirement level (`must`, `could`, `must-unless-documented`) |
| `{completion_status}` | `step_instance.completion_status` (lowercase) | Completion status (`on_time`, `early`, `late`) |

---

## 5. Target Subscription Routing

### 5.1 Many-to-Many Routing Model

The Intelligence Service uses a **target subscription** model that maps `(protocol_definition_id, target)` → `List<ReceiverAdaptor>`. This enables:

- **Multiple adaptors per target:** A single target (e.g., `supervisor`) in protocol A can deliver to both an SMS gateway and an in-app notification system.
- **One adaptor across protocols:** A facility adaptor can subscribe to targets across multiple protocols.
- **Protocol-scoped target names:** Target names like `supervisor`, `patient-reminder`, or `chw-alert` are meaningful within a protocol definition — different protocols can reuse the same target name with different adaptor subscriptions.

```mermaid
erDiagram
    PROTOCOL_DEFINITION ||--o{ TARGET_SUBSCRIPTION : "scopes"
    RECEIVER_ADAPTOR ||--o{ TARGET_SUBSCRIPTION : "subscribes"
    TARGET_SUBSCRIPTION ||--o{ DELIVERY_RUN : "routes to"

    PROTOCOL_DEFINITION {
        uuid id PK
        varchar url
        varchar version
    }

    TARGET_SUBSCRIPTION {
        uuid id PK
        uuid protocol_definition_id FK
        varchar target
        uuid receiver_adaptor_id FK
        varchar status
    }

    RECEIVER_ADAPTOR {
        uuid id PK
        varchar name
        varchar endpoint_url
        varchar delivery_mode
    }

    DELIVERY_RUN {
        uuid id PK
        uuid action_run_id FK
        uuid target_subscription_id FK
        varchar status
    }
```

### 5.2 Routing Flow

```mermaid
flowchart TD
    ACTION["Intelligence Action Fires<br/>definitionCanonical = ActivityDefinition/anc-overdue-alert|1.0"] --> RESOLVE["Resolve ActionDefinition<br/>→ target = 'supervisor'"]
    RESOLVE --> LOOKUP["Query target_subscription<br/>WHERE protocol_definition_id = :pdId<br/>AND target = 'supervisor'<br/>AND status = 'ACTIVE'"]
    LOOKUP --> RESULT{"Subscriptions found?"}
    RESULT -->|"None"| FAIL["DeliveryRun → FAILED<br/>'No active subscription'"]
    RESULT -->|"1+ found"| FANOUT["Fan-out: create one DeliveryRun<br/>per subscribed adaptor"]
    FANOUT --> D1["DeliveryRun #1<br/>→ SMS Gateway Adaptor"]
    FANOUT --> D2["DeliveryRun #2<br/>→ In-App Notification Adaptor"]
```

### 5.3 Example: Multi-Protocol Target Subscriptions

| Protocol | Target | Receiver Adaptor | Purpose |
|---|---|---|---|
| ANC High-Risk v2.1 | `supervisor` | Kigali South SMS Gateway | SMS alert to supervisor |
| ANC High-Risk v2.1 | `supervisor` | CCE Dashboard Adaptor | In-app notification |
| ANC High-Risk v2.1 | `patient-reminder` | WhatsApp Bot Adaptor | Patient reminder via WhatsApp |
| HIV Treatment v1.0 | `supervisor` | Musanze District Adaptor | Different adaptor for different protocol |
| HIV Treatment v1.0 | `lab-coordinator` | Lab System Adaptor | Lab-specific routing |
| Child Immunization v1.0 | `supervisor` | Kigali South SMS Gateway | Same adaptor, different protocol |

> **Target name scoping:** The target `supervisor` appears in all three protocols but can route to entirely different sets of adaptors. This is the key benefit of protocol-scoped target subscriptions versus a global `target_type` matching approach.

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

- **Per-action_run:** `(action_run_id, target_subscription_id)` unique constraint on `delivery_run` prevents duplicate processing for the same action_run + adaptor combination.
- **Cross-trigger:** If the Compliance Service publishes duplicate trigger events for the same `action_run`, the Intelligence Service's idempotency guard ensures each adaptor gets exactly one `delivery_run`.
- **Per-adaptor:** Within a single action_run's fan-out, each subscribed adaptor gets exactly one `delivery_run`.

---

## 7. Security

- **Authentication & Authorization:** Handled by the **CCE API Gateway**. This service does not implement security directly — all requests arrive pre-authenticated.
- **Webhook credentials:** Stored in `receiver_adaptor.config` JSONB. Auth headers (API keys, bearer tokens) are injected by `WebhookDeliveryClient` at dispatch time. Credentials are never logged.
- Actuator endpoints are publicly accessible for health checks and monitoring.

---

## 8. Observability

### 8.1 Metrics

| Metric | Type | Tags | Description |
|---|---|---|---|
| `cce.intelligence.triggers.received` | Counter | `deviation_type` | Triggers received from Kafka |
| `cce.intelligence.actions.evaluated` | Counter | `result` | Rules evaluated (matched/skipped/error) |
| `cce.intelligence.deliveries.dispatched` | Counter | `action_type`, `severity` | Deliveries dispatched to adaptors |
| `cce.intelligence.deliveries.delivered` | Counter | `action_type` | Successful deliveries |
| `cce.intelligence.deliveries.failed` | Counter | `action_type` | Failed deliveries |
| `cce.intelligence.webhook.duration` | Timer | `adaptor_name` | Webhook response time |
| `cce.intelligence.consumer.errors` | Counter | — | Consumer processing errors |
| `cce.intelligence.subscriptions.active` | Gauge | — | Active target subscriptions |

### 8.2 Logging & Tracing

- **Format:** `timestamp [thread] [correlationId] level logger - message`
- **MDC fields:** `correlationId`, `actionRunId`, `deliveryRunId`, `subject`, `protocolCanonical`
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
- **Individual action errors:** If one intelligence action fails during evaluation, the remaining actions still process. The failed action is logged and skipped.

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
