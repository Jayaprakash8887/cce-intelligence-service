# Kafka & Event Architecture

## 1. Overview

The CCE Intelligence Service **consumes** intelligence triggers from the Compliance Service via Kafka. It does not produce to any Kafka topic — action delivery is via webhook HTTP POST to Receiver Adaptors.

```mermaid
graph LR
    subgraph Inbound Topics
        T1["cce.intelligence.triggers"]
    end

    subgraph CCE Intelligence Service
        C1["IntelligenceTriggerConsumer"]
        EH["DefaultErrorHandler<br/>retry + DLQ"]
        ENGINE["IntelligenceEngine"]
        ROUTER["SubscriptionRouter"]
    end

    subgraph DLQ Topics
        D1["cce.intelligence.triggers.dlq"]
    end

    subgraph Delivery
        WH["Receiver Adaptors<br/>(Webhook POST)"]
    end

    T1 --> C1
    C1 --> ENGINE
    ENGINE --> ROUTER
    ROUTER --> WH
    C1 -.->|"on failure"| EH
    EH -->|"after retries"| D1

    classDef inbound fill:#3498DB,stroke:#2980B9,color:white
    classDef consumer fill:#2ECC71,stroke:#27AE60,color:white
    classDef dlq fill:#E74C3C,stroke:#C0392B,color:white
    classDef errorHandler fill:#F39C12,stroke:#D35400,color:white
    classDef delivery fill:#9B59B6,stroke:#8E44AD,color:white

    class T1 inbound
    class C1,ENGINE,ROUTER consumer
    class D1 dlq
    class EH errorHandler
    class WH delivery
```

---

## 2. Topic Reference

| Topic | Direction | Partitions | Consumer Group | Description |
|---|---|---|---|---|
| `cce.intelligence.triggers` | Inbound | 25 | `cce-intelligence-service` | Intelligence triggers from Compliance Service (deviation + completion events) |
| `cce.intelligence.triggers.dlq` | DLQ | 25 | — | Dead letter queue for failed trigger processing |

The `cce.intelligence.triggers` topic is declared as a `NewTopic` bean by the **Compliance Service**'s `KafkaConfig`. The Intelligence Service declares only the DLQ topic.

---

## 3. Consumer Configuration

### 3.1 Common Settings

```yaml
spring.kafka:
  bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
  consumer:
    group-id: cce-intelligence-service
    auto-offset-reset: earliest
    enable-auto-commit: false
    properties:
      isolation.level: read_committed
      max.poll.records: 100
      max.poll.interval.ms: 300000
  listener:
    ack-mode: record
    concurrency: 3
```

| Setting | Value | Rationale |
|---|---|---|
| `auto-offset-reset` | `earliest` | Process all triggers from beginning on first join |
| `enable-auto-commit` | `false` | Framework-managed offset commits |
| `isolation.level` | `read_committed` | Only consume committed messages |
| `max.poll.records` | `100` | Batch size per poll |
| `ack-mode` | `RECORD` | Offset committed per record after successful processing |
| `concurrency` | `3` | 3 concurrent listener threads per instance |

### 3.2 Deserialization

```java
// Consumer factory uses ErrorHandlingDeserializer wrapping JsonDeserializer
ConsumerFactory<String, IntelligenceTriggerEvent>
  Key:   StringDeserializer
  Value: ErrorHandlingDeserializer → JsonDeserializer<IntelligenceTriggerEvent>

// Trusted packages
spring.json.trusted.packages: "org.openphc.cce.intelligence.*"
```

### 3.3 Retry & Dead Letter Queue

```yaml
cce.kafka:
  retry:
    max-attempts: 3
    backoff-interval-ms: 1000
```

Spring Kafka's `DefaultErrorHandler` is configured with `FixedBackOff` and `DeadLetterPublishingRecoverer`:

1. **Retry** — up to `max-attempts` with `backoff-interval-ms` between attempts
2. **DLQ** — after exhausting retries, publish to `cce.intelligence.triggers.dlq`
3. **Acknowledge** — original offset committed to move past the failed record

### 3.4 DLQ Topic Provisioning

```yaml
cce.kafka:
  topics:
    intelligence-triggers-dlq: cce.intelligence.triggers.dlq
    default-partitions: 25
```

---

## 4. Inbound Message Schema — IntelligenceTriggerEvent

Published by the Compliance Service (v1.1.0+) when an intelligence action's condition matches — triggered by deviation detection (OVERDUE/MISSED) or step completion (e.g., late completion).

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440099",
  "subject": "260225-0002-5501",
  "actionRunId": "990e8400-e29b-41d4-a716-446655440010",
  "protocolInstanceId": "660e8400-e29b-41d4-a716-446655440001",
  "stepInstanceId": "770e8400-e29b-41d4-a716-446655440002",
  "deviationType": "overdue",
  "stepState": "overdue",
  "actionId": "anc-visit-2",
  "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
  "detectedAt": "2026-03-25T00:00:05Z"
}
```

### Field Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | UUID | Yes | Unique trigger event identifier. Correlates with Compliance Service's `action_run.intelligence_event_id`. |
| `subject` | String | Yes | Patient UPID (e.g., `260225-0002-5501`). Facility code embedded at positions 7-10. |
| `actionRunId` | UUID | Yes | FK to Compliance Service's `action_run.id` — idempotency anchor for `delivery_run`. Starting point for routing: `action_run → action_definition → target`. |
| `protocolInstanceId` | UUID | Yes | Protocol instance that triggered the event |
| `stepInstanceId` | UUID | Yes | Step instance that triggered the event |
| `deviationType` | String | No | `overdue`, `missed`, or `null` for late-completion triggers |
| `stepState` | String | Yes | Current step state (lowercase): `overdue`, `missed`, `completed` |
| `actionId` | String | Yes | PlanDefinition action ID (e.g., `anc-visit-2`) |
| `protocolCanonical` | String | Yes | Protocol `url\|version` |
| `detectedAt` | OffsetDateTime | Yes | When the event was detected |

### Message Key

**Kafka Key:** `protocolInstanceId` — ensures all triggers for the same protocol instance go to the same partition, maintaining ordering.

### Trigger Type Derivation

The `type` field was removed from the event schema (Compliance Service v1.1.0). The trigger type is derivable from `deviationType` + `stepState` and should be computed by the Intelligence Service for metrics and logging:

| Derived Type | Condition | Trigger Scenario |
|---|---|---|
| `deviation.overdue` | `deviationType == "overdue"` | Step transitioned DUE → OVERDUE |
| `deviation.missed` | `deviationType == "missed"` | Step transitioned OVERDUE → MISSED |
| `step.completed.late` | `deviationType == null && stepState == "completed"` | Step completed after being overdue (late completion) |

```java
String triggerType;
if ("overdue".equals(event.getDeviationType())) {
    triggerType = "deviation.overdue";
} else if ("missed".equals(event.getDeviationType())) {
    triggerType = "deviation.missed";
} else if ("completed".equals(event.getStepState())) {
    triggerType = "step.completed.late";
} else {
    triggerType = "unknown";
}
```

Use this derived type for the `cce.intelligence.triggers.received` counter tag.

---

## 5. Sample Messages

### 5.1 Overdue Deviation Trigger

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440099",
  "subject": "260225-0002-5501",
  "actionRunId": "990e8400-e29b-41d4-a716-446655440010",
  "protocolInstanceId": "660e8400-e29b-41d4-a716-446655440001",
  "stepInstanceId": "770e8400-e29b-41d4-a716-446655440002",
  "deviationType": "overdue",
  "stepState": "overdue",
  "actionId": "viral-load-check",
  "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/hiv-treatment|1.0",
  "detectedAt": "2026-03-25T00:00:05Z"
}
```

### 5.2 Missed Deviation Trigger

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440100",
  "subject": "260115-0001-7823",
  "actionRunId": "990e8400-e29b-41d4-a716-446655440011",
  "protocolInstanceId": "770e8400-e29b-41d4-a716-446655440003",
  "stepInstanceId": "880e8400-e29b-41d4-a716-446655440004",
  "deviationType": "missed",
  "stepState": "missed",
  "actionId": "anc-visit-3",
  "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
  "detectedAt": "2026-04-01T00:00:05Z"
}
```

### 5.3 Late Completion Trigger

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440101",
  "subject": "260225-0002-5501",
  "actionRunId": "990e8400-e29b-41d4-a716-446655440012",
  "protocolInstanceId": "660e8400-e29b-41d4-a716-446655440001",
  "stepInstanceId": "770e8400-e29b-41d4-a716-446655440002",
  "deviationType": null,
  "stepState": "completed",
  "actionId": "anc-visit-2",
  "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
  "detectedAt": "2026-04-02T10:30:00Z"
}
```

> **Note:** Late completion triggers have `deviationType: null` (not `"overdue"`). The trigger type `step.completed.late` is derived from `deviationType == null && stepState == "completed"`. See [Trigger Type Derivation](#trigger-type-derivation).

---

## 6. Consumer Implementation

### 6.1 IntelligenceTriggerConsumer

```java
@KafkaListener(
    topics = "${cce.kafka.topics.intelligence-triggers}",
    properties = {
        "spring.json.value.default.type=org.openphc.cce.intelligence.kafka.model.IntelligenceTriggerEvent"
    }
)
public void consume(IntelligenceTriggerEvent event) {
    MDC.put("correlationId", event.getId());
    MDC.put("subject", event.getSubject());
    MDC.put("actionRunId", event.getActionRunId().toString());
    try {
        intelligenceEngine.processTrigger(event);
        String triggerType = deriveTriggerType(event);
        receivedCounter.increment(triggerType);
    } catch (Exception e) {
        errorCounter.increment();
        throw e; // Propagate to DefaultErrorHandler for retry + DLQ
    } finally {
        MDC.clear();
    }
}
```

**Behavior on failure:** Exception propagates to `DefaultErrorHandler` → retries with backoff → routes to `cce.intelligence.triggers.dlq` after exhausting retries. Offset is committed automatically on success (`AckMode.RECORD`).

---

## 7. Ordering & Delivery Guarantees

| Guarantee | Mechanism |
|---|---|
| **At-least-once delivery** | `AckMode.RECORD` + `DefaultErrorHandler` + no auto-commit |
| **Idempotency** | `(actionRunId, targetSubscriptionId)` uniqueness on `delivery_run` |
| **Ordering (per partition)** | Key-based routing on `protocolInstanceId` ensures ordering per protocol instance |
| **Transactional reads** | `isolation.level=read_committed` prevents reading uncommitted |

---

## 8. Error Recovery Flow

```mermaid
flowchart TD
    A["Trigger arrives"] --> B{"Deserialize OK?"}
    B -->|"No"| C["ErrorHandlingDeserializer<br/>wraps error"]
    C --> DLQ_D["Route to cce.intelligence.triggers.dlq"]
    B -->|"Yes"| D{"Process OK?"}
    D -->|"Yes"| E["Acknowledge"]
    D -->|"No"| F["Increment error metric"]
    F --> G{"Retries remaining?"}
    G -->|"Yes"| H["Wait backoff (1s)"]
    H --> D
    G -->|"No"| I["Publish to cce.intelligence.triggers.dlq"]
    I --> J["Acknowledge original offset"]
    J --> K["Log DLQ routing"]
```
