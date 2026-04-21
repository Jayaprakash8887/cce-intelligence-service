# Flow Diagrams

> **CCE Intelligence Service** — Visual representation of core processing flows  
> All diagrams use [Mermaid](https://mermaid.js.org/) syntax.

---

## Table of Contents

1. [End-to-End Intelligence Pipeline](#1-end-to-end-intelligence-pipeline)
2. [Trigger Processing Sequence](#2-trigger-processing-sequence)
3. [Channel Subscription Routing](#3-channel-subscription-routing)
4. [Action Dispatch & Webhook Delivery](#4-action-dispatch--webhook-delivery)
5. [Delivery Run Lifecycle](#5-delivery-run-lifecycle)
6. [Retry & Error Handling](#6-retry--error-handling)
7. [REST API Flows](#7-rest-api-flows)

---

## 1. End-to-End Intelligence Pipeline

High-level data flow from Compliance Service trigger through to Receiver Adaptor delivery. The Compliance Service handles all condition evaluation and publishes a **self-contained** trigger event; this service is purely a routing and delivery engine with **zero Compliance table reads** on the hot path.

```mermaid
flowchart LR
    CS[Compliance Service] -->|IntelligenceTriggerEvent<br/>self-contained fat event| K[Kafka<br/>cce.intelligence.triggers]
    K -->|consume| IC[Intelligence<br/>Consumer]
    IC --> IE[Intelligence<br/>Engine]
    IE --> FB[FHIR Payload<br/>Builder]
    FB --> SR[Subscription<br/>Router]
    SR --> AD[Action<br/>Dispatcher]
    AD -->|HTTP POST| RA1[Receiver<br/>Adaptor #1]
    AD -->|HTTP POST| RA2[Receiver<br/>Adaptor #2]

    subgraph Intelligence Service
        IC
        IE
        FB
        SR
        AD
    end

    subgraph External
        RA1
        RA2
    end

    SR -.->|read channel_subscription| DB[(PostgreSQL)]
    AD -.->|write delivery_run| DB
```

---

## 2. Trigger Processing Sequence

Detailed sequence diagram showing the interaction between components when processing an intelligence trigger with fan-out delivery. The trigger event is self-contained — no Compliance table lookups needed.

```mermaid
sequenceDiagram
    participant Kafka as Kafka (cce.intelligence.triggers)
    participant Consumer as IntelligenceTriggerConsumer
    participant Engine as IntelligenceEngine
    participant DB as PostgreSQL
    participant Builder as FhirPayloadBuilder
    participant Router as SubscriptionRouter
    participant Dispatcher as ActionDispatcher
    participant Webhook1 as Receiver Adaptor #1
    participant Webhook2 as Receiver Adaptor #2

    Kafka->>Consumer: IntelligenceTriggerEvent (fat event)
    Consumer->>Engine: processTrigger(event)

    Note over Engine,DB: Step 1 — Idempotency Check
    Engine->>DB: findDeliveredSubscriptions(actionRunId)
    DB-->>Engine: already-delivered set (may be empty)

    Note over Engine,Router: Step 2 — Resolve Channel Subscriptions (step-level)
    Engine->>Router: findSubscriptions(protocolDefinitionId, actionId, intelligenceChannel)
    Router->>DB: query channel_subscription + receiver_adaptor<br/>WHERE action_id = :actionId OR action_id IS NULL<br/>ORDER BY action_id NULLS LAST
    DB-->>Router: ChannelSubscription list with adaptors
    Router->>Router: Deduplicate (step-specific overrides wildcard)
    Router-->>Engine: [Adaptor #1, Adaptor #2] minus already-delivered

    Note over Engine,Dispatcher: Step 3 — Fan-Out Delivery
    par Deliver to Adaptor #1
        Engine->>DB: save(DeliveryRun [PENDING] for Adaptor #1)
        Engine->>Builder: build(triggerEvent, deliveryRunId)
        Builder-->>Engine: FHIR CommunicationRequest / Task
        Engine->>Dispatcher: dispatch(deliveryRun, fhirPayload, adaptor1)
        Dispatcher->>DB: update(DeliveryRun [EXECUTING])
        Dispatcher->>Webhook1: HTTP POST (FHIR payload)
        Webhook1-->>Dispatcher: 200 OK
        Dispatcher->>DB: update(DeliveryRun [DELIVERED])
    and Deliver to Adaptor #2
        Engine->>DB: save(DeliveryRun [PENDING] for Adaptor #2)
        Engine->>Builder: build(triggerEvent, deliveryRunId)
        Builder-->>Engine: FHIR CommunicationRequest / Task
        Engine->>Dispatcher: dispatch(deliveryRun, fhirPayload, adaptor2)
        Dispatcher->>DB: update(DeliveryRun [EXECUTING])
        Dispatcher->>Webhook2: HTTP POST (FHIR payload)
        Webhook2-->>Dispatcher: 200 OK
        Dispatcher->>DB: update(DeliveryRun [DELIVERED])
    end

    Engine-->>Consumer: processing complete
    Consumer->>Kafka: acknowledge
```

---

## 3. Channel Subscription Routing

How the Intelligence Service resolves which Receiver Adaptors should receive a delivery for a given protocol + action_id + channel combination. Step-specific subscriptions (`action_id` not NULL) take precedence over wildcard subscriptions (`action_id` IS NULL).

```mermaid
flowchart TD
    ACTION["Action fires for ANC High-Risk v2.1<br/>actionId = 'anc-visit-2', channel = 'supervisor'"] --> QUERY["Query channel_subscription<br/>WHERE protocol_definition_id = :pdId<br/>AND channel = 'supervisor'<br/>AND (action_id = 'anc-visit-2' OR action_id IS NULL)<br/>AND status = 'ACTIVE'<br/>ORDER BY action_id NULLS LAST"]
    QUERY --> JOIN["JOIN receiver_adaptor<br/>WHERE status = 'ACTIVE'"]
    JOIN --> DEDUP["Deduplicate: step-specific<br/>subscriptions override wildcards<br/>for same adaptor"]
    DEDUP --> RESULT{Subscriptions found?}

    RESULT -->|None| FAIL["Create DeliveryRun<br/>status = FAILED<br/>error = 'No active subscription for channel'"]
    RESULT -->|1 adaptor| SINGLE["Create 1 DeliveryRun"]
    RESULT -->|N adaptors| FANOUT["Create N DeliveryRuns<br/>(one per adaptor)"]

    SINGLE --> DISPATCH["Dispatch webhook"]
    FANOUT --> D1["Dispatch to Adaptor #1"]
    FANOUT --> D2["Dispatch to Adaptor #2"]
    FANOUT --> DN["Dispatch to Adaptor #N"]
```

### Step-Level Routing Example

```mermaid
flowchart TD
    subgraph "ANC High-Risk v2.1"
        T1["actionId: anc-visit-2<br/>channel: supervisor"]
        T2["actionId: lab-test-1<br/>channel: supervisor"]
        T3["actionId: any other step<br/>channel: supervisor"]
        T4["channel: patient-reminder"]
    end

    subgraph Channel Subscriptions
        TS1["ANC + anc-visit-2 + supervisor → CHW Lead SMS"]
        TS2["ANC + lab-test-1 + supervisor → Lab Coordinator"]
        TS3["ANC + NULL + supervisor → Dashboard (wildcard)"]
        TS4["ANC + NULL + patient-reminder → WhatsApp Bot"]
    end

    subgraph Receiver Adaptors
        A1["CHW Lead SMS Gateway"]
        A2["Lab Coordinator Dashboard"]
        A3["CCE Dashboard Adaptor"]
        A4["WhatsApp Bot"]
    end

    T1 --> TS1 --> A1
    T2 --> TS2 --> A2
    T3 -->|wildcard fallback| TS3 --> A3
    T4 --> TS4 --> A4
```

### Cross-Protocol Routing Example

```mermaid
flowchart TD
    subgraph "ANC High-Risk v2.1"
        P1T1["channel: supervisor"]
        P1T2["channel: patient-reminder"]
    end

    subgraph "HIV Treatment v1.0"
        P2T1["channel: supervisor"]
        P2T2["channel: lab-coordinator"]
    end

    subgraph Channel Subscriptions
        TS1["ANC + supervisor → SMS Gateway"]
        TS2["ANC + supervisor → Dashboard"]
        TS3["ANC + patient-reminder → WhatsApp Bot"]
        TS4["HIV + supervisor → Musanze Adaptor"]
        TS5["HIV + lab-coordinator → Lab System"]
    end

    subgraph Receiver Adaptors
        A1["SMS Gateway"]
        A2["Dashboard Adaptor"]
        A3["WhatsApp Bot"]
        A4["Musanze Adaptor"]
        A5["Lab System Adaptor"]
    end

    P1T1 --> TS1 --> A1
    P1T1 --> TS2 --> A2
    P1T2 --> TS3 --> A3
    P2T1 --> TS4 --> A4
    P2T2 --> TS5 --> A5
```

---

## 4. Action Dispatch & Webhook Delivery

How the Intelligence Service delivers an action to each subscribed Receiver Adaptor via webhook.

```mermaid
sequenceDiagram
    participant Dispatcher as ActionDispatcher
    participant DB as PostgreSQL
    participant Adaptor as Receiver Adaptor (Webhook)
    participant Audit as DeliveryAuditService

    Dispatcher->>DB: update DeliveryRun status=EXECUTING
    Dispatcher->>Audit: log(DISPATCHED, adaptorName, endpointUrl)

    Dispatcher->>Adaptor: HTTP POST endpoint_url
    Note right of Adaptor: Headers:<br/>Content-Type: application/fhir+json<br/>X-CCE-Delivery-Run-Id: {runId}<br/>X-CCE-Action-Run-Id: {actionRunId}<br/>+ adaptor auth headers from config

    alt HTTP 2xx
        Adaptor-->>Dispatcher: 200 OK
        Dispatcher->>DB: update DeliveryRun status=DELIVERED, delivered_at=now()
        Dispatcher->>Audit: log(DELIVERED, httpStatus=200)
    else HTTP 4xx (non-retryable)
        Adaptor-->>Dispatcher: 400/401/403/404
        Dispatcher->>DB: update DeliveryRun status=FAILED
        Dispatcher->>Audit: log(FAILED, httpStatus, "non-retryable")
    else HTTP 5xx / timeout (retryable)
        Adaptor-->>Dispatcher: 500/503/timeout
        Dispatcher->>Dispatcher: retry (see Retry Flow)
    end
```

### Webhook Payload — FHIR Resource Generation

```mermaid
flowchart LR
    TE["TriggerEvent fields<br/>actionType, severity, intelligenceChannel,<br/>subject, facilityId, etc."] --> FB[FhirPayloadBuilder]
    DRI["DeliveryRun ID"] --> FB
    AT{"ActionType?"} --> FB
    FB -->|NOTIFICATION / ESCALATION| CR["FHIR CommunicationRequest"]
    FB -->|COORDINATION| TK["FHIR Task"]
    CR --> H[HTTP POST Body]
    TK --> H

    subgraph "HTTP POST to Receiver Adaptor"
        H
        Headers["Headers:<br/>Content-Type: application/fhir+json<br/>X-CCE-Delivery-Run-Id<br/>X-CCE-Action-Run-Id<br/>+ adaptor.config auth"]
    end
```

---

## 5. Delivery Run Lifecycle

State machine for `delivery_run.status` — all valid transitions.

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

    note right of PENDING
        DeliveryRun created,
        awaiting dispatch
    end note

    note right of EXECUTING
        Webhook call in progress
        (possibly retrying)
    end note

    note right of DELIVERED
        Terminal state.
        Successfully delivered.
    end note

    note right of FAILED
        Terminal state.
        Can be cancelled.
    end note
```

---

## 6. Retry & Error Handling

### 6.1 Webhook Delivery Retry

```mermaid
flowchart TD
    A[HTTP POST to Receiver Adaptor] --> B{Response?}

    B -->|2xx| C[DELIVERED]
    B -->|4xx| D[FAILED<br/>Non-retryable]
    B -->|5xx / Timeout| E{attempt < maxRetries?}

    E -->|Yes| F[Wait retryIntervalMs<br/>default: 2000ms]
    F --> G[Increment attempt_count]
    G --> H[Log: RETRIED audit event]
    H --> A

    E -->|No| I[FAILED<br/>Max retries exceeded]

    subgraph Retry Config
        direction LR
        RC1["maxRetries = 3"]
        RC2["retryInterval = 2000ms"]
        RC3["retryable: 5xx, timeout"]
        RC4["non-retryable: 4xx"]
    end
```

### 6.2 Kafka Consumer Error Handling

```mermaid
flowchart TD
    A[Receive IntelligenceTriggerEvent] --> B{Deserialize OK?}

    B -->|No| C["Send to DLQ<br/>cce.intelligence.triggers.dlq"]
    B -->|Yes| D{Process trigger}

    D -->|Success| E[Acknowledge record]
    D -->|Transient error| F{Retry count < 3?}
    F -->|Yes| G[Retry with backoff]
    G --> D
    F -->|No| H["Send to DLQ<br/>cce.intelligence.triggers.dlq"]

    C --> J[Alert ops:<br/>cce.intelligence.consumer.errors++]
    H --> J
```

---

## 7. REST API Flows

### 7.1 Create Channel Subscription

```mermaid
sequenceDiagram
    participant Client
    participant Controller as ChannelSubscriptionController
    participant Service as ChannelSubscriptionService
    participant DB as PostgreSQL

    Client->>Controller: POST /v1/channel-subscriptions
    Controller->>Controller: Validate request body
    alt Validation failed
        Controller-->>Client: 400 Bad Request
    end

    Controller->>Service: create(dto)
    Service->>DB: existsById(protocolDefinitionId)
    alt Protocol not found
        Service-->>Controller: NotFoundException
        Controller-->>Client: 404 Not Found
    end

    Service->>DB: existsById(receiverAdaptorId)
    alt Adaptor not found
        Service-->>Controller: NotFoundException
        Controller-->>Client: 404 Not Found
    end

    Service->>DB: existsByProtocolChannelActionIdAdaptor(...)
    alt Already exists (same protocol + action_id + channel + adaptor)
        Service-->>Controller: ConflictException
        Controller-->>Client: 409 Conflict
    end

    Service->>DB: save(ChannelSubscription)
    DB-->>Service: saved entity
    Service-->>Controller: ChannelSubscriptionDto
    Controller-->>Client: 201 Created
```

### 7.2 Cancel Delivery Run

```mermaid
sequenceDiagram
    participant Client
    participant Controller as DeliveryRunController
    participant Service as DeliveryRunService
    participant DB as PostgreSQL
    participant Audit as DeliveryAuditService

    Client->>Controller: POST /v1/delivery-runs/{id}/cancel
    Controller->>Service: cancel(id)
    Service->>DB: findById(id)
    alt Not found
        Service-->>Controller: NotFoundException
        Controller-->>Client: 404 Not Found
    end

    Service->>Service: validate status in {PENDING, FAILED}
    alt Invalid status
        Service-->>Controller: UnprocessableEntityException
        Controller-->>Client: 422 Unprocessable Entity
    end

    Service->>DB: update status=CANCELLED
    Service->>Audit: log(CANCELLED, actor=requestUser)
    Service-->>Controller: DeliveryRunDto
    Controller-->>Client: 200 OK
```

### 7.3 Delete Receiver Adaptor

```mermaid
sequenceDiagram
    participant Client
    participant Controller as ReceiverAdaptorController
    participant Service as ReceiverAdaptorService
    participant DB as PostgreSQL

    Client->>Controller: DELETE /v1/receiver-adaptors/{id}
    Controller->>Service: delete(id)
    Service->>DB: findById(id)
    alt Not found
        Service-->>Controller: NotFoundException
        Controller-->>Client: 404 Not Found
    end

    Service->>DB: existsActiveSubscriptions(adaptorId)
    alt Active subscriptions exist
        Service-->>Controller: UnprocessableEntityException
        Controller-->>Client: 422 — Active channel subscriptions reference this adaptor
    end

    Service->>DB: existsActiveDeliveryRuns(adaptorId)
    alt Active delivery runs exist
        Service-->>Controller: UnprocessableEntityException
        Controller-->>Client: 422 — Active delivery runs reference this adaptor
    end

    Service->>DB: delete(adaptor)
    Controller-->>Client: 204 No Content
```
