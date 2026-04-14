# Flow Diagrams

> **CCE Intelligence Service** — Visual representation of core processing flows  
> All diagrams use [Mermaid](https://mermaid.js.org/) syntax.

---

## Table of Contents

1. [End-to-End Intelligence Pipeline](#1-end-to-end-intelligence-pipeline)
2. [Trigger Processing Sequence](#2-trigger-processing-sequence)
3. [Intelligence Action Evaluation](#3-intelligence-action-evaluation)
4. [Target Subscription Routing](#4-target-subscription-routing)
5. [Action Dispatch & Webhook Delivery](#5-action-dispatch--webhook-delivery)
6. [Delivery Run Lifecycle](#6-delivery-run-lifecycle)
7. [Retry & Error Handling](#7-retry--error-handling)
8. [REST API Flows](#8-rest-api-flows)

---

## 1. End-to-End Intelligence Pipeline

High-level data flow from Compliance Service trigger through to Receiver Adaptor delivery.

```mermaid
flowchart LR
    CS[Compliance Service] -->|IntelligenceTriggerEvent| K[Kafka<br/>cce.intelligence.triggers]
    K -->|consume| IC[Intelligence<br/>Consumer]
    IC --> IE[Intelligence<br/>Engine]
    IE --> RE[Action<br/>Evaluator]
    RE --> AR[Action Def<br/>Resolver]
    AR --> TR[Template<br/>Renderer]
    TR --> SR[Subscription<br/>Router]
    SR --> AD[Action<br/>Dispatcher]
    AD -->|HTTP POST| RA1[Receiver<br/>Adaptor #1]
    AD -->|HTTP POST| RA2[Receiver<br/>Adaptor #2]

    subgraph Intelligence Service
        IC
        IE
        RE
        AR
        TR
        SR
        AD
    end

    subgraph External
        RA1
        RA2
    end

    IE -.->|read| DB[(PostgreSQL)]
    SR -.->|read target_subscription| DB
    AD -.->|write delivery_run| DB
```

---

## 2. Trigger Processing Sequence

Detailed sequence diagram showing the interaction between components when processing an intelligence trigger with fan-out delivery.

```mermaid
sequenceDiagram
    participant Kafka as Kafka (cce.intelligence.triggers)
    participant Consumer as IntelligenceTriggerConsumer
    participant Engine as IntelligenceEngine
    participant DB as PostgreSQL
    participant Evaluator as IntelligenceActionEvaluator
    participant Resolver as ActionDefinitionResolver
    participant Renderer as TemplateRenderer
    participant Router as SubscriptionRouter
    participant Dispatcher as ActionDispatcher
    participant Webhook1 as Receiver Adaptor #1
    participant Webhook2 as Receiver Adaptor #2

    Kafka->>Consumer: IntelligenceTriggerEvent
    Consumer->>Engine: processTrigger(event)

    Note over Engine,DB: Step 1 — Load Context (starting from actionRunId)
    Engine->>DB: findById(actionRunId)
    DB-->>Engine: ActionRun (action_definition_id, protocol_instance_id, step_instance_id)
    Engine->>DB: findById(protocol_instance_id)
    DB-->>Engine: ProtocolInstance (with protocolDefinitionId)
    Engine->>DB: findById(step_instance_id)
    DB-->>Engine: StepInstance
    Engine->>DB: findById(protocolDefinitionId)
    DB-->>Engine: ProtocolDefinition (with PlanDefinition JSONB)

    Note over Engine,Evaluator: Step 2 — Extract & Evaluate Rules
    Engine->>Engine: extractIntelligenceActions(planDefinition, actionId)
    loop For each intelligence action
        Engine->>Evaluator: evaluate(action.condition, evaluationContext)
        Evaluator-->>Engine: matched = true/false

        opt matched = true
            Note over Engine,DB: Step 3 — Idempotency Check
            Engine->>DB: existsByActionRunIdAndTargetSubscriptionId(actionRunId, subscriptionId)
            DB-->>Engine: false (not yet processed)

            Note over Engine,Resolver: Step 4 — Resolve Action Definition
            Engine->>Resolver: resolve(action.definitionCanonical)
            Resolver->>DB: findByCanonicalUrlAndVersion(url, version)
            DB-->>Resolver: ActionDefinition (read-only from Compliance)
            Resolver-->>Engine: ActionDefinition (target, message template)

            Note over Engine,Renderer: Step 5 — Render Template
            Engine->>Renderer: render(actionDefinition, evaluationContext)
            Renderer-->>Engine: renderedPayload

            Note over Engine,Router: Step 6 — Resolve Target Subscriptions
            Engine->>Router: findSubscriptions(protocolDefinitionId, target)
            Router->>DB: query target_subscription + receiver_adaptor
            DB-->>Router: TargetSubscription list with adaptors
            Router-->>Engine: [Adaptor #1, Adaptor #2]

            Note over Engine,Dispatcher: Step 7 — Fan-Out Delivery
            par Deliver to Adaptor #1
                Engine->>DB: save(DeliveryRun [PENDING] for Adaptor #1)
                Engine->>Dispatcher: dispatch(deliveryRun, adaptor1)
                Dispatcher->>DB: update(DeliveryRun [EXECUTING])
                Dispatcher->>Webhook1: HTTP POST (renderedPayload)
                Webhook1-->>Dispatcher: 200 OK
                Dispatcher->>DB: update(DeliveryRun [DELIVERED])
            and Deliver to Adaptor #2
                Engine->>DB: save(DeliveryRun [PENDING] for Adaptor #2)
                Engine->>Dispatcher: dispatch(deliveryRun, adaptor2)
                Dispatcher->>DB: update(DeliveryRun [EXECUTING])
                Dispatcher->>Webhook2: HTTP POST (renderedPayload)
                Webhook2-->>Dispatcher: 200 OK
                Dispatcher->>DB: update(DeliveryRun [DELIVERED])
            end
        end
    end

    Engine-->>Consumer: processing complete
    Consumer->>Kafka: acknowledge
```

---

## 3. Intelligence Action Evaluation

How intelligence actions are extracted from PlanDefinition and evaluated using JSONLogic.

```mermaid
flowchart TD
    A[Load PlanDefinition from protocol_definition.definition] --> B[Find action matching trigger's actionId]
    B --> C[Extract nested sub-actions<br/>where condition.language = text/jsonlogic]
    C --> D{Sub-actions found?}
    D -->|No| Z[Skip — no intelligence actions for this step]
    D -->|Yes| E[Build ActionEvaluationContext]

    E --> F[For each sub-action / intelligence action]
    F --> G[Parse JSONLogic condition expression]
    G --> H[Apply JSONLogic with ActionEvaluationContext variables]
    H --> I{Condition evaluates to true?}
    I -->|No| J[Log: action skipped]
    I -->|Yes| K[Check idempotency:<br/>action_run_id + subscription_id in delivery_run?]
    K --> L{Already processed?}
    L -->|Yes| M[Log: duplicate, skip]
    L -->|No| N[Proceed to Action Resolution & Routing]

    J --> F
    M --> F
    N --> O[Done evaluating rules]

    subgraph ActionEvaluationContext Variables
        direction LR
        R1[stepState]
        R2[deviationType]
        R3[daysOverdue]
        R4[daysPastMissedDate]
        R5[requiredBehavior]
        R6[completionStatus]
    end

    E -.-> R1 & R2 & R3 & R4 & R5 & R6
```

### ActionEvaluationContext Variable Computation

```mermaid
flowchart LR
    SI[StepInstance] -->|state| stepState
    SI -->|due_date, now| daysOverdue["daysOverdue<br/>= daysBetween(dueDate, now)"]
    SI -->|missed_date, now| daysPastMissedDate["daysPastMissedDate<br/>= daysBetween(missedDate, now)"]
    SI -->|required_behavior| requiredBehavior
    SI -->|completion_status| completionStatus
    D[Trigger Event] -->|deviationType| deviationType["deviationType<br/>(null for late completion)"]
    PD[PlanDefinition<br/>sub-action extension] -->|intelligence-severity| severity
    PD -->|intelligence-target| target
```

---

## 4. Target Subscription Routing

How the Intelligence Service resolves which Receiver Adaptors should receive a delivery for a given protocol + target combination.

```mermaid
flowchart TD
    ACTION["Action fires for ANC High-Risk v2.1<br/>target = 'supervisor'"] --> QUERY["Query target_subscription<br/>WHERE protocol_definition_id = :pdId<br/>AND target = 'supervisor'<br/>AND status = 'ACTIVE'"]
    QUERY --> JOIN["JOIN receiver_adaptor<br/>WHERE status = 'ACTIVE'"]
    JOIN --> RESULT{Subscriptions found?}

    RESULT -->|None| FAIL["Create DeliveryRun<br/>status = FAILED<br/>error = 'No active subscription for target'"]
    RESULT -->|1 adaptor| SINGLE["Create 1 DeliveryRun"]
    RESULT -->|N adaptors| FANOUT["Create N DeliveryRuns<br/>(one per adaptor)"]

    SINGLE --> DISPATCH["Dispatch webhook"]
    FANOUT --> D1["Dispatch to Adaptor #1"]
    FANOUT --> D2["Dispatch to Adaptor #2"]
    FANOUT --> DN["Dispatch to Adaptor #N"]
```

### Cross-Protocol Routing Example

```mermaid
flowchart TD
    subgraph "ANC High-Risk v2.1"
        T1["target: supervisor"]
        T2["target: patient-reminder"]
    end

    subgraph "HIV Treatment v1.0"
        T3["target: supervisor"]
        T4["target: lab-coordinator"]
    end

    subgraph Target Subscriptions
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

    T1 --> TS1 --> A1
    T1 --> TS2 --> A2
    T2 --> TS3 --> A3
    T3 --> TS4 --> A4
    T4 --> TS5 --> A5
```

---

## 5. Action Dispatch & Webhook Delivery

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
    Note right of Adaptor: Headers:<br/>Content-Type: application/json<br/>X-CCE-Delivery-Run-Id: {runId}<br/>X-CCE-Action-Run-Id: {actionRunId}<br/>+ adaptor auth headers from config

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

### Webhook Payload

```mermaid
flowchart LR
    AD[ActionDefinition<br/>.definition JSONB<br/>(FHIR extension)] --> TR[TemplateRenderer]
    RC[ActionEvaluationContext<br/>variables] --> TR
    TE[TriggerEvent<br/>fields] --> TR
    TR --> P[Rendered Payload JSON]
    P --> H[HTTP POST Body]

    subgraph "HTTP POST to Receiver Adaptor"
        H
        Headers["Headers:<br/>Content-Type: application/json<br/>X-CCE-Delivery-Run-Id<br/>X-CCE-Trigger-Event-Id<br/>+ adaptor.config auth"]
    end
```

---

## 6. Delivery Run Lifecycle

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

## 7. Retry & Error Handling

### 7.1 Webhook Delivery Retry

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

### 7.2 Kafka Consumer Error Handling

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

    D -->|Individual action error| I[Log error, continue<br/>to remaining actions]
    I --> E

    C --> J[Alert ops:<br/>cce.intelligence.consumer.errors++]
    H --> J
```

---

## 8. REST API Flows

### 8.1 Create Target Subscription

```mermaid
sequenceDiagram
    participant Client
    participant Controller as TargetSubscriptionController
    participant Service as TargetSubscriptionService
    participant DB as PostgreSQL

    Client->>Controller: POST /v1/target-subscriptions
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

    Service->>DB: existsByProtocolTargetAdaptor(...)
    alt Already exists
        Service-->>Controller: ConflictException
        Controller-->>Client: 409 Conflict
    end

    Service->>DB: save(TargetSubscription)
    DB-->>Service: saved entity
    Service-->>Controller: TargetSubscriptionDto
    Controller-->>Client: 201 Created
```

### 8.2 Cancel Delivery Run

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

### 8.3 Delete Receiver Adaptor

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
        Controller-->>Client: 422 — Active target subscriptions reference this adaptor
    end

    Service->>DB: existsActiveDeliveryRuns(adaptorId)
    alt Active delivery runs exist
        Service-->>Controller: UnprocessableEntityException
        Controller-->>Client: 422 — Active delivery runs reference this adaptor
    end

    Service->>DB: delete(adaptor)
    Controller-->>Client: 204 No Content
```
