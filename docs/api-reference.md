# API Reference

> **CCE Intelligence Service** — REST API endpoint reference  
> **Base URL**: `http://localhost:8085` | **Prefix**: `/v1/`  
> **Authentication**: OAuth 2.0 via CCE Gateway (no direct token validation)

---

## Table of Contents

1. [Delivery Runs](#1-delivery-runs)
2. [Channel Subscriptions](#2-channel-subscriptions)
3. [Receiver Adaptors](#3-receiver-adaptors)
4. [Actuator Endpoints](#4-actuator-endpoints)
5. [Error Response Format](#5-error-response-format)

---

## 1. Delivery Runs

Delivery Runs track the **delivery lifecycle** of fired intelligence actions to Receiver Adaptors. Created automatically when the intelligence engine processes a trigger and fans out to subscribed adaptors. Exposed read-only with support for manual cancellation.

**Required scope**: `delivery-runs:read` (GET), `delivery-runs:write` (POST cancel)

---

### 1.1 List Delivery Runs

**`GET /v1/delivery-runs`** — Retrieve delivery runs with filtering and pagination.

**Query Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `status` | `String` | No | Filter by status: `PENDING`, `EXECUTING`, `DELIVERED`, `FAILED`, `CANCELLED` |
| `subject` | `String` | No | Filter by patient UPID |
| `intelligenceEventId` | `UUID` | No | Filter by intelligence event |
| `actionDefinitionId` | `UUID` | No | Filter by action definition |
| `actionType` | `String` | No | Filter by action type: `NOTIFICATION`, `ESCALATION`, `COORDINATION` |
| `severity` | `String` | No | Filter by severity: `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `channel` | `String` | No | Filter by channel name (e.g., `supervisor`) |
| `protocolDefinitionId` | `UUID` | No | Filter by protocol definition |
| `page` | `int` | No | Page number (0-based, default: `0`) |
| `size` | `int` | No | Page size (default: `20`) |

**Response:** `200 OK`

```json
{
  "data": [
    {
      "id": "b2c3d4e5-0001-4000-b000-000000000001",
      "intelligenceEventId": "990e8400-e29b-41d4-a716-446655440010",
      "actionDefinitionId": "a1b2c3d4-0001-4000-a000-000000000001",
      "actionType": "NOTIFICATION",
      "actionId": "anc-visit-2",
      "channel": "supervisor",
      "channelSubscriptionId": "d4e5f6a7-0001-4000-d000-000000000010",
      "receiverAdaptorId": "c3d4e5f6-0001-4000-c000-000000000001",
      "receiverAdaptorName": "Kigali South SMS Gateway",
      "status": "DELIVERED",
      "subject": "260225-0002-5501",
      "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
      "severity": "HIGH",
      "attemptCount": 1,
      "createdAt": "2026-03-25T10:05:00Z",
      "updatedAt": "2026-03-25T10:05:02Z",
      "deliveredAt": "2026-03-25T10:05:02Z"
    }
  ],
  "pagination": {
    "page": 0,
    "size": 20,
    "totalElements": 1,
    "totalPages": 1
  }
}
```

---

### 1.2 Get Delivery Run by ID

**`GET /v1/delivery-runs/{id}`** — Retrieve a single delivery run with full detail including rendered payload and delivery result.

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Delivery run ID |

**Response:** `200 OK`

```json
{
  "data": {
    "id": "b2c3d4e5-0001-4000-b000-000000000001",
    "intelligenceEventId": "990e8400-e29b-41d4-a716-446655440010",
    "actionDefinitionId": "a1b2c3d4-0001-4000-a000-000000000001",
    "actionType": "NOTIFICATION",
    "actionId": "anc-visit-2",
    "channel": "supervisor",
    "channelSubscriptionId": "d4e5f6a7-0001-4000-d000-000000000010",
    "receiverAdaptorId": "c3d4e5f6-0001-4000-c000-000000000001",
    "receiverAdaptorName": "Kigali South SMS Gateway",
    "status": "DELIVERED",
    "subject": "260225-0002-5501",
    "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
    "severity": "HIGH",
    "fhirPayload": {
      "resourceType": "CommunicationRequest",
      "status": "active",
      "priority": "urgent",
      "category": [{ "coding": [{ "system": "http://openphc.org/fhir/CodeSystem/cce-action-type", "code": "NOTIFICATION" }] }],
      "subject": { "identifier": { "system": "http://openphc.org/fhir/patient-upid", "value": "260225-0002-5501" } },
      "payload": [{ "contentString": "[HIGH] NOTIFICATION for patient 260225-0002-5501 — step anc-visit-2 overdue (PlanDefinition/anc-high-risk|2.1)" }]
    },
    "deliveryResult": {
      "httpStatus": 200,
      "responseBody": "{\"status\": \"accepted\"}"
    },
    "attemptCount": 1,
    "createdAt": "2026-03-25T10:05:00Z",
    "updatedAt": "2026-03-25T10:05:02Z",
    "deliveredAt": "2026-03-25T10:05:02Z"
  }
}
```

| Error Status | Condition |
|:-------------|:----------|
| `404` | Delivery run not found |

---

### 1.3 Get Delivery Run Audit Trail

**`GET /v1/delivery-runs/{id}/audit`** — Retrieve the audit log entries for a delivery run.

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Delivery run ID |

**Response:** `200 OK`

```json
{
  "data": [
    {
      "id": "e5f6a7b8-0001-4000-e000-000000000001",
      "deliveryRunId": "b2c3d4e5-0001-4000-b000-000000000001",
      "eventType": "CREATED",
      "actor": "system",
      "details": null,
      "timestamp": "2026-03-25T10:05:00Z"
    },
    {
      "id": "e5f6a7b8-0002-4000-e000-000000000002",
      "deliveryRunId": "b2c3d4e5-0001-4000-b000-000000000001",
      "eventType": "DISPATCHED",
      "actor": "system",
      "details": { "adaptorName": "Kigali South SMS Gateway", "endpointUrl": "https://sms.example.com/webhook" },
      "timestamp": "2026-03-25T10:05:01Z"
    },
    {
      "id": "e5f6a7b8-0003-4000-e000-000000000003",
      "deliveryRunId": "b2c3d4e5-0001-4000-b000-000000000001",
      "eventType": "DELIVERED",
      "actor": "system",
      "details": { "httpStatus": 200, "responseBody": "{\"status\": \"accepted\"}" },
      "timestamp": "2026-03-25T10:05:02Z"
    }
  ]
}
```

| Error Status | Condition |
|:-------------|:----------|
| `404` | Delivery run not found |

---

### 1.4 Cancel Delivery Run

**`POST /v1/delivery-runs/{id}/cancel`** — Cancel a pending or failed delivery run.

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Delivery run ID |

**Pre-conditions:**
- Only delivery runs with status `PENDING` or `FAILED` can be cancelled.
- `EXECUTING`, `DELIVERED`, and `CANCELLED` runs cannot be cancelled.

**Response:** `200 OK`

```json
{
  "data": {
    "id": "b2c3d4e5-0001-4000-b000-000000000001",
    "status": "CANCELLED",
    "updatedAt": "2026-03-25T11:00:00Z"
  }
}
```

| Error Status | Condition |
|:-------------|:----------|
| `404` | Delivery run not found |
| `422` | Status does not allow cancellation |

**Side Effects:**
- Creates a `delivery_audit_log` entry with `event_type = 'CANCELLED'`

---

## 2. Channel Subscriptions

Channel Subscriptions define the **many-to-many routing** between protocol definition channels and Receiver Adaptors. Each subscription maps a `(protocolDefinitionId, actionId, channel)` tuple to a specific Receiver Adaptor. The `actionId` is optional — when omitted (`null`), the subscription acts as a wildcard for all steps in the protocol. Step-specific subscriptions take precedence over wildcards during routing.

**Required scope**: `channel-subscriptions:read` (GET), `channel-subscriptions:write` (POST, PUT, DELETE)

---

### 2.1 Create Channel Subscription

**`POST /v1/channel-subscriptions`** — Create a new channel subscription.

**Request Body**

```json
{
  "protocolDefinitionId": "550e8400-e29b-41d4-a716-446655440000",
  "actionId": "anc-visit-2",
  "channel": "supervisor",
  "receiverAdaptorId": "c3d4e5f6-0001-4000-c000-000000000001"
}
```

> **Note:** `actionId` is optional. Omit to create a wildcard subscription that matches all steps in the protocol for the given channel.

**Response:** `201 Created`

```json
{
  "data": {
    "id": "d4e5f6a7-0001-4000-d000-000000000010",
    "protocolDefinitionId": "550e8400-e29b-41d4-a716-446655440000",
    "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
    "actionId": "anc-visit-2",
    "channel": "supervisor",
    "receiverAdaptorId": "c3d4e5f6-0001-4000-c000-000000000001",
    "receiverAdaptorName": "Kigali South SMS Gateway",
    "status": "ACTIVE",
    "createdAt": "2026-03-25T10:00:00Z",
    "updatedAt": "2026-03-25T10:00:00Z"
  }
}
```

| Error Status | Condition |
|:-------------|:----------|
| `400` | Missing required fields or invalid UUIDs |
| `404` | `protocolDefinitionId` or `receiverAdaptorId` does not exist |
| `409` | Subscription for `(protocolDefinitionId, actionId, channel, receiverAdaptorId)` already exists |

---

### 2.2 List Channel Subscriptions

**`GET /v1/channel-subscriptions`** — Retrieve all channel subscriptions with optional filters.

**Query Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `protocolDefinitionId` | `UUID` | No | Filter by protocol definition |
| `actionId` | `String` | No | Filter by action ID (step-level); use `__null__` for wildcard-only subscriptions |
| `channel` | `String` | No | Filter by channel name |
| `receiverAdaptorId` | `UUID` | No | Filter by receiver adaptor |
| `status` | `String` | No | Filter by status: `ACTIVE`, `INACTIVE` |

**Response:** `200 OK`

```json
{
  "data": [
    {
      "id": "d4e5f6a7-0001-4000-d000-000000000010",
      "protocolDefinitionId": "550e8400-e29b-41d4-a716-446655440000",
      "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
      "actionId": "anc-visit-2",
      "channel": "supervisor",
      "receiverAdaptorId": "c3d4e5f6-0001-4000-c000-000000000001",
      "receiverAdaptorName": "Kigali South SMS Gateway",
      "status": "ACTIVE",
      "createdAt": "2026-03-25T10:00:00Z",
      "updatedAt": "2026-03-25T10:00:00Z"
    },
    {
      "id": "d4e5f6a7-0002-4000-d000-000000000011",
      "protocolDefinitionId": "550e8400-e29b-41d4-a716-446655440000",
      "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
      "actionId": null,
      "channel": "supervisor",
      "receiverAdaptorId": "c3d4e5f6-0002-4000-c000-000000000002",
      "receiverAdaptorName": "CCE Dashboard Adaptor",
      "status": "ACTIVE",
      "createdAt": "2026-03-25T10:00:00Z",
      "updatedAt": "2026-03-25T10:00:00Z"
    }
  ]
}
```

---

### 2.3 Get Channel Subscription by ID

**`GET /v1/channel-subscriptions/{id}`**

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Channel subscription ID |

**Response:** `200 OK` — `ChannelSubscriptionDto`

| Error Status | Condition |
|:-------------|:----------|
| `404` | Subscription not found |

---

### 2.4 Update Channel Subscription

**`PUT /v1/channel-subscriptions/{id}`** — Update status of an existing subscription.

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Channel subscription ID |

**Request Body**

```json
{
  "status": "INACTIVE"
}
```

> **Note:** `protocolDefinitionId`, `actionId`, `channel`, and `receiverAdaptorId` are immutable. To change routing, delete the subscription and create a new one.

**Response:** `200 OK`

```json
{
  "data": {
    "id": "d4e5f6a7-0001-4000-d000-000000000010",
    "protocolDefinitionId": "550e8400-e29b-41d4-a716-446655440000",
    "protocolCanonical": "http://openphc.org/fhir/PlanDefinition/anc-high-risk|2.1",
    "actionId": "anc-visit-2",
    "channel": "supervisor",
    "receiverAdaptorId": "c3d4e5f6-0001-4000-c000-000000000001",
    "receiverAdaptorName": "Kigali South SMS Gateway",
    "status": "INACTIVE",
    "createdAt": "2026-03-25T10:00:00Z",
    "updatedAt": "2026-03-25T14:00:00Z"
  }
}
```

| Error Status | Condition |
|:-------------|:----------|
| `400` | Invalid request body |
| `404` | Subscription not found |

---

### 2.5 Delete Channel Subscription

**`DELETE /v1/channel-subscriptions/{id}`** — Remove a channel subscription.

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Channel subscription ID |

**Pre-conditions:**
- Only subscriptions with no `PENDING` or `EXECUTING` delivery runs can be deleted.
- Subscriptions with historical delivery runs (`DELIVERED`, `FAILED`, `CANCELLED`) can be deleted; the `delivery_run.channel_subscription_id` FK is preserved (soft reference).

**Response:** `204 No Content`

| Error Status | Condition |
|:-------------|:----------|
| `404` | Subscription not found |
| `422` | Subscription has active (PENDING/EXECUTING) delivery runs |

---

## 3. Receiver Adaptors

Receiver Adaptors represent external **webhook endpoints** that receive intelligence actions. Routing from channels to adaptors is managed via Channel Subscriptions.

**Required scope**: `admin` (all operations)

---

### 3.1 Register Receiver Adaptor

**`POST /v1/receiver-adaptors`** — Register a new webhook endpoint.

**Request Body**

```json
{
  "name": "Kigali South SMS Gateway",
  "definition": {
    "resourceType": "Endpoint",
    "id": "kigali-south-sms",
    "status": "active",
    "connectionType": {
      "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
      "code": "hl7-fhir-rest",
      "display": "HL7 FHIR REST"
    },
    "name": "Kigali South SMS Gateway",
    "payloadType": [
      {
        "coding": [
          { "system": "http://hl7.org/fhir/resource-types", "code": "CommunicationRequest" }
        ]
      }
    ],
    "payloadMimeType": ["application/fhir+json"],
    "address": "https://sms-gateway.example.com/webhook/intelligence"
  },
  "config": {
    "authHeader": "X-API-Key",
    "authValue": "sk-abc123",
    "timeoutMs": 10000
  }
}
```

**Response:** `201 Created`

```json
{
  "data": {
    "id": "c3d4e5f6-0001-4000-c000-000000000001",
    "name": "Kigali South SMS Gateway",
    "definition": {
      "resourceType": "Endpoint",
      "id": "kigali-south-sms",
      "status": "active",
      "connectionType": {
        "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
        "code": "hl7-fhir-rest",
        "display": "HL7 FHIR REST"
      },
      "name": "Kigali South SMS Gateway",
      "payloadType": [
        {
          "coding": [
            { "system": "http://hl7.org/fhir/resource-types", "code": "CommunicationRequest" }
          ]
        }
      ],
      "payloadMimeType": ["application/fhir+json"],
      "address": "https://sms-gateway.example.com/webhook/intelligence"
    },
    "status": "ACTIVE",
    "config": {
      "authHeader": "X-API-Key",
      "authValue": "sk-***123",
      "timeoutMs": 10000
    },
    "createdAt": "2026-03-20T08:00:00Z",
    "updatedAt": "2026-03-20T08:00:00Z"
  }
}
```

> **Note:** `authValue` is masked in all API responses. Full credentials are accepted on write operations (`POST`, `PUT`) but never returned.

| Error Status | Condition |
|:-------------|:----------|
| `400` | Invalid request body (missing required fields, invalid URL, `definition.resourceType` not `Endpoint`, `definition.name` mismatch) |
| `409` | Adaptor `name` already exists |

---

### 3.2 List Receiver Adaptors

**`GET /v1/receiver-adaptors`** — Retrieve all receiver adaptors.

**Query Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `status` | `String` | No | Filter by status: `ACTIVE`, `INACTIVE` |

**Response:** `200 OK`

```json
{
  "data": [
    {
      "id": "c3d4e5f6-0001-4000-c000-000000000001",
      "name": "Kigali South SMS Gateway",
      "definition": {
        "resourceType": "Endpoint",
        "id": "kigali-south-sms",
        "status": "active",
        "connectionType": {
          "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
          "code": "hl7-fhir-rest",
          "display": "HL7 FHIR REST"
        },
        "name": "Kigali South SMS Gateway",
        "payloadType": [
          {
            "coding": [
              { "system": "http://hl7.org/fhir/resource-types", "code": "CommunicationRequest" }
            ]
          }
        ],
        "payloadMimeType": ["application/fhir+json"],
        "address": "https://sms-gateway.example.com/webhook/intelligence"
      },
      "status": "ACTIVE",
      "config": { "authHeader": "X-API-Key", "authValue": "sk-***123" },
      "createdAt": "2026-03-20T08:00:00Z",
      "updatedAt": "2026-03-20T08:00:00Z"
    }
  ]
}
```

---

### 3.3 Get Receiver Adaptor by ID

**`GET /v1/receiver-adaptors/{id}`**

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Receiver adaptor ID |

**Response:** `200 OK` — `ReceiverAdaptorDto`

| Error Status | Condition |
|:-------------|:----------|
| `404` | Receiver adaptor not found |

---

### 3.4 Update Receiver Adaptor

**`PUT /v1/receiver-adaptors/{id}`** — Update an existing adaptor.

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Receiver adaptor ID |

**Request Body**

```json
{
  "name": "Kigali South SMS Gateway",
  "definition": {
    "resourceType": "Endpoint",
    "id": "kigali-south-sms",
    "status": "active",
    "connectionType": {
      "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
      "code": "hl7-fhir-rest",
      "display": "HL7 FHIR REST"
    },
    "name": "Kigali South SMS Gateway",
    "payloadType": [
      {
        "coding": [
          { "system": "http://hl7.org/fhir/resource-types", "code": "CommunicationRequest" }
        ]
      }
    ],
    "payloadMimeType": ["application/fhir+json"],
    "address": "https://sms-gateway-v2.example.com/webhook/intelligence"
  },
  "status": "ACTIVE",
  "config": {
    "authHeader": "Authorization",
    "authValue": "Bearer tok-xyz789",
    "timeoutMs": 15000
  }
}
```

> **Note:** `authValue` is accepted in full on `PUT` requests but will be masked in the response.

**Response:** `200 OK` — Updated `ReceiverAdaptorDto`

| Error Status | Condition |
|:-------------|:----------|
| `400` | Invalid request body |
| `404` | Receiver adaptor not found |

---

### 3.5 Delete Receiver Adaptor

**`DELETE /v1/receiver-adaptors/{id}`** — Remove a receiver adaptor.

| Path Parameter | Type | Description |
|:---------------|:-----|:------------|
| `id` | `UUID` | Receiver adaptor ID |

**Pre-conditions:**
- Only adaptors with no `PENDING` or `EXECUTING` delivery runs (via channel subscriptions) can be deleted.
- All channel subscriptions referencing this adaptor must be deleted or inactive first.

**Response:** `204 No Content`

| Error Status | Condition |
|:-------------|:----------|
| `404` | Receiver adaptor not found |
| `422` | Adaptor has active delivery runs or active channel subscriptions |

---

## 4. Actuator Endpoints

Standard Spring Boot Actuator endpoints for monitoring and operations.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/actuator/health` | Application health with component details |
| `GET` | `/actuator/health/readiness` | Kubernetes readiness probe |
| `GET` | `/actuator/health/liveness` | Kubernetes liveness probe |
| `GET` | `/actuator/info` | Build and version metadata |
| `GET` | `/actuator/prometheus` | Prometheus metrics scrape endpoint |

### Health Check Detail

**`GET /actuator/health`**

```json
{
  "status": "UP",
  "components": {
    "db": { "status": "UP", "details": { "database": "PostgreSQL", "validationQuery": "isValid()" } },
    "kafka": { "status": "UP" },
    "diskSpace": { "status": "UP" }
  }
}
```

---

## 5. Error Response Format

All error responses follow a consistent envelope:

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Delivery run not found: b2c3d4e5-0001-4000-b000-000000000099",
    "path": "/v1/delivery-runs/b2c3d4e5-0001-4000-b000-000000000099",
    "timestamp": "2026-03-25T10:05:00Z"
  }
}
```

### HTTP Status Codes

| Status | Code | Description |
|--------|------|-------------|
| `400` | `BAD_REQUEST` | Invalid request body, missing parameters |
| `404` | `NOT_FOUND` | Resource does not exist |
| `409` | `CONFLICT` | Uniqueness constraint violation |
| `422` | `UNPROCESSABLE_ENTITY` | Business rule violation (invalid state transition, active dependencies) |
| `500` | `INTERNAL_ERROR` | Unexpected server error |
