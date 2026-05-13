# Insights Pre-Computation Optimization

> **CCE Intelligence Service** — Pre-computed delivery metrics for the Insights Service  
> **Status**: Proposed | **Target**: v1.2.0  
> **Last Updated**: 2026-05-11

---

## 1. Problem Statement

The Intelligence Service tracks webhook delivery lifecycle in the `intelligence_delivery` table (one row per trigger event × destination adaptor). At production scale, this table grows at the same rate as intelligence triggers — potentially thousands of rows per day. The `intelligence_delivery_audit_log` grows even faster (3–5 audit rows per delivery).

The Insights Service currently has **no intelligence analytics endpoints**. However, a complete operational dashboard requires:

- **Delivery success/failure rates** — by destination, action type, severity, time period
- **Webhook latency distribution** — which adaptors are slow or degrading?
- **Delivery volume trends** — are intelligence actions firing at expected rates?
- **Adaptor health** — which receiver adaptors have elevated failure rates?
- **End-to-end intelligence pipeline** — from deviation detection (Compliance) to delivery (Intelligence)

Without pre-computation, the Insights Service would need to run `GROUP BY` aggregations on the full `intelligence_delivery` table with JOINs to `destination_adaptor_mapping` and `receiver_adaptor` — reproducing the same scaling problems already identified in Collector and Compliance service tables.

### 1.1 Anticipated Insights Queries

| Query Category | Tables Involved | Expected Complexity |
|----------------|-----------------|---------------------|
| Delivery funnel (PENDING → DELIVERED/FAILED) | `intelligence_delivery` | `GROUP BY status` on full table |
| Delivery rate by destination | `intelligence_delivery` JOIN `destination_adaptor_mapping` | 2-table JOIN + GROUP BY |
| Delivery rate by severity/action type | `intelligence_delivery` | `GROUP BY severity, action_type` |
| Delivery trends over time | `intelligence_delivery` | `DATE_TRUNC + GROUP BY` on growing table |
| Adaptor health (failure rate) | `intelligence_delivery` JOIN `destination_adaptor_mapping` JOIN `receiver_adaptor` | 3-table JOIN + conditional aggregates |
| Average webhook latency | `intelligence_delivery` | `AVG(delivered_at - created_at)` |
| Failed delivery details | `intelligence_delivery` + `intelligence_delivery_audit_log` | JOIN + filter |
| Retry distribution | `intelligence_delivery` | `GROUP BY attempt_count` |

---

## 2. Optimizations Owned by Intelligence Service

### 2.1 New Table: `delivery_summary_daily`

**Problem:** Every dashboard query for delivery metrics would scan the full `intelligence_delivery` table. Delivery status distribution, success rates, and volume by destination/severity are the most common analytics queries, but they only change when new deliveries complete.

**Solution:** Maintain a pre-aggregated daily summary table, updated incrementally on each delivery status change (DELIVERED or FAILED).

**Schema:**

```sql
CREATE TABLE delivery_summary_daily (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    summary_date        DATE NOT NULL,
    destination         VARCHAR(200) NOT NULL,
    receiver_adaptor_id UUID,                    -- denormalized from mapping
    action_type         VARCHAR(30) NOT NULL,     -- NOTIFICATION, ESCALATION, COORDINATION
    severity            VARCHAR(20) NOT NULL,     -- LOW, MEDIUM, HIGH, CRITICAL
    total_count         BIGINT NOT NULL DEFAULT 0,
    delivered_count     BIGINT NOT NULL DEFAULT 0,
    failed_count        BIGINT NOT NULL DEFAULT 0,
    cancelled_count     BIGINT NOT NULL DEFAULT 0,
    total_attempts      BIGINT NOT NULL DEFAULT 0, -- sum of attempt_count across deliveries
    total_latency_ms    BIGINT NOT NULL DEFAULT 0, -- sum of (delivered_at - created_at) in ms
    min_latency_ms      BIGINT,
    max_latency_ms      BIGINT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (summary_date, destination, action_type, severity)
);

CREATE INDEX idx_delivery_daily_date ON delivery_summary_daily (summary_date);
CREATE INDEX idx_delivery_daily_dest ON delivery_summary_daily (destination);
CREATE INDEX idx_delivery_daily_adaptor ON delivery_summary_daily (receiver_adaptor_id);
```

**Update trigger:** In `ActionDispatcher`, after Phase 3 (delivery result update), upsert the summary row:

```sql
-- On delivery creation (PENDING)
INSERT INTO delivery_summary_daily (summary_date, destination, receiver_adaptor_id, action_type, severity, total_count)
VALUES (:date, :destination, :adaptorId, :actionType, :severity, 1)
ON CONFLICT (summary_date, destination, action_type, severity)
DO UPDATE SET
    total_count = delivery_summary_daily.total_count + 1,
    updated_at = now();

-- On delivery success (DELIVERED)
UPDATE delivery_summary_daily SET
    delivered_count = delivered_count + 1,
    total_attempts = total_attempts + :attemptCount,
    total_latency_ms = total_latency_ms + :latencyMs,
    min_latency_ms = LEAST(min_latency_ms, :latencyMs),
    max_latency_ms = GREATEST(max_latency_ms, :latencyMs),
    updated_at = now()
WHERE summary_date = :date AND destination = :destination
  AND action_type = :actionType AND severity = :severity;

-- On delivery failure (FAILED)
UPDATE delivery_summary_daily SET
    failed_count = failed_count + 1,
    total_attempts = total_attempts + :attemptCount,
    updated_at = now()
WHERE summary_date = :date AND destination = :destination
  AND action_type = :actionType AND severity = :severity;
```

**Insights Service query enablement:**

| Analytics Query | SQL on Pre-Computed Table |
|----------------|--------------------------|
| Delivery funnel | `SELECT SUM(total_count), SUM(delivered_count), SUM(failed_count), SUM(cancelled_count) FROM delivery_summary_daily WHERE summary_date BETWEEN ? AND ?` |
| Success rate by destination | `SELECT destination, SUM(delivered_count)::float / NULLIF(SUM(total_count), 0) * 100 FROM delivery_summary_daily GROUP BY destination` |
| Success rate by severity | `SELECT severity, SUM(delivered_count)::float / NULLIF(SUM(total_count), 0) * 100 FROM delivery_summary_daily GROUP BY severity` |
| Volume trends | `SELECT summary_date, SUM(total_count) FROM delivery_summary_daily GROUP BY summary_date ORDER BY summary_date` |
| Avg webhook latency by destination | `SELECT destination, SUM(total_latency_ms)::float / NULLIF(SUM(delivered_count), 0) FROM delivery_summary_daily GROUP BY destination` |
| Retry pressure (avg attempts) | `SELECT destination, SUM(total_attempts)::float / NULLIF(SUM(total_count), 0) FROM delivery_summary_daily GROUP BY destination` |
| Volume by action type | `SELECT action_type, SUM(total_count) FROM delivery_summary_daily GROUP BY action_type` |

---

### 2.2 New Table: `adaptor_health_snapshot`

**Problem:** Determining adaptor health requires joining `intelligence_delivery` with `destination_adaptor_mapping` and `receiver_adaptor`, then computing failure rates over a rolling window. This is a 3-table JOIN with conditional aggregates on a growing table.

**Solution:** Maintain a per-adaptor health snapshot, updated on every delivery outcome.

**Schema:**

```sql
CREATE TABLE adaptor_health_snapshot (
    receiver_adaptor_id UUID PRIMARY KEY,
    adaptor_name        VARCHAR(200) NOT NULL,
    active_destinations INTEGER NOT NULL DEFAULT 0,
    total_deliveries    BIGINT NOT NULL DEFAULT 0,
    successful_deliveries BIGINT NOT NULL DEFAULT 0,
    failed_deliveries   BIGINT NOT NULL DEFAULT 0,
    success_rate        NUMERIC(5,2),             -- (successful / total) * 100
    avg_latency_ms      BIGINT,                   -- average webhook response time
    last_delivery_at    TIMESTAMPTZ,
    last_failure_at     TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0, -- reset on success
    health_status       VARCHAR(20) NOT NULL DEFAULT 'HEALTHY',  -- HEALTHY, DEGRADED, UNHEALTHY
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Health status rules:**

| Condition | Status |
|-----------|--------|
| `success_rate >= 95%` AND `consecutive_failures < 3` | `HEALTHY` |
| `success_rate >= 80%` OR `consecutive_failures BETWEEN 3 AND 9` | `DEGRADED` |
| `success_rate < 80%` OR `consecutive_failures >= 10` | `UNHEALTHY` |

**Update trigger:** In `ActionDispatcher`, after Phase 3:

- **On DELIVERED:**
  - `successful_deliveries += 1`, `total_deliveries += 1`
  - `consecutive_failures = 0`
  - Recalculate `success_rate` and `avg_latency_ms`
  - Update `last_delivery_at`, re-evaluate `health_status`

- **On FAILED:**
  - `failed_deliveries += 1`, `total_deliveries += 1`
  - `consecutive_failures += 1`
  - Recalculate `success_rate`
  - Update `last_failure_at`, re-evaluate `health_status`

**Insights Service query enablement:**

| Analytics Query | SQL on Pre-Computed Table |
|----------------|--------------------------|
| Adaptor health dashboard | `SELECT * FROM adaptor_health_snapshot ORDER BY success_rate ASC` |
| Unhealthy adaptors alert | `SELECT * FROM adaptor_health_snapshot WHERE health_status = 'UNHEALTHY'` |
| Adaptor ranking by latency | `SELECT adaptor_name, avg_latency_ms FROM adaptor_health_snapshot ORDER BY avg_latency_ms DESC` |
| Adaptors with recent failures | `SELECT * FROM adaptor_health_snapshot WHERE last_failure_at > now() - interval '1 hour'` |

---

### 2.3 Denormalize `facility_id` on `intelligence_delivery`

**Problem:** Intelligence trigger events carry `facilityid` in the CloudEvents extension, but the `intelligence_delivery` table does not store it. The Insights Service would need to correlate deliveries with facilities by joining back to Compliance Service tables (`event_log` or `protocol_instance`), which crosses service boundaries.

**Solution:** Extract `facility_id` from the trigger event's CloudEvents envelope and persist it on `intelligence_delivery`.

**Schema change:**

```sql
ALTER TABLE intelligence_delivery ADD COLUMN facility_id VARCHAR(100);
CREATE INDEX idx_intelligence_delivery_facility ON intelligence_delivery (facility_id);
```

**Code change:** In `IntelligenceEngine.processTrigger()`, extract the facility ID from the Kafka message headers (`ce_facilityid`) and set it on the delivery record.

**Impact:** Enables facility-scoped intelligence analytics without cross-service table reads:

```sql
-- Deliveries by facility
SELECT facility_id, COUNT(*), SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END)
FROM intelligence_delivery
WHERE facility_id IS NOT NULL
GROUP BY facility_id;
```

This also supports adding a `facility_id` column to `delivery_summary_daily` for facility-level pre-aggregation.

---

## 3. Consistency Guarantees

- **`delivery_summary_daily`** — Updated synchronously in the same transaction as the delivery status update (Phase 3 of `ActionDispatcher`). No consistency lag between delivery table and summary.
- **`adaptor_health_snapshot`** — Updated synchronously on each delivery outcome. Health status is always current with the latest delivery result.
- **`facility_id` denormalization** — Set at delivery creation time from the trigger event headers. Immutable once set.

### 3.1 Backfill Strategy

```sql
-- delivery_summary_daily backfill
INSERT INTO delivery_summary_daily (
    summary_date, destination, receiver_adaptor_id, action_type, severity,
    total_count, delivered_count, failed_count, cancelled_count,
    total_attempts, total_latency_ms, min_latency_ms, max_latency_ms
)
SELECT
    DATE(id.created_at),
    id.destination,
    dam.receiver_adaptor_id,
    id.action_type,
    id.severity,
    COUNT(*),
    COUNT(*) FILTER (WHERE id.status = 'DELIVERED'),
    COUNT(*) FILTER (WHERE id.status = 'FAILED'),
    COUNT(*) FILTER (WHERE id.status = 'CANCELLED'),
    SUM(id.attempt_count),
    SUM(EXTRACT(EPOCH FROM (id.delivered_at - id.created_at)) * 1000) FILTER (WHERE id.status = 'DELIVERED'),
    MIN(EXTRACT(EPOCH FROM (id.delivered_at - id.created_at)) * 1000) FILTER (WHERE id.status = 'DELIVERED'),
    MAX(EXTRACT(EPOCH FROM (id.delivered_at - id.created_at)) * 1000) FILTER (WHERE id.status = 'DELIVERED')
FROM intelligence_delivery id
JOIN destination_adaptor_mapping dam ON dam.id = id.destination_adaptor_mapping_id
GROUP BY DATE(id.created_at), id.destination, dam.receiver_adaptor_id, id.action_type, id.severity;

-- adaptor_health_snapshot backfill
INSERT INTO adaptor_health_snapshot (
    receiver_adaptor_id, adaptor_name, active_destinations,
    total_deliveries, successful_deliveries, failed_deliveries,
    success_rate, last_delivery_at, last_failure_at
)
SELECT
    ra.id,
    ra.name,
    (SELECT COUNT(*) FROM destination_adaptor_mapping dam WHERE dam.receiver_adaptor_id = ra.id AND dam.status = 'ACTIVE'),
    COUNT(id.id),
    COUNT(id.id) FILTER (WHERE id.status = 'DELIVERED'),
    COUNT(id.id) FILTER (WHERE id.status = 'FAILED'),
    CASE WHEN COUNT(id.id) > 0
         THEN ROUND(COUNT(id.id) FILTER (WHERE id.status = 'DELIVERED') * 100.0 / COUNT(id.id), 2)
         ELSE 100 END,
    MAX(id.delivered_at),
    MAX(id.updated_at) FILTER (WHERE id.status = 'FAILED')
FROM receiver_adaptor ra
LEFT JOIN destination_adaptor_mapping dam ON dam.receiver_adaptor_id = ra.id
LEFT JOIN intelligence_delivery id ON id.destination_adaptor_mapping_id = dam.id
GROUP BY ra.id, ra.name;
```

---

## 4. Summary of Changes

| Change | Type | Table | Updated By | Trigger |
|--------|------|-------|-----------|---------|
| New `delivery_summary_daily` table | Table | New | Intelligence | Delivery create/complete/fail |
| New `adaptor_health_snapshot` table | Table | New | Intelligence | Delivery complete/fail |
| Add `facility_id` to `intelligence_delivery` | Column | Existing | Intelligence | Delivery creation |

### 4.1 Flyway Migration Plan

| Order | Migration | Description |
|-------|-----------|-------------|
| V2 | `V2__create_delivery_summary_daily.sql` | Create table + indexes + backfill |
| V3 | `V3__create_adaptor_health_snapshot.sql` | Create table + backfill |
| V4 | `V4__add_facility_to_intelligence_delivery.sql` | Add column + index |

### 4.2 Metrics Additions

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `cce.intelligence.adaptor.health` | Gauge | `adaptor_name`, `status` | 1 = healthy, 0.5 = degraded, 0 = unhealthy |
| `cce.intelligence.adaptor.consecutive_failures` | Gauge | `adaptor_name` | Current consecutive failure count |

---

## 5. Cross-Service Dependencies

### 5.1 Dead Column: intelligence_event_log.error_message (Compliance Service §2.6)

The `intelligence_event_log` table (owned by Compliance Service, written by `IntelligenceActionEvaluator`) has a declared `error_message` TEXT column that is **never populated** — `setErrorMessage()` has zero call sites across the entire codebase.

**Action:** This column will be dropped as part of **Compliance Service §2.6** (Flyway `V15__drop_dead_columns.sql`). The Intelligence Service does not read or write this column, so no code changes are required on this side.

If error tracking for intelligence action evaluation is needed in the future, it should be implemented as a separate concern (e.g., structured error events on a Kafka DLQ or an `intelligence_error_log` table) rather than an unused nullable column.
