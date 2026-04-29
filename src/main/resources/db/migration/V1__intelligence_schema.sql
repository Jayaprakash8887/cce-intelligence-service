-- ============================================================================
-- V1: Intelligence Service Schema
-- Database: cce_collector (shared with Compliance Service)
-- Tables: receiver_adaptor, channel_subscription, intelligence_delivery,
--         intelligence_delivery_audit_log
-- ============================================================================

-- Note: protocol_definition table is owned/created by the Compliance Service.
-- The FK from channel_subscription references it for referential integrity.
-- If running standalone (without Compliance migration), create a minimal stub:
CREATE TABLE IF NOT EXISTS protocol_definition (
    id UUID PRIMARY KEY
);

-- ============================================================================
-- 1. receiver_adaptor
-- ============================================================================
CREATE TABLE receiver_adaptor (
    id              UUID        NOT NULL DEFAULT gen_random_uuid(),
    name            VARCHAR     NOT NULL,
    definition      JSONB       NOT NULL,
    status          VARCHAR     NOT NULL DEFAULT 'ACTIVE',
    config          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT receiver_adaptor_pkey PRIMARY KEY (id),
    CONSTRAINT receiver_adaptor_name_key UNIQUE (name),
    CONSTRAINT receiver_adaptor_definition_type_check
        CHECK (definition->>'resourceType' = 'Endpoint'),
    CONSTRAINT receiver_adaptor_definition_address_check
        CHECK (definition->>'address' IS NOT NULL),
    CONSTRAINT receiver_adaptor_status_check
        CHECK (status IN ('ACTIVE', 'INACTIVE'))
);

-- ============================================================================
-- 2. channel_subscription
-- ============================================================================
CREATE TABLE channel_subscription (
    id                      UUID        NOT NULL DEFAULT gen_random_uuid(),
    protocol_definition_id  UUID        NOT NULL,
    action_id               VARCHAR,
    channel                 VARCHAR     NOT NULL,
    receiver_adaptor_id     UUID        NOT NULL,
    status                  VARCHAR     NOT NULL DEFAULT 'ACTIVE',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT channel_subscription_pkey PRIMARY KEY (id),
    CONSTRAINT channel_subscription_protocol_definition_id_fkey
        FOREIGN KEY (protocol_definition_id) REFERENCES protocol_definition(id),
    CONSTRAINT channel_subscription_receiver_adaptor_id_fkey
        FOREIGN KEY (receiver_adaptor_id) REFERENCES receiver_adaptor(id),
    CONSTRAINT channel_subscription_status_check
        CHECK (status IN ('ACTIVE', 'INACTIVE'))
);

-- Partial unique: step-specific subscriptions
CREATE UNIQUE INDEX channel_subscription_step_key
    ON channel_subscription (protocol_definition_id, action_id, channel, receiver_adaptor_id)
    WHERE action_id IS NOT NULL;

-- Partial unique: wildcard subscriptions
CREATE UNIQUE INDEX channel_subscription_wildcard_key
    ON channel_subscription (protocol_definition_id, channel, receiver_adaptor_id)
    WHERE action_id IS NULL;

-- Routing lookup index
CREATE INDEX idx_channel_subscription_routing
    ON channel_subscription (protocol_definition_id, action_id, channel);

-- Active subscriptions partial index
CREATE INDEX idx_channel_subscription_active
    ON channel_subscription (status)
    WHERE status = 'ACTIVE';

-- Adaptor lookup
CREATE INDEX idx_channel_subscription_adaptor
    ON channel_subscription (receiver_adaptor_id);

-- ============================================================================
-- 3. intelligence_delivery
-- ============================================================================
CREATE TABLE intelligence_delivery (
    id                      UUID        NOT NULL DEFAULT gen_random_uuid(),
    intelligence_event_id   UUID        NOT NULL,
    action_definition_id    UUID        NOT NULL,
    channel_subscription_id UUID,
    action_type             VARCHAR     NOT NULL,
    status                  VARCHAR     NOT NULL,
    subject                 VARCHAR     NOT NULL,
    protocol_canonical      VARCHAR     NOT NULL,
    action_id               VARCHAR     NOT NULL,
    severity                VARCHAR     NOT NULL,
    channel                 VARCHAR     NOT NULL,
    fhir_payload            JSONB       NOT NULL,
    delivery_result         JSONB,
    attempt_count           INTEGER     NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at            TIMESTAMPTZ,

    CONSTRAINT intelligence_delivery_pkey PRIMARY KEY (id),
    CONSTRAINT intelligence_delivery_channel_subscription_id_fkey
        FOREIGN KEY (channel_subscription_id) REFERENCES channel_subscription(id),
    CONSTRAINT intelligence_delivery_intel_event_subscription_key
        UNIQUE (intelligence_event_id, channel_subscription_id),
    CONSTRAINT intelligence_delivery_action_type_check
        CHECK (action_type IN ('NOTIFICATION', 'ESCALATION', 'COORDINATION')),
    CONSTRAINT intelligence_delivery_status_check
        CHECK (status IN ('PENDING', 'EXECUTING', 'DELIVERED', 'FAILED', 'CANCELLED')),
    CONSTRAINT intelligence_delivery_severity_check
        CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'))
);

CREATE INDEX idx_intelligence_delivery_intelligence_event
    ON intelligence_delivery (intelligence_event_id);

CREATE INDEX idx_intelligence_delivery_status
    ON intelligence_delivery (status);

CREATE INDEX idx_intelligence_delivery_subject
    ON intelligence_delivery (subject);

CREATE INDEX idx_intelligence_delivery_failed
    ON intelligence_delivery (status)
    WHERE status = 'FAILED';

-- ============================================================================
-- 4. intelligence_delivery_audit_log
-- ============================================================================
CREATE TABLE intelligence_delivery_audit_log (
    id                      UUID        NOT NULL DEFAULT gen_random_uuid(),
    intelligence_delivery_id UUID       NOT NULL,
    event_type              VARCHAR     NOT NULL,
    actor                   VARCHAR     NOT NULL DEFAULT 'system',
    details                 JSONB,
    timestamp               TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT intelligence_delivery_audit_log_pkey PRIMARY KEY (id),
    CONSTRAINT intelligence_delivery_audit_log_intelligence_delivery_id_fkey
        FOREIGN KEY (intelligence_delivery_id) REFERENCES intelligence_delivery(id)
);

CREATE INDEX idx_intelligence_delivery_audit_log_run
    ON intelligence_delivery_audit_log (intelligence_delivery_id);

CREATE INDEX idx_intelligence_delivery_audit_log_timestamp
    ON intelligence_delivery_audit_log (timestamp);
