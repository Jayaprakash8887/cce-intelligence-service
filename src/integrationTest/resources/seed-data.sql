-- Seed data for integration tests

-- Protocol definition (referenced by channel subscriptions)
INSERT INTO protocol_definition (id) VALUES ('11111111-1111-1111-1111-111111111111');

-- Receiver Adaptors
INSERT INTO receiver_adaptor (id, name, definition, status, config)
VALUES (
    '22222222-2222-2222-2222-222222222221',
    'Hospital EHR Adaptor',
    '{"resourceType":"Endpoint","status":"active","address":"http://localhost:{{MOCK_PORT}}/webhook/ehr","connectionType":{"code":"hl7-fhir-rest"}}',
    'ACTIVE',
    '{"authHeader":{"name":"Authorization","value":"Bearer test-token"}}'
);

INSERT INTO receiver_adaptor (id, name, definition, status, config)
VALUES (
    '22222222-2222-2222-2222-222222222222',
    'Lab System Adaptor',
    '{"resourceType":"Endpoint","status":"active","address":"http://localhost:{{MOCK_PORT}}/webhook/lab","connectionType":{"code":"hl7-fhir-rest"}}',
    'ACTIVE',
    '{"customHeaders":{"X-Lab-System":"true"}}'
);

INSERT INTO receiver_adaptor (id, name, definition, status, config)
VALUES (
    '22222222-2222-2222-2222-222222222223',
    'Notification Hub Adaptor',
    '{"resourceType":"Endpoint","status":"active","address":"http://localhost:{{MOCK_PORT}}/webhook/notify","connectionType":{"code":"hl7-fhir-rest"}}',
    'ACTIVE',
    null
);

-- Channel Subscriptions
-- Step-specific subscription for the EHR adaptor
INSERT INTO channel_subscription (id, protocol_definition_id, action_id, channel, receiver_adaptor_id, status)
VALUES (
    '33333333-3333-3333-3333-333333333331',
    '11111111-1111-1111-1111-111111111111',
    'action-overdue-check',
    'sms',
    '22222222-2222-2222-2222-222222222221',
    'ACTIVE'
);

-- Wildcard subscription for the Lab adaptor (same channel)
INSERT INTO channel_subscription (id, protocol_definition_id, action_id, channel, receiver_adaptor_id, status)
VALUES (
    '33333333-3333-3333-3333-333333333332',
    '11111111-1111-1111-1111-111111111111',
    null,
    'sms',
    '22222222-2222-2222-2222-222222222222',
    'ACTIVE'
);

-- Another wildcard subscription for fan-out testing (Notification Hub)
INSERT INTO channel_subscription (id, protocol_definition_id, action_id, channel, receiver_adaptor_id, status)
VALUES (
    '33333333-3333-3333-3333-333333333333',
    '11111111-1111-1111-1111-111111111111',
    null,
    'sms',
    '22222222-2222-2222-2222-222222222223',
    'ACTIVE'
);

-- Wildcard subscription for EHR adaptor (should be overridden by step-specific above)
INSERT INTO channel_subscription (id, protocol_definition_id, action_id, channel, receiver_adaptor_id, status)
VALUES (
    '33333333-3333-3333-3333-333333333334',
    '11111111-1111-1111-1111-111111111111',
    null,
    'sms',
    '22222222-2222-2222-2222-222222222221',
    'ACTIVE'
);
