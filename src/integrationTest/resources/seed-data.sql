-- Seed data for integration tests

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

-- Destination Adaptor Mappings (1:1 destination to adaptor)
INSERT INTO destination_adaptor_mapping (id, destination, receiver_adaptor_id, status)
VALUES (
    '33333333-3333-3333-3333-333333333331',
    'sms',
    '22222222-2222-2222-2222-222222222221',
    'ACTIVE'
);

INSERT INTO destination_adaptor_mapping (id, destination, receiver_adaptor_id, status)
VALUES (
    '33333333-3333-3333-3333-333333333332',
    'email',
    '22222222-2222-2222-2222-222222222222',
    'ACTIVE'
);

INSERT INTO destination_adaptor_mapping (id, destination, receiver_adaptor_id, status)
VALUES (
    '33333333-3333-3333-3333-333333333333',
    'push',
    '22222222-2222-2222-2222-222222222223',
    'ACTIVE'
);
