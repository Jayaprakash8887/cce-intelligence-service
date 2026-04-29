package org.openphc.cce.intelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;
import org.openphc.cce.intelligence.domain.entity.ChannelSubscription;
import org.openphc.cce.intelligence.domain.entity.IntelligenceDelivery;
import org.openphc.cce.intelligence.domain.entity.ReceiverAdaptor;
import org.openphc.cce.intelligence.domain.enums.ActionType;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.openphc.cce.intelligence.domain.enums.IntelligenceSeverity;
import org.openphc.cce.intelligence.domain.repository.ChannelSubscriptionRepository;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryAuditLogRepository;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryRepository;
import org.openphc.cce.intelligence.domain.repository.ReceiverAdaptorRepository;
import org.openphc.cce.intelligence.kafka.model.IntelligenceTriggerEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {"intelligence-triggers-test"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:0"}
)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntelligenceEndToEndIntegrationTest {

    @TestConfiguration
    static class TestConfig {
        @Bean
        PlatformTransactionManager transactionManager() {
            return new PlatformTransactionManager() {
                @Override
                public TransactionStatus getTransaction(TransactionDefinition definition) {
                    return new SimpleTransactionStatus();
                }
                @Override
                public void commit(TransactionStatus status) {}
                @Override
                public void rollback(TransactionStatus status) {}
            };
        }

        @Bean
        TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
            return new TransactionTemplate(transactionManager);
        }
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private IntelligenceDeliveryRepository deliveryRepository;

    @MockBean
    private IntelligenceDeliveryAuditLogRepository auditLogRepository;

    @MockBean
    private ChannelSubscriptionRepository subscriptionRepository;

    @MockBean
    private ReceiverAdaptorRepository adaptorRepository;

    private static MockWebServer mockWebServer;
    private final List<IntelligenceDelivery> savedDeliveries = new CopyOnWriteArrayList<>();

    private static final UUID PROTOCOL_DEF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID ADAPTOR_ID_1 = UUID.fromString("22222222-2222-2222-2222-222222222221");
    private static final UUID ADAPTOR_ID_2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID ADAPTOR_ID_3 = UUID.fromString("22222222-2222-2222-2222-222222222223");

    @BeforeAll
    static void startMockServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
    }

    @AfterAll
    static void stopMockServer() throws IOException {
        mockWebServer.shutdown();
    }

    @BeforeEach
    void setup() {
        savedDeliveries.clear();
        String mockUrl = mockWebServer.url("/webhook").toString();

        // Setup adaptors
        ReceiverAdaptor adaptor1 = buildAdaptor(ADAPTOR_ID_1, "Hospital EHR Adaptor", mockUrl);
        ReceiverAdaptor adaptor2 = buildAdaptor(ADAPTOR_ID_2, "Lab System Adaptor", mockUrl);
        ReceiverAdaptor adaptor3 = buildAdaptor(ADAPTOR_ID_3, "Notification Hub Adaptor", mockUrl);

        // Step-specific subscription (overrides wildcard for adaptor1)
        ChannelSubscription stepSub = buildSubscription(
                UUID.randomUUID(), PROTOCOL_DEF_ID, "action-overdue-check", "sms", ADAPTOR_ID_1, adaptor1);
        // Wildcard subscriptions
        ChannelSubscription wildcardSub2 = buildSubscription(
                UUID.randomUUID(), PROTOCOL_DEF_ID, null, "sms", ADAPTOR_ID_2, adaptor2);
        ChannelSubscription wildcardSub3 = buildSubscription(
                UUID.randomUUID(), PROTOCOL_DEF_ID, null, "sms", ADAPTOR_ID_3, adaptor3);
        ChannelSubscription wildcardSub1 = buildSubscription(
                UUID.randomUUID(), PROTOCOL_DEF_ID, null, "sms", ADAPTOR_ID_1, adaptor1);

        // findRoutableSubscriptions returns step-specific + wildcards
        when(subscriptionRepository.findRoutableSubscriptions(any(), any(), any()))
                .thenReturn(List.of(stepSub, wildcardSub1, wildcardSub2, wildcardSub3));

        // Idempotency check — default no duplicates
        when(deliveryRepository.existsByIntelligenceEventIdAndChannelSubscriptionId(any(), any()))
                .thenReturn(false);

        // findByIntelligenceEventId — returns empty list (no prior deliveries)
        when(deliveryRepository.findByIntelligenceEventId(any())).thenReturn(List.of());

        // Save returns delivery with generated ID
        when(deliveryRepository.save(any(IntelligenceDelivery.class))).thenAnswer(invocation -> {
            IntelligenceDelivery d = invocation.getArgument(0);
            if (d.getId() == null) d.setId(UUID.randomUUID());
            savedDeliveries.add(d);
            return d;
        });

        // findById — return the latest saved version of the delivery
        when(deliveryRepository.findById(any(UUID.class))).thenAnswer(invocation -> {
            UUID id = invocation.getArgument(0);
            // Return the most recent version with this ID
            return savedDeliveries.stream()
                    .filter(d -> id.equals(d.getId()))
                    .reduce((first, second) -> second)
                    .map(Optional::of)
                    .orElse(Optional.empty());
        });

        // Audit log save
        when(auditLogRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    @Order(1)
    void testOverdueTriggerProducesDeliveries() throws Exception {
        enqueueSuccessResponses(3);

        UUID eventId = UUID.randomUUID();
        IntelligenceTriggerEvent event = buildTriggerEvent(eventId, "overdue", "MEDIUM", "CommunicationRequest", "action-overdue-check");

        kafkaTemplate.send("intelligence-triggers-test", event).get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            // 3 deliveries: step-specific for adaptor1, wildcard for adaptor2, wildcard for adaptor3
            assertThat(savedDeliveries).hasSizeGreaterThanOrEqualTo(3 * 3); // 3 saves per delivery (create, update to executing, final)
            List<IntelligenceDelivery> delivered = savedDeliveries.stream()
                    .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                    .toList();
            assertThat(delivered).hasSize(3);
        });

        // Validate FHIR payload is CommunicationRequest
        IntelligenceDelivery delivery = savedDeliveries.stream()
                .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                .findFirst().orElseThrow();
        JsonNode payload = delivery.getFhirPayload();
        assertThat(payload.get("resourceType").asText()).isEqualTo("CommunicationRequest");
        assertThat(payload.get("status").asText()).isEqualTo("active");
        assertThat(payload.get("priority").asText()).isEqualTo("urgent"); // MEDIUM -> urgent
        assertThat(payload.has("subject")).isTrue();
        assertThat(payload.has("payload")).isTrue();
        assertThat(payload.has("extension")).isTrue();
    }

    @Test
    @Order(2)
    void testEscalationCriticalPriority() throws Exception {
        enqueueSuccessResponses(3);

        UUID eventId = UUID.randomUUID();
        IntelligenceTriggerEvent event = buildTriggerEvent(eventId, "overdue", "CRITICAL", "CommunicationRequest", "action-overdue-check");

        kafkaTemplate.send("intelligence-triggers-test", event).get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<IntelligenceDelivery> delivered = savedDeliveries.stream()
                    .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                    .toList();
            assertThat(delivered).isNotEmpty();
            assertThat(delivered.get(0).getFhirPayload().get("priority").asText()).isEqualTo("asap");
        });
    }

    @Test
    @Order(3)
    void testCoordinationTriggerBuildsFhirTask() throws Exception {
        enqueueSuccessResponses(3);

        UUID eventId = UUID.randomUUID();
        IntelligenceTriggerEvent event = buildTriggerEvent(eventId, "overdue", "HIGH", "Task", "action-overdue-check");

        kafkaTemplate.send("intelligence-triggers-test", event).get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<IntelligenceDelivery> delivered = savedDeliveries.stream()
                    .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                    .toList();
            assertThat(delivered).isNotEmpty();
            assertThat(delivered.get(0).getFhirPayload().get("resourceType").asText()).isEqualTo("Task");
        });
    }

    @Test
    @Order(4)
    void testDuplicateTriggerIdempotency() throws Exception {
        // First call — allow through
        UUID eventId = UUID.randomUUID();

        enqueueSuccessResponses(3);
        kafkaTemplate.send("intelligence-triggers-test", buildTriggerEvent(eventId, "overdue", "LOW", "CommunicationRequest", "action-overdue-check"))
                .get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<IntelligenceDelivery> delivered = savedDeliveries.stream()
                    .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                    .toList();
            assertThat(delivered).hasSize(3);
        });

        int countAfterFirst = savedDeliveries.size();

        // Mark duplicates exist for second trigger
        when(deliveryRepository.existsByIntelligenceEventIdAndChannelSubscriptionId(eq(eventId), any()))
                .thenReturn(true);

        kafkaTemplate.send("intelligence-triggers-test", buildTriggerEvent(eventId, "overdue", "LOW", "CommunicationRequest", "action-overdue-check"))
                .get(5, TimeUnit.SECONDS);

        // Wait briefly and verify no new saves
        Thread.sleep(3000);
        assertThat(savedDeliveries).hasSize(countAfterFirst);
    }

    @Test
    @Order(5)
    void testWebhook503RetrySuccess() throws Exception {
        // 2 succeed, 1 gets 503 twice then 200
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        mockWebServer.enqueue(new MockResponse().setResponseCode(503).setBody("Service Unavailable"));
        mockWebServer.enqueue(new MockResponse().setResponseCode(503).setBody("Service Unavailable"));
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        UUID eventId = UUID.randomUUID();
        kafkaTemplate.send("intelligence-triggers-test", buildTriggerEvent(eventId, "overdue", "LOW", "CommunicationRequest", "action-overdue-check"))
                .get(5, TimeUnit.SECONDS);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<IntelligenceDelivery> delivered = savedDeliveries.stream()
                    .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                    .toList();
            assertThat(delivered).hasSize(3);
        });

        // Verify the retried one has attemptCount > 1
        IntelligenceDelivery retried = savedDeliveries.stream()
                .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED && d.getAttemptCount() > 1)
                .findFirst().orElse(null);
        assertThat(retried).isNotNull();
        assertThat(retried.getAttemptCount()).isEqualTo(3);
    }

    @Test
    @Order(6)
    void testWebhook400NonRetryableFails() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        mockWebServer.enqueue(new MockResponse().setResponseCode(400).setBody("Bad Request"));

        UUID eventId = UUID.randomUUID();
        kafkaTemplate.send("intelligence-triggers-test", buildTriggerEvent(eventId, "overdue", "LOW", "CommunicationRequest", "action-overdue-check"))
                .get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            long delivered = savedDeliveries.stream().filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED).count();
            long failed = savedDeliveries.stream().filter(d -> d.getStatus() == IntelligenceDeliveryStatus.FAILED).count();
            assertThat(delivered).isEqualTo(2);
            assertThat(failed).isEqualTo(1);
        });

        // Verify the failed one has attemptCount = 1 (no retries for 4xx)
        IntelligenceDelivery failedDelivery = savedDeliveries.stream()
                .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.FAILED)
                .findFirst().orElseThrow();
        assertThat(failedDelivery.getAttemptCount()).isEqualTo(1);
    }

    @Test
    @Order(7)
    void testStepLevelRoutingOverridesWildcard() throws Exception {
        enqueueSuccessResponses(3);

        UUID eventId = UUID.randomUUID();
        kafkaTemplate.send("intelligence-triggers-test", buildTriggerEvent(eventId, "overdue", "LOW", "CommunicationRequest", "action-overdue-check"))
                .get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<IntelligenceDelivery> delivered = savedDeliveries.stream()
                    .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                    .toList();
            // 3 deliveries: step-specific for adaptor1 (overrides wildcard), wildcard for adaptor2, wildcard for adaptor3
            assertThat(delivered).hasSize(3);
        });
    }

    @Test
    @Order(8)
    void testLateCompletionTrigger() throws Exception {
        enqueueSuccessResponses(3);

        UUID eventId = UUID.randomUUID();
        kafkaTemplate.send("intelligence-triggers-test", buildTriggerEvent(eventId, "completed", "LOW", "CommunicationRequest", "action-overdue-check"))
                .get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<IntelligenceDelivery> delivered = savedDeliveries.stream()
                    .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                    .toList();
            assertThat(delivered).hasSize(3);
        });
    }

    @Test
    @Order(9)
    void testFhirPayloadStructureValidation() throws Exception {
        enqueueSuccessResponses(3);

        UUID eventId = UUID.randomUUID();
        kafkaTemplate.send("intelligence-triggers-test", buildTriggerEvent(eventId, "overdue", "HIGH", "CommunicationRequest", "action-overdue-check"))
                .get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(savedDeliveries.stream().filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED).count()).isGreaterThanOrEqualTo(1);
        });

        IntelligenceDelivery delivery = savedDeliveries.stream()
                .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                .findFirst().orElseThrow();
        JsonNode payload = delivery.getFhirPayload();

        assertThat(payload.get("resourceType").asText()).isEqualTo("CommunicationRequest");
        assertThat(payload.has("identifier")).isTrue();
        assertThat(payload.get("identifier").isArray()).isTrue();
        assertThat(payload.get("status").asText()).isEqualTo("active");
        assertThat(payload.get("priority").asText()).isEqualTo("urgent"); // HIGH -> urgent
        assertThat(payload.has("category")).isTrue();
        assertThat(payload.has("subject")).isTrue();
        assertThat(payload.get("subject").has("reference")).isTrue();
        assertThat(payload.has("payload")).isTrue();
        assertThat(payload.has("authoredOn")).isTrue();
        assertThat(payload.has("extension")).isTrue();

        // Check extensions contain cce-severity, cce-step-state
        JsonNode extensions = payload.get("extension");
        assertThat(extensions.isArray()).isTrue();
        List<String> extUrls = new ArrayList<>();
        extensions.forEach(ext -> extUrls.add(ext.get("url").asText()));
        assertThat(extUrls).contains(
                "http://openphc.org/fhir/StructureDefinition/cce-severity",
                "http://openphc.org/fhir/StructureDefinition/cce-step-state"
        );
    }

    private void enqueueSuccessResponses(int count) {
        for (int i = 0; i < count; i++) {
            mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        }
    }

    private ReceiverAdaptor buildAdaptor(UUID id, String name, String address) {
        ObjectNode definition = objectMapper.createObjectNode();
        definition.put("resourceType", "Endpoint");
        definition.put("status", "active");
        definition.put("address", address);
        definition.putObject("connectionType").put("code", "hl7-fhir-rest");

        return ReceiverAdaptor.builder()
                .id(id)
                .name(name)
                .definition(definition)
                .status("ACTIVE")
                .config(null)
                .build();
    }

    private ChannelSubscription buildSubscription(UUID id, UUID protocolDefId, String actionId,
                                                   String channel, UUID adaptorId, ReceiverAdaptor adaptor) {
        return ChannelSubscription.builder()
                .id(id)
                .protocolDefinitionId(protocolDefId)
                .actionId(actionId)
                .channel(channel)
                .receiverAdaptorId(adaptorId)
                .receiverAdaptor(adaptor)
                .status("ACTIVE")
                .build();
    }

    private IntelligenceTriggerEvent buildTriggerEvent(UUID eventId, String stepState, String severity, String actionType, String actionId) {
        IntelligenceTriggerEvent event = new IntelligenceTriggerEvent();
        event.setId(UUID.randomUUID());
        event.setSubject("Patient/test-patient-123");
        event.setIntelligenceEventId(eventId);
        event.setActionDefinitionId(UUID.randomUUID());
        event.setProtocolDefinitionId(PROTOCOL_DEF_ID);
        event.setActionType(actionType);
        event.setSeverity(severity);
        event.setIntelligenceChannel("sms");
        event.setStepState(stepState);
        event.setActionId(actionId);
        event.setProtocolCanonical("http://example.org/PlanDefinition/test-protocol");
        event.setDetectedAt(OffsetDateTime.now());
        return event;
    }
}
