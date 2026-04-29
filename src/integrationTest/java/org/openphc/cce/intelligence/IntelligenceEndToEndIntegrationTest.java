package org.openphc.cce.intelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;
import org.openphc.cce.intelligence.domain.entity.ChannelSubscription;
import org.openphc.cce.intelligence.domain.entity.IntelligenceDelivery;
import org.openphc.cce.intelligence.domain.entity.ReceiverAdaptor;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.openphc.cce.intelligence.domain.repository.ChannelSubscriptionRepository;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryAuditLogRepository;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryRepository;
import org.openphc.cce.intelligence.domain.repository.ReceiverAdaptorRepository;
import org.openphc.cce.intelligence.kafka.consumer.IntelligenceTriggerConsumer;
import org.openphc.cce.intelligence.webhook.WebhookDeliveryClient;
import org.openphc.cce.intelligence.webhook.WebhookResult;
import org.openphc.cce.intelligence.kafka.model.IntelligenceTriggerEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.kafka.bootstrap-servers=localhost:19092",
                "spring.kafka.listener.auto-startup=false",
                "spring.kafka.admin.fail-fast=false",
                "cce.kafka.topics.intelligence-triggers=test-topic"
        }
)
@EnableAutoConfiguration(exclude = {KafkaAutoConfiguration.class})
@ActiveProfiles("test")
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
        TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
            return new TransactionTemplate(txManager);
        }
    }

    @Autowired
    private IntelligenceTriggerConsumer consumer;

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

    @MockBean
    private WebhookDeliveryClient webhookDeliveryClient;

    private final List<IntelligenceDelivery> savedDeliveries = new CopyOnWriteArrayList<>();

    private static final UUID PROTOCOL_DEF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID ADAPTOR_ID_1 = UUID.fromString("22222222-2222-2222-2222-222222222221");
    private static final UUID ADAPTOR_ID_2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID ADAPTOR_ID_3 = UUID.fromString("22222222-2222-2222-2222-222222222223");

    @BeforeEach
    void setup() {
        savedDeliveries.clear();
        String mockUrl = "http://localhost:9999/webhook";

        ReceiverAdaptor adaptor1 = buildAdaptor(ADAPTOR_ID_1, "Hospital EHR", mockUrl);
        ReceiverAdaptor adaptor2 = buildAdaptor(ADAPTOR_ID_2, "Lab System", mockUrl);
        ReceiverAdaptor adaptor3 = buildAdaptor(ADAPTOR_ID_3, "Notification Hub", mockUrl);

        ChannelSubscription stepSub = buildSubscription(UUID.randomUUID(), PROTOCOL_DEF_ID, "action-overdue-check", "sms", ADAPTOR_ID_1, adaptor1);
        ChannelSubscription wildcardSub1 = buildSubscription(UUID.randomUUID(), PROTOCOL_DEF_ID, null, "sms", ADAPTOR_ID_2, adaptor2);
        ChannelSubscription wildcardSub2 = buildSubscription(UUID.randomUUID(), PROTOCOL_DEF_ID, null, "sms", ADAPTOR_ID_3, adaptor3);

        when(subscriptionRepository.findRoutableSubscriptions(any(), any(), any()))
                .thenReturn(List.of(stepSub, wildcardSub1, wildcardSub2));

        when(deliveryRepository.existsByIntelligenceEventIdAndChannelSubscriptionId(any(), any()))
                .thenReturn(false);

        when(deliveryRepository.findByIntelligenceEventId(any())).thenReturn(List.of());

        when(deliveryRepository.save(any(IntelligenceDelivery.class))).thenAnswer(invocation -> {
            IntelligenceDelivery d = invocation.getArgument(0);
            if (d.getId() == null) d.setId(UUID.randomUUID());
            savedDeliveries.add(d);
            return d;
        });

        when(deliveryRepository.findById(any(UUID.class))).thenAnswer(invocation -> {
            UUID id = invocation.getArgument(0);
            return savedDeliveries.stream()
                    .filter(d -> id.equals(d.getId()))
                    .reduce((first, second) -> second);
        });

        when(auditLogRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        when(webhookDeliveryClient.deliver(any(), any(), any(), any(), any())).thenReturn(
                WebhookResult.success(1));
    }

    @Test
    @Order(1)
    void testOverdueTriggerProducesDeliveries() {
        IntelligenceTriggerEvent event = buildTriggerEvent(UUID.randomUUID(), "overdue", "MEDIUM", "CommunicationRequest", "action-overdue-check");
        try {
            consumer.consume(event);
        } catch (Exception e) {
            System.err.println("TEST_DEBUG_EX: " + e.getClass().getName() + ": " + e.getMessage());
            e.printStackTrace(System.err);
        }
        System.err.println("TEST_DEBUG: savedDeliveries.size=" + savedDeliveries.size());
        savedDeliveries.forEach(d -> System.err.println("TEST_DEBUG: status=" + d.getStatus()));

        List<IntelligenceDelivery> delivered = savedDeliveries.stream()
                .filter(d -> d.getStatus() == IntelligenceDeliveryStatus.DELIVERED)
                .toList();
        assertThat(delivered).hasSize(3);
    }

    @Test
    @Order(2)
    void testDuplicateTriggerIdempotency() {
        UUID eventId = UUID.randomUUID();
        IntelligenceTriggerEvent event = buildTriggerEvent(eventId, "overdue", "LOW", "CommunicationRequest", "action-overdue-check");
        consumer.consume(event);

        int countAfterFirst = savedDeliveries.size();

        when(deliveryRepository.existsByIntelligenceEventIdAndChannelSubscriptionId(eq(eventId), any()))
                .thenReturn(true);

        consumer.consume(event);
        assertThat(savedDeliveries).hasSize(countAfterFirst);
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
