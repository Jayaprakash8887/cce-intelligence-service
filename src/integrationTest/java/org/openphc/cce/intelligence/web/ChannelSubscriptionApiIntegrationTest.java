package org.openphc.cce.intelligence.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;
import org.openphc.cce.intelligence.domain.entity.ChannelSubscription;
import org.openphc.cce.intelligence.domain.entity.ReceiverAdaptor;
import org.openphc.cce.intelligence.service.ChannelSubscriptionService;
import org.openphc.cce.intelligence.web.controller.ChannelSubscriptionController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ChannelSubscriptionController.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ChannelSubscriptionApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ChannelSubscriptionService subscriptionService;

    private static final UUID PROTOCOL_DEF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID ADAPTOR_ID = UUID.fromString("22222222-2222-2222-2222-222222222221");
    private static final UUID SUB_ID = UUID.fromString("33333333-3333-3333-3333-333333333331");

    private ChannelSubscription buildSubscription(UUID id, String actionId, String channel, String status) {
        ReceiverAdaptor adaptor = ReceiverAdaptor.builder()
                .id(ADAPTOR_ID)
                .name("Hospital EHR Adaptor")
                .status("ACTIVE")
                .build();

        return ChannelSubscription.builder()
                .id(id)
                .protocolDefinitionId(PROTOCOL_DEF_ID)
                .actionId(actionId)
                .channel(channel)
                .receiverAdaptorId(ADAPTOR_ID)
                .receiverAdaptor(adaptor)
                .status(status)
                .createdAt(OffsetDateTime.now())
                .updatedAt(OffsetDateTime.now())
                .build();
    }

    @Test
    @Order(1)
    void testCreateSubscription() throws Exception {
        ChannelSubscription created = buildSubscription(SUB_ID, null, "email", "ACTIVE");
        when(subscriptionService.create(eq(PROTOCOL_DEF_ID), isNull(), eq("email"), eq(ADAPTOR_ID)))
                .thenReturn(created);

        ObjectNode request = objectMapper.createObjectNode();
        request.put("protocolDefinitionId", PROTOCOL_DEF_ID.toString());
        request.put("channel", "email");
        request.put("receiverAdaptorId", ADAPTOR_ID.toString());

        mockMvc.perform(post("/v1/channel-subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.data.id").value(SUB_ID.toString()))
                .andExpect(jsonPath("$.data.channel").value("email"))
                .andExpect(jsonPath("$.data.status").value("ACTIVE"))
                .andExpect(jsonPath("$.data.receiverAdaptorName").value("Hospital EHR Adaptor"));
    }

    @Test
    @Order(2)
    void testCreateSubscriptionWithActionId() throws Exception {
        ChannelSubscription created = buildSubscription(SUB_ID, "specific-action-step", "push", "ACTIVE");
        when(subscriptionService.create(eq(PROTOCOL_DEF_ID), eq("specific-action-step"), eq("push"), eq(ADAPTOR_ID)))
                .thenReturn(created);

        ObjectNode request = objectMapper.createObjectNode();
        request.put("protocolDefinitionId", PROTOCOL_DEF_ID.toString());
        request.put("actionId", "specific-action-step");
        request.put("channel", "push");
        request.put("receiverAdaptorId", ADAPTOR_ID.toString());

        mockMvc.perform(post("/v1/channel-subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.data.actionId").value("specific-action-step"));
    }

    @Test
    @Order(3)
    void testCreateSubscriptionValidationError() throws Exception {
        ObjectNode request = objectMapper.createObjectNode();
        // Missing required fields

        mockMvc.perform(post("/v1/channel-subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.fieldErrors").isArray());
    }

    @Test
    @Order(4)
    void testListSubscriptions() throws Exception {
        List<ChannelSubscription> subs = List.of(
                buildSubscription(SUB_ID, null, "sms", "ACTIVE"),
                buildSubscription(UUID.randomUUID(), "action-1", "email", "ACTIVE")
        );
        when(subscriptionService.findFiltered(isNull(), isNull(), isNull(), isNull(), isNull())).thenReturn(subs);

        mockMvc.perform(get("/v1/channel-subscriptions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(2));
    }

    @Test
    @Order(5)
    void testGetSubscriptionById() throws Exception {
        ChannelSubscription sub = buildSubscription(SUB_ID, null, "sms", "ACTIVE");
        when(subscriptionService.findById(SUB_ID)).thenReturn(Optional.of(sub));

        mockMvc.perform(get("/v1/channel-subscriptions/" + SUB_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.id").value(SUB_ID.toString()))
                .andExpect(jsonPath("$.data.channel").value("sms"));
    }

    @Test
    @Order(6)
    void testGetSubscriptionByIdNotFound() throws Exception {
        UUID randomId = UUID.randomUUID();
        when(subscriptionService.findById(randomId)).thenReturn(Optional.empty());

        mockMvc.perform(get("/v1/channel-subscriptions/" + randomId))
                .andExpect(status().isNotFound());
    }

    @Test
    @Order(7)
    void testUpdateSubscriptionStatus() throws Exception {
        ChannelSubscription updated = buildSubscription(SUB_ID, null, "sms", "INACTIVE");
        when(subscriptionService.updateStatus(SUB_ID, "INACTIVE")).thenReturn(updated);

        ObjectNode request = objectMapper.createObjectNode();
        request.put("status", "INACTIVE");

        mockMvc.perform(put("/v1/channel-subscriptions/" + SUB_ID)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value("INACTIVE"));
    }

    @Test
    @Order(8)
    void testDeleteSubscription() throws Exception {
        doNothing().when(subscriptionService).delete(SUB_ID);

        mockMvc.perform(delete("/v1/channel-subscriptions/" + SUB_ID))
                .andExpect(status().isNoContent());
    }

    @Test
    @Order(9)
    void testDeleteSubscriptionWithActiveDeliveries() throws Exception {
        when(subscriptionService.findById(SUB_ID)).thenReturn(Optional.of(buildSubscription(SUB_ID, null, "sms", "ACTIVE")));
        org.mockito.Mockito.doThrow(new IllegalStateException("Cannot delete subscription with active (PENDING/EXECUTING) intelligence deliveries"))
                .when(subscriptionService).delete(SUB_ID);

        mockMvc.perform(delete("/v1/channel-subscriptions/" + SUB_ID))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.message").value("Cannot delete subscription with active (PENDING/EXECUTING) intelligence deliveries"));
    }
}
