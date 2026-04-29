package org.openphc.cce.intelligence.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;
import org.openphc.cce.intelligence.domain.entity.ReceiverAdaptor;
import org.openphc.cce.intelligence.service.ReceiverAdaptorService;
import org.openphc.cce.intelligence.web.controller.ReceiverAdaptorController;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ReceiverAdaptorController.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReceiverAdaptorApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ReceiverAdaptorService adaptorService;

    private static final UUID ADAPTOR_ID = UUID.fromString("22222222-2222-2222-2222-222222222221");

    private ReceiverAdaptor buildAdaptor(UUID id, String name, String status) {
        ObjectNode definition = objectMapper.createObjectNode();
        definition.put("resourceType", "Endpoint");
        definition.put("status", "active");
        definition.put("address", "http://localhost:9999/webhook");
        definition.putObject("connectionType").put("code", "hl7-fhir-rest");

        return ReceiverAdaptor.builder()
                .id(id)
                .name(name)
                .definition(definition)
                .status(status)
                .config(null)
                .createdAt(OffsetDateTime.now())
                .updatedAt(OffsetDateTime.now())
                .build();
    }

    @Test
    @Order(1)
    void testCreateAdaptor() throws Exception {
        ReceiverAdaptor created = buildAdaptor(ADAPTOR_ID, "Test New Adaptor", "ACTIVE");
        when(adaptorService.create(eq("Test New Adaptor"), any(JsonNode.class), isNull()))
                .thenReturn(created);

        ObjectNode definition = objectMapper.createObjectNode();
        definition.put("resourceType", "Endpoint");
        definition.put("status", "active");
        definition.put("address", "http://localhost:9999/new-adaptor");
        definition.putObject("connectionType").put("code", "hl7-fhir-rest");

        ObjectNode request = objectMapper.createObjectNode();
        request.put("name", "Test New Adaptor");
        request.set("definition", definition);

        mockMvc.perform(post("/v1/receiver-adaptors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.data.name").value("Test New Adaptor"))
                .andExpect(jsonPath("$.data.status").value("ACTIVE"))
                .andExpect(jsonPath("$.data.id").value(ADAPTOR_ID.toString()));
    }

    @Test
    @Order(2)
    void testCreateAdaptorInvalidDefinition() throws Exception {
        when(adaptorService.create(eq("Bad Adaptor"), any(JsonNode.class), isNull()))
                .thenThrow(new IllegalArgumentException("definition.resourceType must be 'Endpoint'"));

        ObjectNode definition = objectMapper.createObjectNode();
        definition.put("resourceType", "Organization");

        ObjectNode request = objectMapper.createObjectNode();
        request.put("name", "Bad Adaptor");
        request.set("definition", definition);

        mockMvc.perform(post("/v1/receiver-adaptors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.message").value("definition.resourceType must be 'Endpoint'"));
    }

    @Test
    @Order(3)
    void testCreateAdaptorValidationError() throws Exception {
        ObjectNode request = objectMapper.createObjectNode();
        // Missing name and definition

        mockMvc.perform(post("/v1/receiver-adaptors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.fieldErrors").isArray());
    }

    @Test
    @Order(4)
    void testListAdaptors() throws Exception {
        List<ReceiverAdaptor> adaptors = List.of(
                buildAdaptor(ADAPTOR_ID, "EHR Adaptor", "ACTIVE"),
                buildAdaptor(UUID.randomUUID(), "Lab Adaptor", "ACTIVE")
        );
        when(adaptorService.findAll()).thenReturn(adaptors);

        mockMvc.perform(get("/v1/receiver-adaptors"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(2));
    }

    @Test
    @Order(5)
    void testListAdaptorsByStatus() throws Exception {
        List<ReceiverAdaptor> adaptors = List.of(buildAdaptor(ADAPTOR_ID, "EHR Adaptor", "ACTIVE"));
        when(adaptorService.findByStatus("ACTIVE")).thenReturn(adaptors);

        mockMvc.perform(get("/v1/receiver-adaptors").param("status", "ACTIVE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(1))
                .andExpect(jsonPath("$.data[0].status").value("ACTIVE"));
    }

    @Test
    @Order(6)
    void testGetAdaptorById() throws Exception {
        ReceiverAdaptor adaptor = buildAdaptor(ADAPTOR_ID, "EHR Adaptor", "ACTIVE");
        when(adaptorService.findById(ADAPTOR_ID)).thenReturn(Optional.of(adaptor));

        mockMvc.perform(get("/v1/receiver-adaptors/" + ADAPTOR_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.id").value(ADAPTOR_ID.toString()))
                .andExpect(jsonPath("$.data.name").value("EHR Adaptor"));
    }

    @Test
    @Order(7)
    void testGetAdaptorByIdNotFound() throws Exception {
        UUID randomId = UUID.randomUUID();
        when(adaptorService.findById(randomId)).thenReturn(Optional.empty());

        mockMvc.perform(get("/v1/receiver-adaptors/" + randomId))
                .andExpect(status().isNotFound());
    }

    @Test
    @Order(8)
    void testUpdateAdaptor() throws Exception {
        ReceiverAdaptor updated = buildAdaptor(ADAPTOR_ID, "EHR Adaptor", "INACTIVE");
        when(adaptorService.update(eq(ADAPTOR_ID), isNull(), isNull(), isNull(), eq("INACTIVE")))
                .thenReturn(updated);

        ObjectNode request = objectMapper.createObjectNode();
        request.put("status", "INACTIVE");

        mockMvc.perform(put("/v1/receiver-adaptors/" + ADAPTOR_ID)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value("INACTIVE"));
    }

    @Test
    @Order(9)
    void testDeleteAdaptorBlockedByActiveSubscriptions() throws Exception {
        doThrow(new IllegalStateException("Cannot delete adaptor with active channel subscriptions"))
                .when(adaptorService).delete(ADAPTOR_ID);

        mockMvc.perform(delete("/v1/receiver-adaptors/" + ADAPTOR_ID))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.message").value("Cannot delete adaptor with active channel subscriptions"));
    }

    @Test
    @Order(10)
    void testDeleteAdaptorSuccess() throws Exception {
        UUID newId = UUID.randomUUID();
        doNothing().when(adaptorService).delete(newId);

        mockMvc.perform(delete("/v1/receiver-adaptors/" + newId))
                .andExpect(status().isNoContent());
    }
}
