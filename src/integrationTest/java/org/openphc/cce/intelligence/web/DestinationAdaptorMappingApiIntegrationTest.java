package org.openphc.cce.intelligence.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;
import org.openphc.cce.intelligence.domain.entity.DestinationAdaptorMapping;
import org.openphc.cce.intelligence.domain.entity.ReceiverAdaptor;
import org.openphc.cce.intelligence.service.DestinationAdaptorMappingService;
import org.openphc.cce.intelligence.web.controller.DestinationAdaptorMappingController;
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

@WebMvcTest(DestinationAdaptorMappingController.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DestinationAdaptorMappingApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private DestinationAdaptorMappingService mappingService;

    private static final UUID ADAPTOR_ID = UUID.fromString("22222222-2222-2222-2222-222222222221");
    private static final UUID MAPPING_ID = UUID.fromString("33333333-3333-3333-3333-333333333331");

    private DestinationAdaptorMapping buildMapping(UUID id, String destination, String status) {
        ReceiverAdaptor adaptor = ReceiverAdaptor.builder()
                .id(ADAPTOR_ID)
                .name("Hospital EHR Adaptor")
                .status("ACTIVE")
                .build();

        return DestinationAdaptorMapping.builder()
                .id(id)
                .destination(destination)
                .receiverAdaptorId(ADAPTOR_ID)
                .receiverAdaptor(adaptor)
                .status(status)
                .createdAt(OffsetDateTime.now())
                .updatedAt(OffsetDateTime.now())
                .build();
    }

    @Test
    @Order(1)
    void testCreateMapping() throws Exception {
        DestinationAdaptorMapping created = buildMapping(MAPPING_ID, "email", "ACTIVE");
        when(mappingService.create(eq("email"), eq(ADAPTOR_ID)))
                .thenReturn(created);

        ObjectNode request = objectMapper.createObjectNode();
        request.put("destination", "email");
        request.put("receiverAdaptorId", ADAPTOR_ID.toString());

        mockMvc.perform(post("/v1/destination-adaptor-mappings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.data.id").value(MAPPING_ID.toString()))
                .andExpect(jsonPath("$.data.destination").value("email"))
                .andExpect(jsonPath("$.data.status").value("ACTIVE"))
                .andExpect(jsonPath("$.data.receiverAdaptorName").value("Hospital EHR Adaptor"));
    }

    @Test
    @Order(2)
    void testCreateMappingValidationError() throws Exception {
        ObjectNode request = objectMapper.createObjectNode();
        // Missing required fields

        mockMvc.perform(post("/v1/destination-adaptor-mappings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.fieldErrors").isArray());
    }

    @Test
    @Order(3)
    void testListMappings() throws Exception {
        List<DestinationAdaptorMapping> mappings = List.of(
                buildMapping(MAPPING_ID, "sms", "ACTIVE"),
                buildMapping(UUID.randomUUID(), "email", "ACTIVE")
        );
        when(mappingService.findFiltered(isNull(), isNull(), isNull())).thenReturn(mappings);

        mockMvc.perform(get("/v1/destination-adaptor-mappings"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(2));
    }

    @Test
    @Order(4)
    void testGetMappingById() throws Exception {
        DestinationAdaptorMapping mapping = buildMapping(MAPPING_ID, "sms", "ACTIVE");
        when(mappingService.findById(MAPPING_ID)).thenReturn(Optional.of(mapping));

        mockMvc.perform(get("/v1/destination-adaptor-mappings/" + MAPPING_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.id").value(MAPPING_ID.toString()))
                .andExpect(jsonPath("$.data.destination").value("sms"));
    }

    @Test
    @Order(5)
    void testGetMappingByIdNotFound() throws Exception {
        UUID randomId = UUID.randomUUID();
        when(mappingService.findById(randomId)).thenReturn(Optional.empty());

        mockMvc.perform(get("/v1/destination-adaptor-mappings/" + randomId))
                .andExpect(status().isNotFound());
    }

    @Test
    @Order(6)
    void testUpdateMapping() throws Exception {
        DestinationAdaptorMapping updated = buildMapping(MAPPING_ID, "sms", "INACTIVE");
        when(mappingService.update(eq(MAPPING_ID), isNull(), eq("INACTIVE"))).thenReturn(updated);

        ObjectNode request = objectMapper.createObjectNode();
        request.put("status", "INACTIVE");

        mockMvc.perform(put("/v1/destination-adaptor-mappings/" + MAPPING_ID)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value("INACTIVE"));
    }

    @Test
    @Order(7)
    void testDeleteMapping() throws Exception {
        doNothing().when(mappingService).delete(MAPPING_ID);

        mockMvc.perform(delete("/v1/destination-adaptor-mappings/" + MAPPING_ID))
                .andExpect(status().isNoContent());
    }

    @Test
    @Order(8)
    void testDeleteMappingWithActiveDeliveries() throws Exception {
        when(mappingService.findById(MAPPING_ID)).thenReturn(Optional.of(buildMapping(MAPPING_ID, "sms", "ACTIVE")));
        org.mockito.Mockito.doThrow(new IllegalStateException("Cannot delete mapping with active (PENDING/EXECUTING) intelligence deliveries"))
                .when(mappingService).delete(MAPPING_ID);

        mockMvc.perform(delete("/v1/destination-adaptor-mappings/" + MAPPING_ID))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.message").value("Cannot delete mapping with active (PENDING/EXECUTING) intelligence deliveries"));
    }
}
