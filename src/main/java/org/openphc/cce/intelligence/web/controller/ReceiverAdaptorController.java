package org.openphc.cce.intelligence.web.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.openphc.cce.intelligence.domain.entity.ReceiverAdaptor;
import org.openphc.cce.intelligence.service.ReceiverAdaptorService;
import org.openphc.cce.intelligence.web.dto.ApiResponse;
import org.openphc.cce.intelligence.web.dto.CreateReceiverAdaptorRequest;
import org.openphc.cce.intelligence.web.dto.ReceiverAdaptorDto;
import org.openphc.cce.intelligence.web.dto.UpdateReceiverAdaptorRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/receiver-adaptors")
@RequiredArgsConstructor
public class ReceiverAdaptorController {

    private final ReceiverAdaptorService adaptorService;

    @PostMapping
    public ResponseEntity<ApiResponse<ReceiverAdaptorDto>> create(@Valid @RequestBody CreateReceiverAdaptorRequest request) {
        ReceiverAdaptor adaptor = adaptorService.create(
                request.getName(),
                request.getDefinition(),
                request.getConfig()
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.of(toDto(adaptor)));
    }

    @GetMapping
    public ApiResponse<List<ReceiverAdaptorDto>> list(@RequestParam(required = false) String status) {
        List<ReceiverAdaptor> adaptors = status != null
                ? adaptorService.findByStatus(status)
                : adaptorService.findAll();
        return ApiResponse.of(adaptors.stream().map(this::toDto).toList());
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<ReceiverAdaptorDto>> getById(@PathVariable UUID id) {
        return adaptorService.findById(id)
                .map(this::toDto)
                .map(dto -> ResponseEntity.ok(ApiResponse.of(dto)))
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public ResponseEntity<ApiResponse<ReceiverAdaptorDto>> update(@PathVariable UUID id,
                                                     @RequestBody UpdateReceiverAdaptorRequest request) {
        ReceiverAdaptor adaptor = adaptorService.update(
                id,
                request.getName(),
                request.getDefinition(),
                request.getConfig(),
                request.getStatus()
        );
        return ResponseEntity.ok(ApiResponse.of(toDto(adaptor)));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        adaptorService.delete(id);
        return ResponseEntity.noContent().build();
    }

    private ReceiverAdaptorDto toDto(ReceiverAdaptor a) {
        return ReceiverAdaptorDto.builder()
                .id(a.getId())
                .name(a.getName())
                .definition(a.getDefinition())
                .status(a.getStatus())
                .config(maskConfig(a.getConfig()))
                .createdAt(a.getCreatedAt())
                .updatedAt(a.getUpdatedAt())
                .build();
    }

    private JsonNode maskConfig(JsonNode config) {
        if (config == null || !config.isObject()) return config;
        ObjectNode masked = config.deepCopy();
        if (masked.has("authValue")) {
            String original = masked.get("authValue").asText();
            String maskedValue = original.length() > 6
                    ? original.substring(0, 3) + "***" + original.substring(original.length() - 3)
                    : "***";
            masked.put("authValue", maskedValue);
        }
        if (masked.has("webhookSecret")) {
            masked.put("webhookSecret", "***");
        }
        return masked;
    }
}
