package org.openphc.cce.intelligence.web.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.openphc.cce.intelligence.domain.entity.DestinationAdaptorMapping;
import org.openphc.cce.intelligence.service.DestinationAdaptorMappingService;
import org.openphc.cce.intelligence.web.dto.ApiResponse;
import org.openphc.cce.intelligence.web.dto.CreateDestinationAdaptorMappingRequest;
import org.openphc.cce.intelligence.web.dto.DestinationAdaptorMappingDto;
import org.openphc.cce.intelligence.web.dto.UpdateDestinationAdaptorMappingRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/destination-adaptor-mappings")
@RequiredArgsConstructor
public class DestinationAdaptorMappingController {

    private final DestinationAdaptorMappingService mappingService;

    @PostMapping
    public ResponseEntity<ApiResponse<DestinationAdaptorMappingDto>> create(@Valid @RequestBody CreateDestinationAdaptorMappingRequest request) {
        DestinationAdaptorMapping mapping = mappingService.create(
                request.getDestination(),
                request.getReceiverAdaptorId()
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.of(toDto(mapping)));
    }

    @GetMapping
    public ApiResponse<List<DestinationAdaptorMappingDto>> list(
            @RequestParam(required = false) String destination,
            @RequestParam(required = false) UUID receiverAdaptorId,
            @RequestParam(required = false) String status) {
        List<DestinationAdaptorMapping> mappings = mappingService.findFiltered(destination, receiverAdaptorId, status);
        return ApiResponse.of(mappings.stream().map(this::toDto).toList());
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<DestinationAdaptorMappingDto>> getById(@PathVariable UUID id) {
        return mappingService.findById(id)
                .map(this::toDto)
                .map(dto -> ResponseEntity.ok(ApiResponse.of(dto)))
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public ResponseEntity<ApiResponse<DestinationAdaptorMappingDto>> update(@PathVariable UUID id,
                                                         @Valid @RequestBody UpdateDestinationAdaptorMappingRequest request) {
        DestinationAdaptorMapping mapping = mappingService.update(id, request.getReceiverAdaptorId(), request.getStatus());
        return ResponseEntity.ok(ApiResponse.of(toDto(mapping)));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        mappingService.delete(id);
        return ResponseEntity.noContent().build();
    }

    private DestinationAdaptorMappingDto toDto(DestinationAdaptorMapping m) {
        return DestinationAdaptorMappingDto.builder()
                .id(m.getId())
                .destination(m.getDestination())
                .receiverAdaptorId(m.getReceiverAdaptorId())
                .receiverAdaptorName(m.getReceiverAdaptor() != null ? m.getReceiverAdaptor().getName() : null)
                .status(m.getStatus())
                .createdAt(m.getCreatedAt())
                .updatedAt(m.getUpdatedAt())
                .build();
    }
}
