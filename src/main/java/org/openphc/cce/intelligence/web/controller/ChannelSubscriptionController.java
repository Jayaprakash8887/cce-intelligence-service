package org.openphc.cce.intelligence.web.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.openphc.cce.intelligence.domain.entity.ChannelSubscription;
import org.openphc.cce.intelligence.service.ChannelSubscriptionService;
import org.openphc.cce.intelligence.web.dto.ApiResponse;
import org.openphc.cce.intelligence.web.dto.ChannelSubscriptionDto;
import org.openphc.cce.intelligence.web.dto.CreateChannelSubscriptionRequest;
import org.openphc.cce.intelligence.web.dto.UpdateChannelSubscriptionRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/channel-subscriptions")
@RequiredArgsConstructor
public class ChannelSubscriptionController {

    private final ChannelSubscriptionService subscriptionService;

    @PostMapping
    public ResponseEntity<ApiResponse<ChannelSubscriptionDto>> create(@Valid @RequestBody CreateChannelSubscriptionRequest request) {
        ChannelSubscription subscription = subscriptionService.create(
                request.getProtocolDefinitionId(),
                request.getActionId(),
                request.getChannel(),
                request.getReceiverAdaptorId()
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.of(toDto(subscription)));
    }

    @GetMapping
    public ApiResponse<List<ChannelSubscriptionDto>> list(
            @RequestParam(required = false) UUID protocolDefinitionId,
            @RequestParam(required = false) String actionId,
            @RequestParam(required = false) String channel,
            @RequestParam(required = false) UUID receiverAdaptorId,
            @RequestParam(required = false) String status) {
        List<ChannelSubscription> subscriptions = subscriptionService.findFiltered(
                protocolDefinitionId, actionId, channel, receiverAdaptorId, status);
        return ApiResponse.of(subscriptions.stream().map(this::toDto).toList());
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<ChannelSubscriptionDto>> getById(@PathVariable UUID id) {
        return subscriptionService.findById(id)
                .map(this::toDto)
                .map(dto -> ResponseEntity.ok(ApiResponse.of(dto)))
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public ResponseEntity<ApiResponse<ChannelSubscriptionDto>> update(@PathVariable UUID id,
                                                         @Valid @RequestBody UpdateChannelSubscriptionRequest request) {
        ChannelSubscription subscription = subscriptionService.updateStatus(id, request.getStatus());
        return ResponseEntity.ok(ApiResponse.of(toDto(subscription)));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        subscriptionService.delete(id);
        return ResponseEntity.noContent().build();
    }

    private ChannelSubscriptionDto toDto(ChannelSubscription s) {
        return ChannelSubscriptionDto.builder()
                .id(s.getId())
                .protocolDefinitionId(s.getProtocolDefinitionId())
                .actionId(s.getActionId())
                .channel(s.getChannel())
                .receiverAdaptorId(s.getReceiverAdaptorId())
                .receiverAdaptorName(s.getReceiverAdaptor() != null ? s.getReceiverAdaptor().getName() : null)
                .status(s.getStatus())
                .createdAt(s.getCreatedAt())
                .updatedAt(s.getUpdatedAt())
                .build();
    }
}
