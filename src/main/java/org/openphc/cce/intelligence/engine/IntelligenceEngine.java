package org.openphc.cce.intelligence.engine;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.intelligence.domain.entity.ChannelSubscription;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryRepository;
import org.openphc.cce.intelligence.kafka.model.IntelligenceTriggerEvent;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class IntelligenceEngine {

    private final SubscriptionRouter subscriptionRouter;
    private final FhirPayloadBuilder fhirPayloadBuilder;
    private final ActionDispatcher actionDispatcher;
    private final IntelligenceDeliveryRepository intelligenceDeliveryRepository;

    public void processTrigger(IntelligenceTriggerEvent event) {
        UUID intelligenceEventId = event.getIntelligenceEventId();

        // 1. Resolve channel subscriptions with step-level routing
        List<ChannelSubscription> subscriptions = subscriptionRouter.resolveSubscriptions(
                event.getProtocolDefinitionId(),
                event.getActionId(),
                event.getIntelligenceChannel());

        if (subscriptions.isEmpty()) {
            log.warn("No active subscriptions for trigger: eventId={}, channel={}, protocolDefinitionId={}",
                    intelligenceEventId, event.getIntelligenceChannel(), event.getProtocolDefinitionId());
            return;
        }

        // 2. Idempotency check — filter out already-delivered subscriptions
        Set<UUID> alreadyDelivered = intelligenceDeliveryRepository
                .findByIntelligenceEventId(intelligenceEventId)
                .stream()
                .map(d -> d.getChannelSubscriptionId())
                .collect(Collectors.toSet());

        List<ChannelSubscription> pendingSubscriptions = subscriptions.stream()
                .filter(cs -> !alreadyDelivered.contains(cs.getId()))
                .toList();

        if (pendingSubscriptions.isEmpty()) {
            log.info("All subscriptions already delivered for eventId={}", intelligenceEventId);
            return;
        }

        // 3. Parallel fan-out delivery — one IntelligenceDelivery per adaptor
        CompletableFuture<?>[] futures = pendingSubscriptions.stream()
                .map(subscription -> CompletableFuture.runAsync(() -> {
                    try {
                        actionDispatcher.dispatch(event, subscription);
                    } catch (Exception e) {
                        log.error("Dispatch failed for eventId={}, subscriptionId={}",
                                intelligenceEventId, subscription.getId(), e);
                    }
                }))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();

        log.info("Fan-out complete: eventId={}, deliveries={}", intelligenceEventId, pendingSubscriptions.size());
    }
}
