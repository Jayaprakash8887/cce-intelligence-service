package org.openphc.cce.intelligence.engine;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.intelligence.domain.entity.ChannelSubscription;
import org.openphc.cce.intelligence.domain.repository.ChannelSubscriptionRepository;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class SubscriptionRouter {

    private final ChannelSubscriptionRepository channelSubscriptionRepository;

    /**
     * Resolves active channel subscriptions with step-level routing.
     * Step-specific subscriptions (action_id matches) take precedence over wildcard (action_id IS NULL).
     * Deduplicates: if a step-specific row exists for an adaptor, the wildcard row for the same adaptor is skipped.
     */
    public List<ChannelSubscription> resolveSubscriptions(UUID protocolDefinitionId, String actionId, String channel) {
        List<ChannelSubscription> candidates = channelSubscriptionRepository
                .findRoutableSubscriptions(protocolDefinitionId, actionId, channel);

        if (candidates.isEmpty()) {
            log.warn("No active subscriptions found: protocolDefinitionId={}, actionId={}, channel={}",
                    protocolDefinitionId, actionId, channel);
            return Collections.emptyList();
        }

        // Deduplicate: step-specific overrides wildcard for same adaptor
        Set<UUID> stepSpecificAdaptors = candidates.stream()
                .filter(cs -> cs.getActionId() != null)
                .map(ChannelSubscription::getReceiverAdaptorId)
                .collect(Collectors.toSet());

        return candidates.stream()
                .filter(cs -> cs.getActionId() != null || !stepSpecificAdaptors.contains(cs.getReceiverAdaptorId()))
                .toList();
    }
}
