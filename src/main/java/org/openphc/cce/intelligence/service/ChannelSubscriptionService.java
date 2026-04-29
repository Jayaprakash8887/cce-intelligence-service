package org.openphc.cce.intelligence.service;

import lombok.RequiredArgsConstructor;
import org.openphc.cce.intelligence.domain.entity.ChannelSubscription;
import org.openphc.cce.intelligence.domain.entity.IntelligenceDelivery;
import org.openphc.cce.intelligence.domain.enums.IntelligenceDeliveryStatus;
import org.openphc.cce.intelligence.domain.repository.ChannelSubscriptionRepository;
import org.openphc.cce.intelligence.domain.repository.IntelligenceDeliveryRepository;
import org.openphc.cce.intelligence.domain.repository.ReceiverAdaptorRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ChannelSubscriptionService {

    private final ChannelSubscriptionRepository subscriptionRepository;
    private final ReceiverAdaptorRepository receiverAdaptorRepository;
    private final IntelligenceDeliveryRepository deliveryRepository;

    public List<ChannelSubscription> findAll() {
        return subscriptionRepository.findAll();
    }

    public List<ChannelSubscription> findFiltered(UUID protocolDefinitionId, String actionId,
                                                   String channel, UUID receiverAdaptorId, String status) {
        if (protocolDefinitionId == null && actionId == null && channel == null
                && receiverAdaptorId == null && status == null) {
            return subscriptionRepository.findAll();
        }
        return subscriptionRepository.findByFilters(protocolDefinitionId, actionId, channel, receiverAdaptorId, status);
    }

    public Optional<ChannelSubscription> findById(UUID id) {
        return subscriptionRepository.findById(id);
    }

    @Transactional
    public ChannelSubscription create(UUID protocolDefinitionId, String actionId, String channel, UUID receiverAdaptorId) {
        if (!receiverAdaptorRepository.existsById(receiverAdaptorId)) {
            throw new IllegalArgumentException("Receiver adaptor not found: " + receiverAdaptorId);
        }

        ChannelSubscription subscription = ChannelSubscription.builder()
                .protocolDefinitionId(protocolDefinitionId)
                .actionId(actionId)
                .channel(channel)
                .receiverAdaptorId(receiverAdaptorId)
                .status("ACTIVE")
                .build();

        return subscriptionRepository.save(subscription);
    }

    @Transactional
    public ChannelSubscription updateStatus(UUID id, String status) {
        ChannelSubscription subscription = subscriptionRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Channel subscription not found: " + id));
        subscription.setStatus(status);
        return subscriptionRepository.save(subscription);
    }

    @Transactional
    public void delete(UUID id) {
        ChannelSubscription subscription = subscriptionRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Channel subscription not found: " + id));

        boolean hasActiveDeliveries = deliveryRepository.existsByChannelSubscriptionIdAndStatusIn(
                id, List.of(IntelligenceDeliveryStatus.PENDING, IntelligenceDeliveryStatus.EXECUTING));

        if (hasActiveDeliveries) {
            throw new IllegalStateException("Cannot delete subscription with active (PENDING/EXECUTING) intelligence deliveries");
        }

        subscriptionRepository.delete(subscription);
    }
}
