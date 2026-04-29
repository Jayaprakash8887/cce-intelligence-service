package org.openphc.cce.intelligence.domain.repository;

import org.openphc.cce.intelligence.domain.entity.ChannelSubscription;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ChannelSubscriptionRepository extends JpaRepository<ChannelSubscription, UUID> {

    List<ChannelSubscription> findByProtocolDefinitionIdAndChannelAndStatus(
            UUID protocolDefinitionId, String channel, String status);

    /**
     * Step-level routing query: finds active subscriptions matching the protocol, channel,
     * and either the specific action_id or wildcard (NULL).
     * Results ordered with step-specific first (action_id NOT NULL), wildcard last.
     */
    @Query("""
        SELECT cs FROM ChannelSubscription cs
        JOIN FETCH cs.receiverAdaptor ra
        WHERE cs.protocolDefinitionId = :protocolDefinitionId
          AND cs.channel = :channel
          AND (cs.actionId = :actionId OR cs.actionId IS NULL)
          AND cs.status = 'ACTIVE'
          AND ra.status = 'ACTIVE'
        ORDER BY cs.actionId NULLS LAST
        """)
    List<ChannelSubscription> findRoutableSubscriptions(
            @Param("protocolDefinitionId") UUID protocolDefinitionId,
            @Param("actionId") String actionId,
            @Param("channel") String channel);

    List<ChannelSubscription> findByReceiverAdaptorId(UUID receiverAdaptorId);

    boolean existsByReceiverAdaptorIdAndStatus(UUID receiverAdaptorId, String status);

    long countByStatus(String status);

    @Query("""
        SELECT cs FROM ChannelSubscription cs
        WHERE (:protocolDefinitionId IS NULL OR cs.protocolDefinitionId = :protocolDefinitionId)
          AND (:actionId IS NULL OR cs.actionId = :actionId)
          AND (:channel IS NULL OR cs.channel = :channel)
          AND (:receiverAdaptorId IS NULL OR cs.receiverAdaptorId = :receiverAdaptorId)
          AND (:status IS NULL OR cs.status = :status)
        """)
    List<ChannelSubscription> findByFilters(
            @Param("protocolDefinitionId") UUID protocolDefinitionId,
            @Param("actionId") String actionId,
            @Param("channel") String channel,
            @Param("receiverAdaptorId") UUID receiverAdaptorId,
            @Param("status") String status);
}
