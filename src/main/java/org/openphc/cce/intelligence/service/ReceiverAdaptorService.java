package org.openphc.cce.intelligence.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.openphc.cce.intelligence.domain.entity.ReceiverAdaptor;
import org.openphc.cce.intelligence.domain.repository.ChannelSubscriptionRepository;
import org.openphc.cce.intelligence.domain.repository.ReceiverAdaptorRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ReceiverAdaptorService {

    private final ReceiverAdaptorRepository adaptorRepository;
    private final ChannelSubscriptionRepository subscriptionRepository;

    public List<ReceiverAdaptor> findAll() {
        return adaptorRepository.findAll();
    }

    public List<ReceiverAdaptor> findByStatus(String status) {
        return adaptorRepository.findByStatus(status);
    }

    public Optional<ReceiverAdaptor> findById(UUID id) {
        return adaptorRepository.findById(id);
    }

    @Transactional
    public ReceiverAdaptor create(String name, JsonNode definition, JsonNode config) {
        validateDefinition(definition);

        ReceiverAdaptor adaptor = ReceiverAdaptor.builder()
                .name(name)
                .definition(definition)
                .config(config)
                .status("ACTIVE")
                .build();

        return adaptorRepository.save(adaptor);
    }

    @Transactional
    public ReceiverAdaptor update(UUID id, String name, JsonNode definition, JsonNode config, String status) {
        ReceiverAdaptor adaptor = adaptorRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Receiver adaptor not found: " + id));

        if (name != null) adaptor.setName(name);
        if (definition != null) {
            validateDefinition(definition);
            adaptor.setDefinition(definition);
        }
        if (config != null) adaptor.setConfig(config);
        if (status != null) adaptor.setStatus(status);

        return adaptorRepository.save(adaptor);
    }

    @Transactional
    public void delete(UUID id) {
        ReceiverAdaptor adaptor = adaptorRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Receiver adaptor not found: " + id));

        boolean hasActiveSubscriptions = subscriptionRepository.existsByReceiverAdaptorIdAndStatus(id, "ACTIVE");
        if (hasActiveSubscriptions) {
            throw new IllegalStateException("Cannot delete adaptor with active channel subscriptions");
        }

        adaptorRepository.delete(adaptor);
    }

    private void validateDefinition(JsonNode definition) {
        if (definition == null || !definition.has("resourceType") || !"Endpoint".equals(definition.get("resourceType").asText())) {
            throw new IllegalArgumentException("definition.resourceType must be 'Endpoint'");
        }
        if (!definition.has("address") || definition.get("address").isNull() || definition.get("address").asText().isBlank()) {
            throw new IllegalArgumentException("definition.address is required");
        }
    }
}
