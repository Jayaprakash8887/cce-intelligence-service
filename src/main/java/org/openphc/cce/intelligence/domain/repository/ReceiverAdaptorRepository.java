package org.openphc.cce.intelligence.domain.repository;

import org.openphc.cce.intelligence.domain.entity.ReceiverAdaptor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ReceiverAdaptorRepository extends JpaRepository<ReceiverAdaptor, UUID> {

    List<ReceiverAdaptor> findByStatus(String status);

    Optional<ReceiverAdaptor> findByName(String name);
}
