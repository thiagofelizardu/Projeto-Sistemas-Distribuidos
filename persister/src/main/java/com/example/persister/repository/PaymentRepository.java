package com.example.persister.repository;

import com.example.persister.entity.PaymentEntity;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
public interface PaymentRepository extends JpaRepository<PaymentEntity, Long> {
    boolean existsByTxId(UUID txId);

}
