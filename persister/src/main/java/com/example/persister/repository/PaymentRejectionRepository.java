package com.example.persister.repository;

import com.example.persister.entity.PaymentRejectionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRejectionRepository extends JpaRepository<PaymentRejectionEntity, Long> {}