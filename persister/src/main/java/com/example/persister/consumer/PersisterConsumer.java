package com.example.persister.consumer;

import com.example.common.enuns.Status;
import com.example.common.event.PaymentRejectedEvent;
import com.example.common.event.PaymentAuthorizedEvent;
import com.example.persister.entity.PaymentEntity;
import com.example.persister.entity.PaymentRejectionEntity;
import com.example.persister.repository.PaymentRejectionRepository;
import com.example.persister.repository.PaymentRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Transactional
@Component
@RequiredArgsConstructor
public class PersisterConsumer {

    private static final Logger log = LoggerFactory.getLogger(PersisterConsumer.class);
    private final PaymentRepository paymentRepository;
    private final PaymentRejectionRepository rejectionRepository;

    private final ObjectMapper objectMapper;


    @KafkaListener(topics = "${app.kafka.topics.persist-in}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAuthorized(PaymentAuthorizedEvent event) {
        log.info("Persisting authorized payment with txId={}", event.txId());
        if (paymentRepository.existsByTxId(event.txId())) {
            log.warn("Duplicate authorized event for txId={}. Ignoring.", event.txId());
            return;
        }
        PaymentEntity entity = new PaymentEntity();
        entity.setTxId(event.txId());
        entity.setMerchantId(event.merchantId());
        entity.setCustomerId(event.customerId());
        entity.setAmount(event.amount());
        entity.setStatus(Status.valueOf(event.status().name()));
        OffsetDateTime authorizedTimestamp = event.authorizedAt().atOffset(ZoneOffset.UTC);
        entity.setCreatedAt(authorizedTimestamp);

        try {
            entity.setPayload(objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize payload for txId={}", event.txId(), e);
        }

        paymentRepository.save(entity);
    }

    @KafkaListener(topics = "${app.kafka.topics.rejected}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeRejected(PaymentRejectedEvent event) {
        log.info("Persisting rejected payment with txId={}", event.txId());
        PaymentRejectionEntity entity = new PaymentRejectionEntity();
        entity.setTxId(event.txId());
        entity.setMerchantId(event.merchantId());
        entity.setCustomerId(event.customerId());
        entity.setAmount(event.amount());
        entity.setCode(event.code().name());
        entity.setDetail(event.detail());
        entity.setCreatedAt(event.rejectedAt().atOffset(ZoneOffset.UTC));

        try {
            entity.setPayload(objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize payload for rejected txId={}", event.txId(), e);
        }

        rejectionRepository.save(entity);
    }
}