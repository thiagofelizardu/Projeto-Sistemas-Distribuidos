package com.example.persister.consumer;

import com.example.common.enuns.Status;
import com.example.common.event.PaymentEvent;
import com.example.persister.entity.PaymentEntity;
import com.example.persister.repository.PaymentRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Slf4j
@Transactional
@Component
@RequiredArgsConstructor
public class PersisterConsumer {

    private final PaymentRepository paymentRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.persist-in}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeAuthorized(final PaymentEvent event) {
        if (!isAuthorized(event)) {
            log.debug("Ignorando evento não autorizado txId={} status={}", event.txId(), event.status());
            return;
        }
        if (isDuplicate(event)) {
            log.warn("Evento duplicado (txId={}). Ignorando.", event.txId());
            return;
        }

        PaymentEntity entity = buildEntity(event);
        entity.setPayload(serialize(event));
        saveSafely(entity, event.txId().toString());
    }

    // ----- helpers -----
    private boolean isAuthorized(PaymentEvent e) {
        return e.status() == Status.AUTHORIZED;
    }

    private boolean isDuplicate(PaymentEvent e) {
        return paymentRepository.existsByTxId(e.txId());
    }

    private PaymentEntity buildEntity(PaymentEvent e) {
        var entity = new PaymentEntity();
        entity.setTxId(e.txId());
        entity.setMerchantId(e.merchantId());
        entity.setCustomerId(e.customerId());
        entity.setAmount(e.amount());
        entity.setStatus(Status.AUTHORIZED);
        // PaymentEvent tem createdAt; use como createdAt (ou authorizedAt lógico)
        entity.setCreatedAt(e.createdAt().atOffset(ZoneOffset.UTC));
        return entity;
    }

    private String serialize(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException ex) {
            log.error("Falha ao serializar payload", ex);
            return "{}";
        }
    }

    private void saveSafely(PaymentEntity entity, String txId) {
        try {
            paymentRepository.save(entity);
            log.info("Pagamento autorizado persistido (txId={})", txId);
        } catch (DataIntegrityViolationException dup) {
            log.warn("Conflito de unicidade ao persistir txId={} (duplicado concorrente). Ignorando.", txId);
        }
    }
}
