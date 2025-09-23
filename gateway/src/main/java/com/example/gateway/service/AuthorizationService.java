package com.example.gateway.service;

import com.example.common.enuns.RejectCode;
import com.example.common.enuns.Status;
import com.example.common.event.PaymentAuthorizedEvent;
import com.example.common.event.PaymentEvent;
import com.example.common.event.PaymentRejectedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Service
public class AuthorizationService {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.persist-in}")
    private String persistTopic;

    @Value("${app.kafka.topics.rejected}")
    private String rejectedTopic;

    public AuthorizationService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${app.kafka.topics.authorize-in}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void processPaymentAuthorization(@Payload PaymentEvent event, Acknowledgment ack) {
        String key = event.txId().toString();
        log.info("Processing authorization txId={} amount={}", event.txId(), event.amount());

        try {
            if (event.amount() <= 0) {
                PaymentRejectedEvent rejectedEvent = new PaymentRejectedEvent(event.txId(), event.merchantId(), event.customerId(), event.amount(),
                        Status.REJECTED,
                        RejectCode.INVALID_REQUEST,
                        "Amount must be greater than 0",
                        event.traceId(),
                        Instant.now()
                );

                ProducerRecord<String, Object> rec =
                        new ProducerRecord<>(rejectedTopic, key, rejectedEvent);
                addHeaders(rec, "paymentRejectedEvent", event);
                kafkaTemplate.send(rec).get(); // aguarda para só então commitar
                log.info("txId={} REJECTED invalid amount", event.txId());
            } else {
                PaymentAuthorizedEvent authorizedEvent = new PaymentAuthorizedEvent(
                        event.txId(),
                        event.merchantId(),
                        event.customerId(),
                        event.amount(),
                        Status.AUTHORIZED,
                        event.traceId(),
                        Instant.now()
                );

                ProducerRecord<String, Object> rec =
                        new ProducerRecord<>(persistTopic, key, authorizedEvent);
                addHeaders(rec, "paymentAuthorizedEvent", event);
                kafkaTemplate.send(rec).get();
                log.info("txId={} AUTHORIZED", event.txId());
            }

            // só confirma o offset depois do publish bem sucedido
            ack.acknowledge();

        } catch (Exception e) {
            // deixa o DefaultErrorHandler aplicar retry
            log.error("Error processing txId={}", event.txId(), e);
            throw new RuntimeException(e);
        }
    }

    private void addHeaders(ProducerRecord<String, Object> rec, String typeAlias, PaymentEvent in) {
        if (in.traceId() != null) {
            rec.headers().add(new RecordHeader("traceId",
                    in.traceId().toString().getBytes(StandardCharsets.UTF_8)));
        }
        rec.headers().add(new RecordHeader("txId",
                in.txId().toString().getBytes(StandardCharsets.UTF_8)));

        rec.headers().add(new RecordHeader("type",
                typeAlias.getBytes(StandardCharsets.UTF_8)));
    }
}
