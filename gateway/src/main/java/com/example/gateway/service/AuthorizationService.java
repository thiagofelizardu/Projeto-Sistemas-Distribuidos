package com.example.gateway.service;

import com.example.common.enuns.RejectCode;
import com.example.common.enuns.Status;
import com.example.common.event.PaymentAuthorizedEvent;
import com.example.common.event.PaymentEvent;
import com.example.common.event.PaymentRejectedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class AuthorizationService {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String persistTopic;
    private final String rejectedTopic;
    private final String dlqTopic;

    public AuthorizationService(KafkaTemplate<String, Object> kafkaTemplate,
                                @Value("${app.kafka.topics.persist-in}") String persistTopic,
                                @Value("${app.kafka.topics.rejected}") String rejectedTopic,
                                @Value("${app.kafka.topics.dlq}") String dlqTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.persistTopic = persistTopic;
        this.rejectedTopic = rejectedTopic;
        this.dlqTopic = dlqTopic;
    }

    @KafkaListener(topics = "${app.kafka.topics.authorize-in}", groupId = "${spring.kafka.consumer.group-id}")
    public void processPaymentAuthorization(@Payload PaymentEvent event) {
        log.info("Processing authorization for txId={}", event.txId());

        if (event.amount() <= 0) {
            PaymentRejectedEvent rejectedEvent = new PaymentRejectedEvent(
                    event.txId(),
                    event.merchantId(),
                    event.customerId(),
                    event.amount(),
                    Status.REJECTED,
                    RejectCode.INVALID_REQUEST,
                    "Amount must be greater than 0",
                    event.traceId(),
                    Instant.now()
            );
            kafkaTemplate.send(rejectedTopic, rejectedEvent.merchantId(), rejectedEvent);
            log.info("txId={} REJECTED due to invalid amount.", event.txId());

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
            kafkaTemplate.send(persistTopic, authorizedEvent.merchantId(), authorizedEvent);
            log.info("txId={} AUTHORIZED.", event.txId());
        }
    }

}