package com.example.gateway.service;

import com.example.common.enuns.Status;
import com.example.common.event.PaymentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class AuthorizationService {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.persist-in.name}")
    private String persistTopic;


    public AuthorizationService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.authorize-in.name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "paymentEventKafkaListenerContainerFactory"
    )
    public void processPaymentAuthorization(@Payload PaymentEvent event, Acknowledgment ack) {
        try {
            PaymentEvent authorized = new PaymentEvent(
                    event.txId(),
                    event.createdAt(),
                    event.merchantId(),
                    event.customerId(),
                    event.terminalId(),
                    event.amount(),
                    event.currency(),
                    event.method(),
                    event.entryMode(),
                    event.cardHash(),
                    Status.AUTHORIZED
            );

            kafkaTemplate.send(persistTopic, event.txId().toString(), authorized)
                    .thenAccept(result -> {
                        var m = result.getRecordMetadata();
                        log.info("Publicado em {} partition={} offset={} txId={}",
                                m.topic(), m.partition(), m.offset(), event.txId());
                        ack.acknowledge();
                    })
                    .exceptionally(ex -> {
                        log.error("Falha ao publicar em {} txId={}", persistTopic, event.txId(), ex);
                        // Sem ack -> reprocesso
                        return null;
                    });

        } catch (Exception e) {
            log.error("Erro ao autorizar txId={}", event.txId(), e);
            // Sem ack -> reprocesso
        }
    }
}
