package com.example.payment.service;

import com.example.common.event.PaymentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentProducerService {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducerService.class);

    private final String topic;
    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public PaymentProducerService(
            @Value("${app.kafka.topic}") String topic,
            KafkaTemplate<String, PaymentEvent> kafkaTemplate
    ) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPaymentEvent(PaymentEvent event) {
        String key = event.txId().toString();
        kafkaTemplate.send(topic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Payment event sent: txId={}, key={}, partition={}, offset={}",
                                event.txId(), key,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to send payment event: txId={}, key={}", event.txId(), key, ex);
                    }
                });
    }
}
