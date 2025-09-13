package com.example.payment.service;

import com.example.payment.event.KafkaTopics;
import com.example.payment.event.PaymentEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, PaymentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPaymentEvent(PaymentEvent event) {
        kafkaTemplate.send(
                KafkaTopics.PAYMENTS_AUTHORIZE_IN, //topic
                event.merchantId(), // id da msg
                event // conteudo da msg
        );
    }

    // quando colocar mais topicos tem que adiconar os metodos aq

}
