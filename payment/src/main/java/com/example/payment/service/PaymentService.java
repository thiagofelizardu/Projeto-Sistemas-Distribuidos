package com.example.payment.service;

import com.example.payment.dto.PaymentRequest;
import com.example.payment.event.PaymentEvent;
import org.springframework.stereotype.Service;

import java.util.UUID;


// Recebe a msg em json
@Service
public class PaymentService {

    private final KafkaProducerService kafkaProducerService;

    public PaymentService(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    public UUID processPayment(PaymentRequest request) {
        UUID txId = UUID.randomUUID();
        String traceId = UUID.randomUUID().toString();

        PaymentEvent event = new PaymentEvent(
                txId,
                traceId,
                request.merchantId(),
                request.customerId(),
                request.terminalId(),
                request.amount(),
                request.currency(),
                request.method(),
                request.entryMode(),
                request.cardHash(),
                System.currentTimeMillis()
        );

        kafkaProducerService.sendPaymentEvent(event);
        return txId;
    }

}
