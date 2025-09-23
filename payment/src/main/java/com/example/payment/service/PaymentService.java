package com.example.payment.service;

import com.example.common.dto.PaymentRequest;
import com.example.common.enuns.Status;
import com.example.common.event.PaymentEvent;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentService {

    private final PaymentProducerService paymentProducerService;

    public PaymentService(PaymentProducerService paymentProducerService) {
        this.paymentProducerService = paymentProducerService;
    }

    @Async("taskExecutor")
    public void processPaymentAsync(UUID txId, PaymentRequest request) {
        PaymentEvent event = new PaymentEvent(
                txId,
                Instant.now(),
                request.merchantId(),
                request.customerId(),
                request.terminalId(),
                request.amount(),
                request.currency(),
                request.method(),
                request.entryMode(),
                request.cardHash(),
                Status.PENDING
        );
        paymentProducerService.sendPaymentEvent(event);
    }
}
