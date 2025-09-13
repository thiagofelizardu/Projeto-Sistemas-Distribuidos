package com.example.payment.controller;

import com.example.payment.dto.PaymentAcceptedResponse;
import com.example.payment.dto.PaymentRequest;
import com.example.payment.service.PaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


import java.util.UUID;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<PaymentAcceptedResponse> createPayment(@RequestBody PaymentRequest request) {
        UUID txId = paymentService.processPayment(request);
        return ResponseEntity.accepted().body(new PaymentAcceptedResponse(txId));
    }
}
