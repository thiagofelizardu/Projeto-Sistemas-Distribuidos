package com.example.payment.controller;

import com.example.common.dto.PaymentAcceptedResponse;
import com.example.common.dto.PaymentRequest;
import com.example.payment.service.PaymentService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
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
    public ResponseEntity<PaymentAcceptedResponse> createPayment(@Valid @RequestBody PaymentRequest request) {

        UUID txId = paymentService.processPayment(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new PaymentAcceptedResponse(txId));

    }
}
