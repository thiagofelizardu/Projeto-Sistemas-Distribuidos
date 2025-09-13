package com.example.payment.dto;

public record PaymentRequest(
        String merchantId,
        String customerId,
        String terminalId,
        long amount,
        String currency,
        String method,
        String entryMode,
        String cardHash
) {}
