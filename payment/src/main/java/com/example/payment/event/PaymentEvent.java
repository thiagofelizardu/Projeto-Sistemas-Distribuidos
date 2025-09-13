package com.example.payment.event;

import java.util.UUID;

public record PaymentEvent(
        UUID txId,
        String traceId,
        String merchantId,
        String customerId,
        String terminalId,
        long amount,
        String currency,
        String method,
        String entryMode,
        String cardHash,
        long timestamp
) {}
