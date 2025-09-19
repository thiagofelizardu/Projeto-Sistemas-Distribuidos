package com.example.common.event;

import com.example.common.enuns.Status;

import java.time.Instant;
import java.util.UUID;

public record PaymentEvent(
        UUID txId,
        Instant createdAt,
        String merchantId,
        String customerId,
        String terminalId,
        long amount,
        String currency,
        String method,
        String entryMode,
        String cardHash,
        Status status,
        UUID traceId
) {}
