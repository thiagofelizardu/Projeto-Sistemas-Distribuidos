package com.example.common.event;

import com.example.common.enuns.Status;
import java.time.Instant;
import java.util.UUID;

public record PaymentAuthorizedEvent(
    UUID txId,
    String merchantId,
    String customerId,
    long amount,
    Status status,
    UUID traceId,
    Instant authorizedAt
) {}