package com.example.common.event;

import com.example.common.enuns.RejectCode;
import com.example.common.enuns.Status;

import java.time.Instant;
import java.util.UUID;


public record PaymentRejectedEvent(
    UUID txId,
    String merchantId,
    String customerId,
    long amount,
    Status status,
    RejectCode code,
    String detail,
    UUID traceId,
    Instant rejectedAt
) {}