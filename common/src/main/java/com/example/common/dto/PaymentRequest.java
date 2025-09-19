package com.example.common.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record PaymentRequest(
        @NotBlank String merchantId,
        @NotBlank String customerId,
        @NotBlank String terminalId,
        @Positive long amount,
        @NotBlank String currency,
        @NotBlank String method,
        String entryMode,
        @NotBlank String cardHash
) {}
