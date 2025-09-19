package com.example.persister.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentRejectionEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private UUID txId;
    private String merchantId;
    private String customerId;
    private long amount;
    private String code;
    private String detail;
    @JdbcTypeCode(SqlTypes.JSON)
    private String payload;
    private OffsetDateTime createdAt;

}