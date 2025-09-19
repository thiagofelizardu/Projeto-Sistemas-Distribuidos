package com.example.persister.entity;

import com.example.common.enuns.Status;
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
public class PaymentEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private UUID txId;
    private String merchantId;
    private String customerId;
    private long amount;
    private Status status;
    @JdbcTypeCode(SqlTypes.JSON)
    private String payload;
    private OffsetDateTime authorizedAt;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;

}