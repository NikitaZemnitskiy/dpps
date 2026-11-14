package com.zemnitskiy.dpps.model;

import lombok.Value;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * A single payment, stored in the Ignite cache under its {@code id}.
 * <p>
 * Immutable. Amounts are exact decimals ({@link BigDecimal}) and timestamps are real date-times, so range
 * filters and statistics never depend on how the source file spelled them.
 */
@Value
public class Payment implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    String id;
    LocalDateTime dateTime;
    String sender;
    String receiver;
    BigDecimal value;
}
