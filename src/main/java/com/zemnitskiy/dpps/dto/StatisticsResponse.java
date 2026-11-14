package com.zemnitskiy.dpps.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Response of the statistics endpoint: one entry per group (a date, a bank or a connection), sorted by key.
 * Only the requested metric sections are present.
 */
public record StatisticsResponse(Map<String, GroupStats> data) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record GroupStats(GeneralStats general, ValueStats value, DateTimeStats dateTime) {
    }

    /** @param count number of payments (for BY_BANK: payments the bank sent or received) */
    public record GeneralStats(long count) {
    }

    /** Exact decimal statistics; {@code average} is rounded half-up to 4 decimal places. */
    public record ValueStats(BigDecimal min, BigDecimal max, BigDecimal sum, BigDecimal average) {
    }

    /** {@code average} is the mean timestamp, to the second. */
    public record DateTimeStats(LocalDateTime min, LocalDateTime max, LocalDateTime average) {
    }
}
