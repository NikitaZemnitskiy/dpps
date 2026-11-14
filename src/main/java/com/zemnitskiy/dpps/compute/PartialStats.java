package com.zemnitskiy.dpps.compute;

import lombok.Getter;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Mergeable accumulator for one statistics group (a date, a bank or a connection).
 * <p>
 * Filled on the data nodes, sent back to the reducer and merged there - hence a plain serializable class.
 * All amounts are exact {@link BigDecimal}s; nothing is rounded until the final average.
 */
@Getter
public class PartialStats implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    /** Decimal places of the average amount. */
    public static final int AVERAGE_SCALE = 4;

    private long count;
    private BigDecimal sum = BigDecimal.ZERO;
    private BigDecimal min;
    private BigDecimal max;
    private LocalDateTime earliest;
    private LocalDateTime latest;
    /** Sum of the timestamps as epoch seconds (UTC is used only as an arithmetic reference, not as a zone). */
    private long epochSecondsSum;

    /** Adds one payment (or, for BY_BANK, one side of a payment) to the group. */
    public void add(BigDecimal value, LocalDateTime dateTime) {
        count++;
        sum = sum.add(value);
        min = min == null || value.compareTo(min) < 0 ? value : min;
        max = max == null || value.compareTo(max) > 0 ? value : max;
        earliest = earliest == null || dateTime.isBefore(earliest) ? dateTime : earliest;
        latest = latest == null || dateTime.isAfter(latest) ? dateTime : latest;
        epochSecondsSum = Math.addExact(epochSecondsSum, dateTime.toEpochSecond(ZoneOffset.UTC));
    }

    /** Merges another partial result of the same group into this one and returns {@code this}. */
    public PartialStats merge(PartialStats other) {
        if (other == null || other.count == 0) {
            return this;
        }
        if (count == 0) {
            return other;
        }
        count += other.count;
        sum = sum.add(other.sum);
        min = other.min.compareTo(min) < 0 ? other.min : min;
        max = other.max.compareTo(max) > 0 ? other.max : max;
        earliest = other.earliest.isBefore(earliest) ? other.earliest : earliest;
        latest = other.latest.isAfter(latest) ? other.latest : latest;
        epochSecondsSum = Math.addExact(epochSecondsSum, other.epochSecondsSum);
        return this;
    }

    /** Mean amount rounded half-up to {@value #AVERAGE_SCALE} decimals, without trailing zeros; null if empty. */
    public BigDecimal average() {
        if (count == 0) {
            return null;
        }
        BigDecimal average = sum.divide(BigDecimal.valueOf(count), AVERAGE_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
        return average.scale() < 0 ? average.setScale(0) : average;
    }

    /** Mean timestamp, to the second; null if empty. */
    public LocalDateTime averageDateTime() {
        if (count == 0) {
            return null;
        }
        return LocalDateTime.ofEpochSecond(Math.floorDiv(epochSecondsSum, count), 0, ZoneOffset.UTC);
    }
}
