package com.zemnitskiy.dpps.model;

import com.zemnitskiy.dpps.exception.InvalidRequestException;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Inclusive date-time interval {@code [from, to]}; a {@code null} bound leaves that side open.
 * <p>
 * Shipped to cluster nodes inside scan filters and compute jobs, therefore a plain immutable class:
 * Ignite 2.x cannot deserialize Java records.
 */
public final class TimeRange implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final TimeRange UNBOUNDED = new TimeRange(null, null);

    private final LocalDateTime from;
    private final LocalDateTime to;

    private TimeRange(LocalDateTime from, LocalDateTime to) {
        this.from = from;
        this.to = to;
    }

    /** The range that contains every date-time. */
    public static TimeRange unbounded() {
        return UNBOUNDED;
    }

    /**
     * @param from inclusive lower bound, or {@code null} for none
     * @param to   inclusive upper bound, or {@code null} for none
     * @throws InvalidRequestException if both bounds are given and {@code from} is after {@code to}
     */
    public static TimeRange of(LocalDateTime from, LocalDateTime to) {
        if (from != null && to != null && from.isAfter(to)) {
            throw new InvalidRequestException("'from' (" + from + ") must not be after 'to' (" + to + ")");
        }
        return from == null && to == null ? UNBOUNDED : new TimeRange(from, to);
    }

    public LocalDateTime from() {
        return from;
    }

    public LocalDateTime to() {
        return to;
    }

    public boolean isUnbounded() {
        return from == null && to == null;
    }

    public boolean isBounded() {
        return from != null && to != null;
    }

    /** Length of a bounded range. */
    public Duration length() {
        if (!isBounded()) {
            throw new IllegalStateException("Range " + this + " is open-ended");
        }
        return Duration.between(from, to);
    }

    /** Whether {@code dateTime} lies within the range, bounds included. */
    public boolean contains(LocalDateTime dateTime) {
        if (dateTime == null) {
            return false;
        }
        return (from == null || !dateTime.isBefore(from)) && (to == null || !dateTime.isAfter(to));
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TimeRange other && Objects.equals(from, other.from) && Objects.equals(to, other.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "[" + (from == null ? "-inf" : from) + " .. " + (to == null ? "+inf" : to) + "]";
    }
}
