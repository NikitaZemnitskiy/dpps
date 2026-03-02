package com.zemnitskiy.dpps.compute;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/**
 * Accumulator for statistics computed on a single Ignite node.
 * Supports {@link #accumulate} for adding individual payments and
 * {@link #merge} for combining results from different nodes.
 */
@Data
public class PartialStats implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private long count;
    private double minValue = Double.MAX_VALUE;
    private double maxValue = -Double.MAX_VALUE;
    private double sumValue;
    private String minDateTime;
    private String maxDateTime;
    private long dateTimeSumEpochSeconds;

    /** Adds a single payment's data to this accumulator. */
    public void accumulate(double value, String dateTime, long epochSeconds) {
        count++;
        sumValue += value;
        if (value < minValue) minValue = value;
        if (value > maxValue) maxValue = value;

        if (minDateTime == null || dateTime.compareTo(minDateTime) < 0) {
            minDateTime = dateTime;
        }
        if (maxDateTime == null || dateTime.compareTo(maxDateTime) > 0) {
            maxDateTime = dateTime;
        }
        dateTimeSumEpochSeconds += epochSeconds;
    }

    /** Merges another node's partial result into this one. Used during the reduce phase. */
    public PartialStats merge(PartialStats other) {
        if (other == null || other.count == 0) return this;
        if (this.count == 0) return other;

        this.count += other.count;
        this.sumValue += other.sumValue;
        if (other.minValue < this.minValue) this.minValue = other.minValue;
        if (other.maxValue > this.maxValue) this.maxValue = other.maxValue;

        if (other.minDateTime != null &&
                (this.minDateTime == null || other.minDateTime.compareTo(this.minDateTime) < 0)) {
            this.minDateTime = other.minDateTime;
        }
        if (other.maxDateTime != null &&
                (this.maxDateTime == null || other.maxDateTime.compareTo(this.maxDateTime) > 0)) {
            this.maxDateTime = other.maxDateTime;
        }
        this.dateTimeSumEpochSeconds += other.dateTimeSumEpochSeconds;

        return this;
    }

    public double getAverageValue() {
        return count > 0 ? sumValue / count : 0;
    }

    public long getAverageEpochSeconds() {
        return count > 0 ? dateTimeSumEpochSeconds / count : 0;
    }
}
