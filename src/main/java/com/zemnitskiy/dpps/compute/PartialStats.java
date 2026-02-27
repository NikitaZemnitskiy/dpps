package com.zemnitskiy.dpps.compute;

import lombok.Data;

import java.io.Serializable;

@Data
public class PartialStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private long count;
    private double minValue = Double.MAX_VALUE;
    private double maxValue = -Double.MAX_VALUE;
    private double sumValue;
    private String minDateTime;
    private String maxDateTime;
    private long dateTimeSumEpochSeconds;

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

    public PartialStats merge(PartialStats other) {
        if (other == null || other.count == 0) return this;
        if (this.count == 0) {
            this.count = other.count;
            this.minValue = other.minValue;
            this.maxValue = other.maxValue;
            this.sumValue = other.sumValue;
            this.minDateTime = other.minDateTime;
            this.maxDateTime = other.maxDateTime;
            this.dateTimeSumEpochSeconds = other.dateTimeSumEpochSeconds;
            return this;
        }

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
