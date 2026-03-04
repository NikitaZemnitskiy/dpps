package com.zemnitskiy.dpps.compute;

import lombok.Data;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Accumulator for statistics computed on a single Ignite node.
 * Supports {@link #accumulate} for adding individual payments and
 * {@link #merge} for combining results from different nodes.
 */
@Data
public class PartialStats implements Serializable, Binarylizable {

    @Serial
    private static final long serialVersionUID = 2L;

    private long count;
    private double minValue = Double.MAX_VALUE;
    private double maxValue = -Double.MAX_VALUE;
    private double sumValue;
    private LocalDateTime minDateTime;
    private LocalDateTime maxDateTime;
    private long dateTimeSumEpochSeconds;

    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeLong("count", count);
        writer.writeDouble("minValue", minValue);
        writer.writeDouble("maxValue", maxValue);
        writer.writeDouble("sumValue", sumValue);
        writer.writeString("minDateTime", minDateTime != null ? minDateTime.toString() : null);
        writer.writeString("maxDateTime", maxDateTime != null ? maxDateTime.toString() : null);
        writer.writeLong("dateTimeSumEpochSeconds", dateTimeSumEpochSeconds);
    }

    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        count = reader.readLong("count");
        minValue = reader.readDouble("minValue");
        maxValue = reader.readDouble("maxValue");
        sumValue = reader.readDouble("sumValue");
        String minDt = reader.readString("minDateTime");
        minDateTime = minDt != null ? LocalDateTime.parse(minDt) : null;
        String maxDt = reader.readString("maxDateTime");
        maxDateTime = maxDt != null ? LocalDateTime.parse(maxDt) : null;
        dateTimeSumEpochSeconds = reader.readLong("dateTimeSumEpochSeconds");
    }

    /** Adds a single payment's data to this accumulator. */
    public void accumulate(double value, LocalDateTime dateTime, long epochSeconds) {
        count++;
        sumValue += value;
        if (value < minValue) minValue = value;
        if (value > maxValue) maxValue = value;

        if (minDateTime == null || dateTime.isBefore(minDateTime)) {
            minDateTime = dateTime;
        }
        if (maxDateTime == null || dateTime.isAfter(maxDateTime)) {
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
                (this.minDateTime == null || other.minDateTime.isBefore(this.minDateTime))) {
            this.minDateTime = other.minDateTime;
        }
        if (other.maxDateTime != null &&
                (this.maxDateTime == null || other.maxDateTime.isAfter(this.maxDateTime))) {
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
