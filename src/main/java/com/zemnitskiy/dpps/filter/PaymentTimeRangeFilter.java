package com.zemnitskiy.dpps.filter;

import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.lang.IgniteBiPredicate;

import java.io.Serial;
import java.time.LocalDateTime;

/**
 * Predicate for ScanQuery that filters payments by datetime range.
 * Serialized and sent to each Ignite node for server-side filtering.
 */
public class PaymentTimeRangeFilter implements IgniteBiPredicate<String, Payment>, Binarylizable {

    @Serial
    private static final long serialVersionUID = 2L;

    private LocalDateTime from;
    private LocalDateTime to;

    public PaymentTimeRangeFilter() {}

    public PaymentTimeRangeFilter(LocalDateTime from, LocalDateTime to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeString("from", from != null ? from.toString() : null);
        writer.writeString("to", to != null ? to.toString() : null);
    }

    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        String f = reader.readString("from");
        from = f != null ? LocalDateTime.parse(f) : null;
        String t = reader.readString("to");
        to = t != null ? LocalDateTime.parse(t) : null;
    }

    @Override
    public boolean apply(String key, Payment payment) {
        LocalDateTime dateTime = payment.getDateTime();
        if (dateTime == null) return false;
        return !dateTime.isBefore(from) && !dateTime.isAfter(to);
    }
}
