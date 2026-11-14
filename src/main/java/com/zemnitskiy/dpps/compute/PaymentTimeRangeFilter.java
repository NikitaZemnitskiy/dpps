package com.zemnitskiy.dpps.compute;

import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.model.TimeRange;
import org.apache.ignite.lang.IgniteBiPredicate;

import java.io.Serial;
import java.util.Objects;

/**
 * ScanQuery predicate that keeps payments inside a {@link TimeRange} (bounds included).
 * Serialized and evaluated on the data nodes, so only matching entries cross the network.
 */
public final class PaymentTimeRangeFilter implements IgniteBiPredicate<String, Payment> {

    @Serial
    private static final long serialVersionUID = 2L;

    private final TimeRange range;

    public PaymentTimeRangeFilter(TimeRange range) {
        this.range = Objects.requireNonNull(range, "range");
    }

    @Override
    public boolean apply(String id, Payment payment) {
        return range.contains(payment.getDateTime());
    }
}
