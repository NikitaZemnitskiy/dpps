package com.zemnitskiy.dpps.filter;

import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.lang.IgniteBiPredicate;

import java.io.Serial;

/**
 * Predicate for ScanQuery that filters payments by datetime range.
 * Serialized and sent to each Ignite node for server-side filtering.
 */
public class PaymentTimeRangeFilter implements IgniteBiPredicate<String, Payment> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String from;
    private final String to;

    public PaymentTimeRangeFilter(String from, String to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public boolean apply(String key, Payment payment) {
        String dateTime = payment.getDateTime();
        if (dateTime == null) return false;
        return dateTime.compareTo(from) >= 0 && dateTime.compareTo(to) <= 0;
    }
}
