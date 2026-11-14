package com.zemnitskiy.dpps.compute;

import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.lang.IgniteClosure;

import javax.cache.Cache;
import java.io.Serial;

/** ScanQuery transformer that returns only the key, so deleting by range doesn't ship whole payments around. */
public final class PaymentIdTransformer implements IgniteClosure<Cache.Entry<String, Payment>, String> {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String apply(Cache.Entry<String, Payment> entry) {
        return entry.getKey();
    }
}
