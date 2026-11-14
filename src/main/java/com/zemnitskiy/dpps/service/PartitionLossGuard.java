package com.zemnitskiy.dpps.service;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.exception.PaymentDataLostException;
import com.zemnitskiy.dpps.model.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

/**
 * Detects payments that were lost because every copy of a partition left the cluster at once.
 * <p>
 * The payments cache uses {@code PartitionLossPolicy.READ_WRITE_SAFE}: instead of silently treating a lost
 * partition as empty (and returning statistics that are quietly wrong), Ignite refuses to read or write it.
 * The API checks for that up front and explains how to recover; deleting all payments acknowledges the loss.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PartitionLossGuard {

    private final Ignite ignite;

    /** @throws PaymentDataLostException if some payments partitions have lost all their copies */
    public void ensureNoDataLoss() {
        IgniteCache<String, Payment> cache = cache();
        Collection<Integer> lost = cache.lostPartitions();
        if (!lost.isEmpty()) {
            log.error("{} partitions of the payments cache have lost all copies; refusing to serve incomplete data",
                    lost.size());
            throw new PaymentDataLostException(lost.size(), ignite.affinity(IgniteConfig.PAYMENTS_CACHE).partitions());
        }
    }

    /** Number of partitions that have lost all their copies. */
    public int lostPartitions() {
        return cache().lostPartitions().size();
    }

    /** Accepts the loss: lost partitions become usable (and empty) again. Returns how many were reset. */
    public int acknowledgeDataLoss() {
        int lost = lostPartitions();
        if (lost > 0) {
            ignite.resetLostPartitions(List.of(IgniteConfig.PAYMENTS_CACHE));
            log.warn("Reset {} lost partitions of the payments cache; their payments are gone", lost);
        }
        return lost;
    }

    private IgniteCache<String, Payment> cache() {
        return ignite.cache(IgniteConfig.PAYMENTS_CACHE);
    }
}
