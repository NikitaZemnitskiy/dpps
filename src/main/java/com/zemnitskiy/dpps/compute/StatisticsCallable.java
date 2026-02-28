package com.zemnitskiy.dpps.compute;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Executes on each cluster node. Scans only LOCAL PRIMARY entries
 * to avoid double-counting records that have backups on other nodes.
 */
public class StatisticsCallable implements IgniteCallable<Map<String, PartialStats>>, Serializable {

    private static final long serialVersionUID = 1L;

    @IgniteInstanceResource
    private transient Ignite ignite;

    private final AggregationType aggregation;
    private final String from;
    private final String to;

    public StatisticsCallable(AggregationType aggregation, String from, String to) {
        this.aggregation = aggregation;
        this.from = from;
        this.to = to;
    }

    @Override
    public Map<String, PartialStats> call() {
        IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
        Map<String, PartialStats> localResult = new HashMap<>();

        for (Cache.Entry<String, Payment> entry : cache.localEntries(CachePeekMode.PRIMARY)) {
            Payment p = entry.getValue();

            if (!matchesTimeRange(p)) continue;

            long epochSeconds = toEpochSeconds(p.getDateTime());

            switch (aggregation) {
                case BY_DATE -> accumulateByDate(localResult, p, epochSeconds);
                case BY_BANK -> accumulateByBank(localResult, p, epochSeconds);
                case BY_CONNECTION -> accumulateByConnection(localResult, p, epochSeconds);
            }
        }

        return localResult;
    }

    private void accumulateByDate(Map<String, PartialStats> result, Payment p, long epochSeconds) {
        String dateKey = extractDate(p.getDateTime());
        result.computeIfAbsent(dateKey, k -> new PartialStats())
                .accumulate(p.getValue(), p.getDateTime(), epochSeconds);
    }

    private void accumulateByBank(Map<String, PartialStats> result, Payment p, long epochSeconds) {
        // Sender loses value (negative)
        result.computeIfAbsent(p.getSender(), k -> new PartialStats())
                .accumulate(-p.getValue(), p.getDateTime(), epochSeconds);
        // Receiver gains value (positive)
        result.computeIfAbsent(p.getReceiver(), k -> new PartialStats())
                .accumulate(p.getValue(), p.getDateTime(), epochSeconds);
    }

    private void accumulateByConnection(Map<String, PartialStats> result, Payment p, long epochSeconds) {
        String connectionKey = p.getSender() + "-" + p.getReceiver();
        result.computeIfAbsent(connectionKey, k -> new PartialStats())
                .accumulate(p.getValue(), p.getDateTime(), epochSeconds);
    }

    private boolean matchesTimeRange(Payment p) {
        if (from == null && to == null) return true;
        String dateTime = p.getDateTime();
        if (dateTime == null) return false;
        if (from != null && dateTime.compareTo(from) < 0) return false;
        if (to != null && dateTime.compareTo(to) > 0) return false;
        return true;
    }

    private String extractDate(String dateTime) {
        // ISO 8601: "2026-02-20T12:00" -> "2026-02-20"
        int tIndex = dateTime.indexOf('T');
        return tIndex > 0 ? dateTime.substring(0, tIndex) : dateTime;
    }

    private long toEpochSeconds(String dateTime) {
        return LocalDateTime.parse(dateTime).toEpochSecond(ZoneOffset.UTC);
    }
}
