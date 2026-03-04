package com.zemnitskiy.dpps.compute;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Executes on each cluster node. Scans only LOCAL PRIMARY entries
 * to avoid double-counting records that have backups on other nodes.
 */
public class StatisticsCallable implements IgniteCallable<Map<String, PartialStats>>, Serializable, Binarylizable {

    @Serial
    private static final long serialVersionUID = 2L;

    private static final transient Logger log = LoggerFactory.getLogger(StatisticsCallable.class);

    @IgniteInstanceResource
    private transient Ignite ignite;

    private AggregationType aggregation;
    private LocalDateTime from;
    private LocalDateTime to;

    public StatisticsCallable() {}

    public StatisticsCallable(AggregationType aggregation, LocalDateTime from, LocalDateTime to) {
        this.aggregation = aggregation;
        this.from = from;
        this.to = to;
    }

    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeString("aggregation", aggregation != null ? aggregation.name() : null);
        writer.writeString("from", from != null ? from.toString() : null);
        writer.writeString("to", to != null ? to.toString() : null);
    }

    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        String agg = reader.readString("aggregation");
        aggregation = agg != null ? AggregationType.valueOf(agg) : null;
        String f = reader.readString("from");
        from = f != null ? LocalDateTime.parse(f) : null;
        String t = reader.readString("to");
        to = t != null ? LocalDateTime.parse(t) : null;
    }

    @Override
    public Map<String, PartialStats> call() {
        IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
        Map<String, PartialStats> localResult = new HashMap<>();

        for (var entry : cache.localEntries(CachePeekMode.PRIMARY)) {
            Payment p = entry.getValue();

            if (!matchesTimeRange(p)) continue;

            long epochSeconds = p.getDateTime().toEpochSecond(ZoneOffset.UTC);

            switch (aggregation) {
                case BY_DATE -> accumulateByDate(localResult, p, epochSeconds);
                case BY_BANK -> accumulateByBank(localResult, p, epochSeconds);
                case BY_CONNECTION -> accumulateByConnection(localResult, p, epochSeconds);
            }
        }

        log.debug("[{}] Computed {} aggregation: {} groups", ignite.name(), aggregation, localResult.size());
        return localResult;
    }

    private void accumulateByDate(Map<String, PartialStats> result, Payment p, long epochSeconds) {
        String dateKey = p.getDateTime().toLocalDate().toString();
        result.computeIfAbsent(dateKey, k -> new PartialStats())
                .accumulate(p.getValue(), p.getDateTime(), epochSeconds);
    }

    private void accumulateByBank(Map<String, PartialStats> result, Payment p, long epochSeconds) {
        result.computeIfAbsent(p.getSender(), k -> new PartialStats())
                .accumulate(-p.getValue(), p.getDateTime(), epochSeconds);
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
        LocalDateTime dateTime = p.getDateTime();
        if (dateTime == null) return false;
        if (from != null && dateTime.isBefore(from)) return false;
        return to == null || !dateTime.isAfter(to);
    }
}
