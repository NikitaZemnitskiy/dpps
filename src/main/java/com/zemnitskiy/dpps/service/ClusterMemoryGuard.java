package com.zemnitskiy.dpps.service;

import com.zemnitskiy.dpps.compute.DataRegionUsageJob;
import com.zemnitskiy.dpps.config.AppProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;

/**
 * Keeps uploads from filling the cluster's off-heap memory.
 * <p>
 * An Ignite node whose data region overflows treats it as a critical failure and halts its JVM - and since
 * payments are spread evenly, one oversized upload would take down every node and lose all data. Uploads
 * therefore check the fill ratio of the fullest node before every batch and stop at a high-watermark.
 * Readings are cached for a short interval, so the check costs one small broadcast per second at most.
 */
@Component
@Slf4j
public class ClusterMemoryGuard {

    private final Ignite ignite;
    private final double highWatermark;
    private final long checkIntervalNanos;

    private volatile Reading lastReading;

    @Autowired
    public ClusterMemoryGuard(Ignite ignite, AppProperties properties) {
        this(ignite, properties.upload().memoryHighWatermark(), properties.upload().memoryCheckInterval());
    }

    public ClusterMemoryGuard(Ignite ignite, double highWatermark, Duration checkInterval) {
        this.ignite = ignite;
        this.highWatermark = highWatermark;
        this.checkIntervalNanos = checkInterval.toNanos();
    }

    /** Current memory state of the cluster; at most {@code checkInterval} old. */
    public Status status() {
        Reading reading = lastReading;
        long now = System.nanoTime();
        if (reading == null || now - reading.takenAtNanos() >= checkIntervalNanos) {
            reading = new Reading(measureFullestNode(), now);
            lastReading = reading;
        }
        return new Status(reading.usage(), highWatermark);
    }

    private double measureFullestNode() {
        try {
            Collection<Double> usages = ignite.compute(ignite.cluster().forServers()).broadcast(new DataRegionUsageJob());
            double max = usages.stream().mapToDouble(Double::doubleValue).max().orElse(0);
            log.debug("Fullest node uses {}% of its data region", Math.round(max * 100));
            return max;
        } catch (IgniteException e) {
            // E.g. a node left while it was asked. Payments are spread evenly, so the local node is a good proxy -
            // and an upload shouldn't fail just because the guard couldn't reach every node.
            log.warn("Could not read the data region usage of all nodes ({}); judging by the local node", e.getMessage());
            return DataRegionUsageJob.localUsage(ignite);
        }
    }

    /**
     * @param usage         fill ratio (0..1) of the fullest node's data region
     * @param highWatermark ratio at which uploads are refused
     */
    public record Status(double usage, double highWatermark) {

        public boolean nearlyFull() {
            return usage >= highWatermark;
        }

        public String describe() {
            return String.format("the fullest node uses %d%% of its data region (limit for uploads: %d%%)",
                    Math.round(usage * 100), Math.round(highWatermark * 100));
        }
    }

    private record Reading(double usage, long takenAtNanos) {
    }
}
