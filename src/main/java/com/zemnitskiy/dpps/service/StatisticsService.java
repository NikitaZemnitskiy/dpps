package com.zemnitskiy.dpps.service;

import com.zemnitskiy.dpps.compute.PartialStats;
import com.zemnitskiy.dpps.compute.StatisticsTask;
import com.zemnitskiy.dpps.config.AppProperties;
import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.dto.StatisticsResponse.DateTimeStats;
import com.zemnitskiy.dpps.dto.StatisticsResponse.GeneralStats;
import com.zemnitskiy.dpps.dto.StatisticsResponse.GroupStats;
import com.zemnitskiy.dpps.dto.StatisticsResponse.ValueStats;
import com.zemnitskiy.dpps.exception.ClusterUnavailableException;
import com.zemnitskiy.dpps.exception.PaymentDataLostException;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.MetricCategory;
import com.zemnitskiy.dpps.model.TimeRange;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Computes payment statistics with a partition-aware MapReduce over the whole cluster (see {@link StatisticsTask})
 * and shapes the merged result into the requested metric sections.
 */
@Service
@Slf4j
public class StatisticsService {

    private final Ignite ignite;
    private final PartitionLossGuard partitionLossGuard;
    private final AppProperties.Statistics settings;

    public StatisticsService(Ignite ignite, PartitionLossGuard partitionLossGuard, AppProperties properties) {
        this.ignite = ignite;
        this.partitionLossGuard = partitionLossGuard;
        this.settings = properties.statistics();
    }

    /**
     * @param aggregation how payments are grouped
     * @param metrics     which metric sections to include (at least one)
     * @param range       only payments within this range are counted
     * @return one entry per group, sorted by group key
     * @throws PaymentDataLostException    if payments were lost and the loss hasn't been acknowledged yet
     * @throws ClusterUnavailableException if the cluster topology kept changing or the computation timed out
     */
    public StatisticsResponse calculateStatistics(AggregationType aggregation, Set<MetricCategory> metrics,
                                                  TimeRange range) {
        partitionLossGuard.ensureNoDataLoss();
        long started = System.currentTimeMillis();
        Map<String, PartialStats> groups = computeWithRetry(aggregation, range);

        Map<String, GroupStats> data = new LinkedHashMap<>();
        new TreeMap<>(groups).forEach((key, stats) -> data.put(key, toGroupStats(stats, metrics)));

        log.info("Statistics {} {} computed in {} ms: {} groups", aggregation, range,
                System.currentTimeMillis() - started, data.size());
        return new StatisticsResponse(data);
    }

    private Map<String, PartialStats> computeWithRetry(AggregationType aggregation, TimeRange range) {
        for (int attempt = 1; ; attempt++) {
            try {
                return ignite.compute(ignite.cluster().forServers())
                        // Jobs are pinned to partitions: failing over a single job is pointless, the task is retried instead.
                        .withNoFailover()
                        .withTimeout(settings.timeout().toMillis())
                        .execute(new StatisticsTask(aggregation, range), null);
            } catch (ComputeTaskTimeoutException e) {
                throw new ClusterUnavailableException(
                        "The statistics computation did not finish within " + settings.timeout().toSeconds() + " s", e);
            } catch (IgniteException e) {
                if (!StatisticsTask.isTopologyChange(e)) {
                    throw e;
                }
                partitionLossGuard.ensureNoDataLoss();
                if (attempt >= settings.maxAttempts()) {
                    throw new ClusterUnavailableException("The cluster topology kept changing while statistics were"
                            + " computed (" + attempt + " attempts); please retry in a moment", e);
                }
                log.warn("Cluster topology changed during the statistics computation (attempt {}/{}), retrying: {}",
                        attempt, settings.maxAttempts(), e.getMessage());
                pause();
            }
        }
    }

    private void pause() {
        try {
            Thread.sleep(settings.retryBackoff().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClusterUnavailableException("Interrupted while waiting to retry the statistics computation", e);
        }
    }

    private static GroupStats toGroupStats(PartialStats stats, Set<MetricCategory> metrics) {
        GeneralStats general = metrics.contains(MetricCategory.GENERAL)
                ? new GeneralStats(stats.getCount())
                : null;
        ValueStats value = metrics.contains(MetricCategory.VALUE)
                ? new ValueStats(stats.getMin(), stats.getMax(), stats.getSum(), stats.average())
                : null;
        DateTimeStats dateTime = metrics.contains(MetricCategory.DATETIME)
                ? new DateTimeStats(stats.getEarliest(), stats.getLatest(), stats.averageDateTime())
                : null;
        return new GroupStats(general, value, dateTime);
    }
}
