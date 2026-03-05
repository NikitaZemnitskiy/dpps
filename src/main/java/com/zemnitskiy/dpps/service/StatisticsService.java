package com.zemnitskiy.dpps.service;

import com.zemnitskiy.dpps.compute.PartialStats;
import com.zemnitskiy.dpps.compute.StatisticsCallable;
import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.dto.StatisticsResponse.*;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.MetricCategory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Orchestrates MapReduce-based statistics computation.
 * Broadcasts a {@link StatisticsCallable} to every cluster node, then merges
 * partial results into a single response filtered by the requested metrics.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StatisticsService {

    private final Ignite ignite;

    /**
     * Computes aggregated payment statistics across the entire Ignite cluster.
     * Each node processes its local primary data; results are merged here.
     *
     * @param aggregation grouping strategy
     * @param metrics     which metric categories to include in the response
     * @param from        optional time range start (ISO 8601)
     * @param to          optional time range end (ISO 8601)
     * @return statistics grouped by the aggregation key
     */
    public StatisticsResponse calculateStatistics(AggregationType aggregation,
                                                   Set<MetricCategory> metrics,
                                                   String from, String to) {
        long start = System.currentTimeMillis();

        Collection<Map<String, PartialStats>> nodeResults =
                ignite.compute().broadcast(new StatisticsCallable(aggregation, from, to));

        Map<String, PartialStats> merged = new HashMap<>();
        for (Map<String, PartialStats> nodeResult : nodeResults) {
            nodeResult.forEach((key, stats) ->
                    merged.merge(key, stats, PartialStats::merge));
        }

        Map<String, GroupStats> data = new LinkedHashMap<>();
        merged.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> data.put(entry.getKey(), buildGroupStats(entry.getValue(), metrics)));

        log.info("Statistics {} computed in {}ms: {} groups from {} nodes",
                aggregation, System.currentTimeMillis() - start, data.size(), nodeResults.size());

        return new StatisticsResponse(data);
    }

    private GroupStats buildGroupStats(PartialStats stats, Set<MetricCategory> metrics) {
        GeneralStats general = metrics.contains(MetricCategory.GENERAL)
                ? new GeneralStats(stats.getCount())
                : null;

        ValueStats value = metrics.contains(MetricCategory.VALUE)
                ? new ValueStats(
                        roundTo4(stats.getMinValue()),
                        roundTo4(stats.getMaxValue()),
                        roundTo4(stats.getSumValue()),
                        roundTo4(stats.getAverageValue()))
                : null;

        DateTimeStats dateTime = metrics.contains(MetricCategory.DATETIME)
                ? buildDateTimeStats(stats)
                : null;

        return new GroupStats(general, value, dateTime);
    }

    private DateTimeStats buildDateTimeStats(PartialStats stats) {
        long avgEpoch = stats.getAverageEpochSeconds();
        LocalDateTime avgDt = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(avgEpoch), ZoneOffset.UTC);
        String avgFormatted = avgDt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        return new DateTimeStats(stats.getMinDateTime(), stats.getMaxDateTime(), avgFormatted);
    }

    private double roundTo4(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }
}
