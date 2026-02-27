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
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class StatisticsService {

    private final Ignite ignite;

    public StatisticsResponse calculateStatistics(AggregationType aggregation,
                                                   Set<MetricCategory> metrics,
                                                   String from, String to) {
        Collection<Map<String, PartialStats>> nodeResults =
                ignite.compute().broadcast(new StatisticsCallable(aggregation, from, to));

        Map<String, PartialStats> merged = new HashMap<>();
        for (Map<String, PartialStats> nodeResult : nodeResults) {
            nodeResult.forEach((key, stats) ->
                    merged.merge(key, stats, PartialStats::merge));
        }

        StatisticsResponse response = new StatisticsResponse();
        Map<String, GroupStats> data = new LinkedHashMap<>();

        merged.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    GroupStats group = buildGroupStats(entry.getValue(), metrics);
                    data.put(entry.getKey(), group);
                });

        response.setData(data);
        return response;
    }

    private GroupStats buildGroupStats(PartialStats stats, Set<MetricCategory> metrics) {
        GroupStats group = new GroupStats();

        if (metrics.contains(MetricCategory.GENERAL)) {
            GeneralStats general = new GeneralStats();
            general.setCount(stats.getCount());
            group.setGeneral(general);
        }

        if (metrics.contains(MetricCategory.VALUE)) {
            ValueStats value = new ValueStats();
            value.setMin(roundTo4(stats.getMinValue()));
            value.setMax(roundTo4(stats.getMaxValue()));
            value.setSum(roundTo4(stats.getSumValue()));
            value.setAverage(roundTo4(stats.getAverageValue()));
            group.setValue(value);
        }

        if (metrics.contains(MetricCategory.DATETIME)) {
            DateTimeStats dateTime = new DateTimeStats();
            dateTime.setMin(stats.getMinDateTime());
            dateTime.setMax(stats.getMaxDateTime());

            long avgEpoch = stats.getAverageEpochSeconds();
            LocalDateTime avgDt = LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(avgEpoch), ZoneOffset.UTC);
            dateTime.setAverage(avgDt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            group.setDateTime(dateTime);
        }

        return group;
    }

    private double roundTo4(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }
}
