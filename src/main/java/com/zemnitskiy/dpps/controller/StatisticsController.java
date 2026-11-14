package com.zemnitskiy.dpps.controller;

import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.exception.InvalidRequestException;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.MetricCategory;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.service.StatisticsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Payment statistics computed with MapReduce across the Ignite cluster. */
@RestController
@RequestMapping("/api/statistics")
@RequiredArgsConstructor
@Slf4j
public class StatisticsController {

    private final StatisticsService statisticsService;

    /**
     * @param aggregation grouping: BY_DATE, BY_BANK or BY_CONNECTION
     * @param metrics     sections to include: GENERAL, VALUE, DATETIME (repeat the parameter or separate by commas)
     * @param from        optional inclusive lower bound
     * @param to          optional inclusive upper bound
     */
    @GetMapping
    public StatisticsResponse getStatistics(@RequestParam AggregationType aggregation,
                                            @RequestParam Set<MetricCategory> metrics,
                                            @RequestParam(required = false) LocalDateTime from,
                                            @RequestParam(required = false) LocalDateTime to) {
        Set<MetricCategory> requested = metrics.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(MetricCategory.class)));
        if (requested.isEmpty()) {
            throw new InvalidRequestException("At least one metric is required: GENERAL, VALUE or DATETIME");
        }
        TimeRange range = TimeRange.of(from, to);
        log.debug("Statistics requested: aggregation={} metrics={} range={}", aggregation, requested, range);
        return statisticsService.calculateStatistics(aggregation, requested, range);
    }
}
