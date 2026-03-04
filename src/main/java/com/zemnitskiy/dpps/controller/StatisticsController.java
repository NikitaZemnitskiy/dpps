package com.zemnitskiy.dpps.controller;

import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.MetricCategory;
import com.zemnitskiy.dpps.service.StatisticsService;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Set;

/**
 * REST controller for computing payment statistics via MapReduce across the Ignite cluster.
 */
@RestController
@RequestMapping("/api/statistics")
@RequiredArgsConstructor
@Validated
@Slf4j
public class StatisticsController {

    private final StatisticsService statisticsService;

    /**
     * Computes aggregated statistics by broadcasting a compute task to all cluster nodes.
     *
     * @param aggregation grouping strategy (BY_DATE, BY_BANK, BY_CONNECTION)
     * @param metrics     metric categories to include (GENERAL, VALUE, DATETIME)
     * @param from        optional start of time range filter (ISO 8601)
     * @param to          optional end of time range filter (ISO 8601)
     */
    @GetMapping
    public ResponseEntity<StatisticsResponse> getStatistics(
            @RequestParam @NotNull AggregationType aggregation,
            @RequestParam @NotEmpty Set<MetricCategory> metrics,
            @RequestParam(required = false) LocalDateTime from,
            @RequestParam(required = false) LocalDateTime to) {
        log.info("GET /api/statistics aggregation={} metrics={} from={} to={}", aggregation, metrics, from, to);
        StatisticsResponse response = statisticsService.calculateStatistics(aggregation, metrics, from, to);
        return ResponseEntity.ok(response);
    }
}
