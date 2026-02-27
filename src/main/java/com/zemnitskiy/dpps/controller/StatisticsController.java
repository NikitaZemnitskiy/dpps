package com.zemnitskiy.dpps.controller;

import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.MetricCategory;
import com.zemnitskiy.dpps.service.StatisticsService;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

@RestController
@RequestMapping("/api/statistics")
@RequiredArgsConstructor
@Validated
public class StatisticsController {

    private final StatisticsService statisticsService;

    @GetMapping
    public ResponseEntity<StatisticsResponse> getStatistics(
            @RequestParam @NotNull AggregationType aggregation,
            @RequestParam @NotEmpty Set<MetricCategory> metrics,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to) {
        StatisticsResponse response = statisticsService.calculateStatistics(aggregation, metrics, from, to);
        return ResponseEntity.ok(response);
    }
}
