package com.zemnitskiy.dpps.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

public record StatisticsResponse(Map<String, GroupStats> data) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record GroupStats(
            GeneralStats general,
            ValueStats value,
            DateTimeStats dateTime
    ) {
    }

    public record GeneralStats(long count) {
    }

    public record ValueStats(double min, double max, double sum, double average) {
    }

    public record DateTimeStats(String min, String max, String average) {
    }
}
