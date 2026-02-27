package com.zemnitskiy.dpps.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.Map;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StatisticsResponse {

    private Map<String, GroupStats> data;

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class GroupStats {
        private GeneralStats general;
        private ValueStats value;
        private DateTimeStats dateTime;
    }

    @Data
    public static class GeneralStats {
        private long count;
    }

    @Data
    public static class ValueStats {
        private double min;
        private double max;
        private double sum;
        private double average;
    }

    @Data
    public static class DateTimeStats {
        private String min;
        private String max;
        private String average;
    }
}
