package com.zemnitskiy.dpps.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Application settings ({@code dpps.*} in application.yml).
 *
 * @param upload     CSV upload settings
 * @param statistics statistics computation settings
 */
@Validated
@ConfigurationProperties(prefix = "dpps")
public record AppProperties(@Valid @NotNull Upload upload, @Valid @NotNull Statistics statistics) {

    /**
     * @param batchSize           payments written to the cluster per {@code putAll}
     * @param memoryHighWatermark fill ratio of the fullest node's data region at which uploads are refused
     * @param memoryCheckInterval how long a cluster memory reading is reused before it is refreshed
     */
    public record Upload(@Min(1) int batchSize,
                         @DecimalMin("0.05") @DecimalMax("1.0") double memoryHighWatermark,
                         @NotNull Duration memoryCheckInterval) {
    }

    /**
     * @param timeout      upper bound for one statistics computation
     * @param maxAttempts  attempts when the cluster topology changes during a computation
     * @param retryBackoff pause between attempts
     */
    public record Statistics(@NotNull Duration timeout, @Min(1) int maxAttempts, @NotNull Duration retryBackoff) {
    }
}
