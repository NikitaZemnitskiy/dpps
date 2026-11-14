package com.zemnitskiy.dpps.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

/**
 * Settings of the embedded Ignite server node ({@code ignite.*} in application.yml).
 *
 * @param instanceName            unique node name within the JVM
 * @param workDirectory           Ignite work directory (marshaller and binary metadata); must be writable
 * @param failureDetectionTimeout how long a silent node is tolerated before it is dropped from the cluster
 * @param localHost               optional address to bind discovery/communication to; blank = all interfaces
 * @param discovery               static discovery settings
 * @param communication           node-to-node communication settings
 * @param cache                   payments cache settings
 * @param dataRegion              off-heap memory settings
 */
@Validated
@ConfigurationProperties(prefix = "ignite")
public record IgniteProperties(
        @NotBlank String instanceName,
        @NotBlank String workDirectory,
        @NotNull Duration failureDetectionTimeout,
        String localHost,
        @Valid @NotNull Discovery discovery,
        @Valid @NotNull Communication communication,
        @Valid @NotNull Cache cache,
        @Valid @NotNull DataRegion dataRegion) {

    /** Same settings for another node in this JVM (used by tests that start extra cluster members). */
    public IgniteProperties withInstanceName(String name) {
        return new IgniteProperties(name, workDirectory, failureDetectionTimeout, localHost,
                discovery, communication, cache, dataRegion);
    }

    /**
     * @param addresses      cluster members, e.g. {@code app-node-1:47500..47509}
     * @param localPort      first discovery port to bind
     * @param localPortRange how many ports after {@code localPort} may be tried
     */
    public record Discovery(@NotEmpty List<String> addresses,
                            @Min(1) @Max(65535) int localPort,
                            @Min(0) int localPortRange) {
    }

    /** @param localPort first port for node-to-node communication */
    public record Communication(@Min(1) @Max(65535) int localPort) {
    }

    /** @param backups number of backup copies of every payment */
    public record Cache(@Min(0) int backups) {
    }

    /**
     * @param initialSize off-heap memory allocated at start
     * @param maxSize     hard limit of off-heap memory for payments on this node
     */
    public record DataRegion(@NotNull DataSize initialSize, @NotNull DataSize maxSize) {
    }
}
