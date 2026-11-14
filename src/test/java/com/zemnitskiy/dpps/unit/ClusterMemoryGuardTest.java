package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.compute.DataRegionUsageJob;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.service.ClusterMemoryGuard;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("ClusterMemoryGuard")
@Tag("unit")
class ClusterMemoryGuardTest {

    private final Ignite ignite = mock(Ignite.class);
    private final IgniteCompute compute = mock(IgniteCompute.class);

    @BeforeEach
    void setUp() {
        IgniteCluster cluster = mock(IgniteCluster.class);
        ClusterGroup servers = mock(ClusterGroup.class);
        when(ignite.cluster()).thenReturn(cluster);
        when(cluster.forServers()).thenReturn(servers);
        when(ignite.compute(servers)).thenReturn(compute);
    }

    @Test
    @DisplayName("judges the cluster by its fullest node")
    void fullestNode() {
        when(compute.broadcast(any(DataRegionUsageJob.class))).thenReturn(List.of(0.20, 0.90, 0.50));

        ClusterMemoryGuard.Status status = new ClusterMemoryGuard(ignite, 0.85, Duration.ZERO).status();

        assertThat(status.usage()).isEqualTo(0.90);
        assertThat(status.nearlyFull()).isTrue();
        assertThat(status.describe()).isEqualTo("the fullest node uses 90% of its data region (limit for uploads: 85%)");
    }

    @Test
    @DisplayName("is not full below the watermark")
    void belowWatermark() {
        when(compute.broadcast(any(DataRegionUsageJob.class))).thenReturn(List.of(0.84));

        assertThat(new ClusterMemoryGuard(ignite, 0.85, Duration.ZERO).status().nearlyFull()).isFalse();
    }

    @Test
    @DisplayName("reuses a reading within the check interval")
    void cachesReadings() {
        when(compute.broadcast(any(DataRegionUsageJob.class))).thenReturn(List.of(0.1));
        ClusterMemoryGuard guard = new ClusterMemoryGuard(ignite, 0.85, Duration.ofMinutes(1));

        guard.status();
        guard.status();
        guard.status();

        verify(compute, times(1)).broadcast(any(DataRegionUsageJob.class));
    }

    @Test
    @DisplayName("measures again once the interval has passed")
    void refreshesReadings() {
        when(compute.broadcast(any(DataRegionUsageJob.class))).thenReturn(List.of(0.1), List.of(0.95));
        ClusterMemoryGuard guard = new ClusterMemoryGuard(ignite, 0.85, Duration.ZERO);

        assertThat(guard.status().nearlyFull()).isFalse();
        assertThat(guard.status().nearlyFull()).isTrue();
    }

    @Test
    @DisplayName("falls back to the local node when not every node can be asked")
    void fallsBackToLocalNode() {
        when(compute.broadcast(any(DataRegionUsageJob.class))).thenThrow(new ClusterTopologyException("node left"));
        DataRegionMetrics metrics = mock(DataRegionMetrics.class);
        when(metrics.getTotalUsedSize()).thenReturn(90L);
        when(ignite.dataRegionMetrics(IgniteConfig.DATA_REGION)).thenReturn(metrics);
        when(ignite.configuration()).thenReturn(new IgniteConfiguration().setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setName(IgniteConfig.DATA_REGION).setMaxSize(100L))));

        ClusterMemoryGuard.Status status = new ClusterMemoryGuard(ignite, 0.85, Duration.ZERO).status();

        assertThat(status.usage()).isEqualTo(0.90);
        assertThat(status.nearlyFull()).isTrue();
    }
}
