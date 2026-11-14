package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.compute.DataRegionUsageJob;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.support.TestData;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("Embedded Ignite node")
@Tag("integration")
class IgniteNodeIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("is started and the cluster is ACTIVE")
    void active() {
        assertThat(ignite.cluster().state()).isEqualTo(ClusterState.ACTIVE);
    }

    @Test
    @DisplayName("payments cache: PARTITIONED, 1 backup, ATOMIC, acknowledged only after the backup is written")
    void cacheConfiguration() {
        CacheConfiguration<?, ?> cfg = ignite.cache(IgniteConfig.PAYMENTS_CACHE).getConfiguration(CacheConfiguration.class);

        assertThat(cfg.getCacheMode()).isEqualTo(CacheMode.PARTITIONED);
        assertThat(cfg.getBackups()).isEqualTo(1);
        assertThat(cfg.getAtomicityMode()).isEqualTo(CacheAtomicityMode.ATOMIC);
        assertThat(cfg.getWriteSynchronizationMode()).isEqualTo(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    @Test
    @DisplayName("discovery uses a static address list (never multicast)")
    void staticDiscovery() {
        TcpDiscoverySpi discovery = (TcpDiscoverySpi) ignite.configuration().getDiscoverySpi();

        assertThat(discovery.getIpFinder()).isExactlyInstanceOf(TcpDiscoveryVmIpFinder.class);
    }

    @Test
    @DisplayName("the data region is bounded and reports its usage")
    void dataRegion() throws Exception {
        DataRegionConfiguration region = ignite.configuration().getDataStorageConfiguration()
                .getDefaultDataRegionConfiguration();
        assertThat(region.getName()).isEqualTo(IgniteConfig.DATA_REGION);
        assertThat(region.getMaxSize()).isEqualTo(256L * 1024 * 1024);
        assertThat(region.isMetricsEnabled()).isTrue();

        double before = DataRegionUsageJob.localUsage(ignite);
        upload(TestData.generatedCsv(5_000, "mem-"));
        double after = DataRegionUsageJob.localUsage(ignite);

        assertThat(before).isBetween(0.0, 1.0);
        assertThat(after).isGreaterThan(before).isLessThan(1.0);
    }

    @Test
    @DisplayName("payments survive a round trip through Ignite's binary format unchanged")
    void binaryRoundTrip() throws Exception {
        upload(TestData.TEST_CSV);

        BinaryObject binary = cache().<String, BinaryObject>withKeepBinary().get("3");
        Payment fromBinary = binary.deserialize();

        assertThat(binary.type().typeName()).isEqualTo(Payment.class.getName());
        assertThat(fromBinary).isEqualTo(TestData.payment(
                "3", "2026-02-20T20:00", "Bank A", "Bank C", "200"));
    }

    @Test
    @DisplayName("the health endpoint is public and reports UP")
    void health() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.components.ignite.details.clusterState").value("ACTIVE"));
    }
}
