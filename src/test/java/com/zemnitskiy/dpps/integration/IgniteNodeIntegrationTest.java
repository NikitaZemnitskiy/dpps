package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Embedded Ignite Node")
@Tag("integration")
class IgniteNodeIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("Node is started and cluster is ACTIVE")
    void nodeShouldBeActiveAndRunning() {
        assertThat(ignite).isNotNull();
        assertThat(ignite.cluster().state()).isEqualTo(ClusterState.ACTIVE);
    }

    @Test
    @DisplayName("Payments cache is PARTITIONED with 1 backup and ATOMIC mode")
    void paymentsCacheShouldBeConfiguredCorrectly() {
        IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
        assertThat(cache).isNotNull();

        CacheConfiguration<?, ?> cfg = cache.getConfiguration(CacheConfiguration.class);
        assertThat(cfg.getCacheMode()).isEqualTo(CacheMode.PARTITIONED);
        assertThat(cfg.getBackups()).isEqualTo(1);
        assertThat(cfg.getAtomicityMode()).isEqualTo(CacheAtomicityMode.ATOMIC);
    }
}
