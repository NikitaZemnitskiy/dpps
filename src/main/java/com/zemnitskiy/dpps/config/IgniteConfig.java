package com.zemnitskiy.dpps.config;

import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Starts the embedded Ignite server node. Every application instance is a full cluster member that stores a
 * share of the partitioned {@value #PAYMENTS_CACHE} cache (1 backup by default).
 */
@Configuration
public class IgniteConfig {

    public static final String PAYMENTS_CACHE = "payments";
    public static final String DATA_REGION = "payments-region";

    private static final int MESSAGE_QUEUE_LIMIT = 1024;

    @Bean(destroyMethod = "close")
    public Ignite ignite(IgniteProperties properties) {
        // Spring owns the node's lifecycle. Without this, Ignite's own JVM shutdown hook would stop the node in
        // parallel with Spring's graceful shutdown and fail requests that are still being served.
        System.setProperty(IgniteSystemProperties.IGNITE_NO_SHUTDOWN_HOOK, "true");
        // Don't contact ignite.apache.org for version checks.
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");
        return Ignition.start(nodeConfiguration(properties));
    }

    /**
     * Builds a fresh node configuration (SPIs can't be shared between nodes, so every node needs its own).
     */
    public static IgniteConfiguration nodeConfiguration(IgniteProperties properties) {
        IgniteConfiguration cfg = new IgniteConfiguration()
                .setIgniteInstanceName(properties.instanceName())
                .setGridLogger(new Slf4jLogger())
                .setWorkDirectory(properties.workDirectory())
                .setFailureDetectionTimeout(properties.failureDetectionTimeout().toMillis())
                .setDiscoverySpi(discoverySpi(properties.discovery()))
                .setCommunicationSpi(communicationSpi(properties.communication()))
                .setDataStorageConfiguration(dataStorage(properties.dataRegion()))
                .setCacheConfiguration(paymentsCache(properties.cache()));
        if (StringUtils.hasText(properties.localHost())) {
            cfg.setLocalHost(properties.localHost());
        }
        return cfg;
    }

    private static TcpDiscoverySpi discoverySpi(IgniteProperties.Discovery discovery) {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(discovery.addresses());
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder);
        spi.setLocalPort(discovery.localPort());
        spi.setLocalPortRange(discovery.localPortRange());
        return spi;
    }

    private static TcpCommunicationSpi communicationSpi(IgniteProperties.Communication communication) {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();
        spi.setLocalPort(communication.localPort());
        // Bounded outbound queues: a slow node gets back-pressure instead of growing the heap until OOM.
        spi.setMessageQueueLimit(MESSAGE_QUEUE_LIMIT);
        return spi;
    }

    private static DataStorageConfiguration dataStorage(IgniteProperties.DataRegion dataRegion) {
        DataRegionConfiguration region = new DataRegionConfiguration()
                .setName(DATA_REGION)
                .setInitialSize(dataRegion.initialSize().toBytes())
                .setMaxSize(dataRegion.maxSize().toBytes())
                // Usage metrics feed the upload memory guard and the health endpoint.
                .setMetricsEnabled(true);
        return new DataStorageConfiguration().setDefaultDataRegionConfiguration(region);
    }

    private static CacheConfiguration<String, Payment> paymentsCache(IgniteProperties.Cache cache) {
        return new CacheConfiguration<String, Payment>(PAYMENTS_CACHE)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(cache.backups())
                // A write is acknowledged only once the backup copy is written too: an accepted upload survives
                // the loss of any single node, and backups are always consistent when they get promoted.
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                // If every copy of a partition is lost, refuse to use it rather than silently treating it as empty
                // (the default) - incomplete statistics must not look like correct ones. See PartitionLossGuard.
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);
    }
}
