package com.zemnitskiy.dpps.config;

import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class IgniteConfig {

    public static final String PAYMENTS_CACHE = "payments";

    @Value("${ignite.instance.name}")
    private String instanceName;

    @Value("${ignite.discovery.addresses}")
    private String discoveryAddresses;

    @Value("${ignite.client-mode}")
    private boolean clientMode;

    @Bean
    public IgniteConfiguration igniteConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName(instanceName);
        cfg.setClientMode(clientMode);

        cfg.setFailureDetectionTimeout(30000);
        cfg.setClientFailureDetectionTimeout(30000);

        CacheConfiguration<String, Payment> cacheCfg = new CacheConfiguration<>(PAYMENTS_CACHE);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList(discoveryAddresses.split(",")));
        spi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    @Bean(destroyMethod = "close")
    public Ignite ignite(IgniteConfiguration igniteConfiguration) {
        return Ignition.start(igniteConfiguration);
    }
}
