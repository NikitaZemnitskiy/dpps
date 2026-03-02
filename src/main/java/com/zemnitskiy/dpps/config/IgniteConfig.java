package com.zemnitskiy.dpps.config;

import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Configures the embedded Apache Ignite node with a partitioned payment cache (1 backup). */
@Configuration
public class IgniteConfig {

    public static final String PAYMENTS_CACHE = "payments";

    @Value("${ignite.instance.name}")
    private String instanceName;

    @Bean
    public IgniteConfiguration igniteConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName(instanceName);

        cfg.setFailureDetectionTimeout(30000);

        CacheConfiguration<String, Payment> cacheCfg = new CacheConfiguration<>(PAYMENTS_CACHE);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    @Bean(destroyMethod = "close")
    public Ignite ignite(IgniteConfiguration igniteConfiguration) {
        return Ignition.start(igniteConfiguration);
    }
}
