package com.zemnitskiy.dpps.compute;

import com.zemnitskiy.dpps.config.IgniteConfig;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.io.Serial;

/** Reports how full the payments data region is on the node it runs on, as a fraction between 0 and 1. */
public final class DataRegionUsageJob implements IgniteCallable<Double> {

    @Serial
    private static final long serialVersionUID = 1L;

    @IgniteInstanceResource
    private transient Ignite ignite;

    @Override
    public Double call() {
        return localUsage(ignite);
    }

    /** Fill ratio of the payments data region on {@code ignite}'s local node. */
    public static double localUsage(Ignite ignite) {
        DataRegionMetrics metrics = ignite.dataRegionMetrics(IgniteConfig.DATA_REGION);
        DataRegionConfiguration region = ignite.configuration().getDataStorageConfiguration()
                .getDefaultDataRegionConfiguration();
        if (metrics == null || region.getMaxSize() <= 0) {
            return 0;
        }
        return (double) metrics.getTotalUsedSize() / region.getMaxSize();
    }
}
