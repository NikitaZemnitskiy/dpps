package com.zemnitskiy.dpps.health;

import com.zemnitskiy.dpps.compute.DataRegionUsageJob;
import com.zemnitskiy.dpps.service.PartitionLossGuard;
import lombok.RequiredArgsConstructor;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports the node as healthy only while its Ignite cluster is active, so load balancers and orchestrators stop
 * routing to a node that has dropped out of the grid. Details are shown to authenticated readers only.
 */
@Component
@RequiredArgsConstructor
public class IgniteHealthIndicator implements HealthIndicator {

    private final Ignite ignite;
    private final PartitionLossGuard partitionLossGuard;

    @Override
    public Health health() {
        ClusterState state = ignite.cluster().state();
        Health.Builder health = state == ClusterState.ACTIVE ? Health.up() : Health.down();
        return health
                .withDetail("node", ignite.name())
                .withDetail("clusterState", state)
                .withDetail("serverNodes", ignite.cluster().forServers().nodes().size())
                .withDetail("dataRegionUsage", Math.round(DataRegionUsageJob.localUsage(ignite) * 100) + "%")
                // Stays UP on purpose: the node must remain reachable so the loss can be acknowledged.
                .withDetail("lostPartitions", partitionLossGuard.lostPartitions())
                .build();
    }
}
