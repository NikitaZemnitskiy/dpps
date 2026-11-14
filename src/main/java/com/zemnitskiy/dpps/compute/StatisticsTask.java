package com.zemnitskiy.dpps.compute;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.model.TimeRange;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobFailoverException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.io.Serial;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Partition-aware MapReduce over the payments cache.
 * <ul>
 *   <li><b>map</b>: takes one consistent snapshot of which node is primary for which partition and sends every
 *   node a single job that lists exactly its partitions. Every partition is scanned exactly once, so backup
 *   copies are never counted.</li>
 *   <li><b>job</b>: scans those partitions with local, partition-scoped scan queries. Ignite reserves a partition
 *   while it is scanned and refuses one the node doesn't own (any more). A topology change can therefore never
 *   make data silently disappear or count twice - the job fails instead.</li>
 *   <li><b>reduce</b>: merges the per-group partial results of all jobs.</li>
 * </ul>
 * When a node leaves or a partition moves mid-way, the task fails with {@link TopologyChangedException} and
 * {@link com.zemnitskiy.dpps.service.StatisticsService} reruns it on a fresh snapshot.
 */
public class StatisticsTask extends ComputeTaskAdapter<Void, Map<String, PartialStats>> {

    /** Separator between sender and receiver in BY_CONNECTION keys; not allowed in bank names. */
    public static final String CONNECTION_ARROW = "->";

    @Serial
    private static final long serialVersionUID = 1L;

    private final AggregationType aggregation;
    private final TimeRange range;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public StatisticsTask(AggregationType aggregation, TimeRange range) {
        this.aggregation = Objects.requireNonNull(aggregation, "aggregation");
        this.range = Objects.requireNonNull(range, "range");
    }

    @Override
    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Void arg) {
        Affinity<String> affinity = ignite.affinity(IgniteConfig.PAYMENTS_CACHE);
        List<Integer> allPartitions = IntStream.range(0, affinity.partitions()).boxed().toList();
        // A single call, so all partitions are resolved against the same topology version.
        Map<Integer, ClusterNode> primaries = affinity.mapPartitionsToNodes(allPartitions);

        Map<ClusterNode, List<Integer>> partitionsByNode = new HashMap<>();
        for (Integer partition : allPartitions) {
            ClusterNode primary = primaries.get(partition);
            if (primary == null || !subgrid.contains(primary)) {
                throw new TopologyChangedException("Partition " + partition + " has no available primary node", null);
            }
            partitionsByNode.computeIfAbsent(primary, node -> new ArrayList<>()).add(partition);
        }

        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();
        partitionsByNode.forEach((node, partitions) -> jobs.put(
                new PartitionScanJob(partitions.stream().mapToInt(Integer::intValue).toArray(), aggregation, range),
                node));
        return jobs;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult result, List<ComputeJobResult> received) {
        IgniteException failure = result.getException();
        if (failure == null) {
            return ComputeJobResultPolicy.WAIT;
        }
        if (isTopologyChange(failure)) {
            // Jobs are pinned to partitions, so running this one elsewhere wouldn't help: rerun the whole task.
            throw new TopologyChangedException("Node " + result.getNode().id() + " could not scan its partitions", failure);
        }
        throw failure;
    }

    @Override
    public Map<String, PartialStats> reduce(List<ComputeJobResult> results) {
        Map<String, PartialStats> merged = new HashMap<>();
        for (ComputeJobResult result : results) {
            Map<String, PartialStats> groups = result.getData();
            groups.forEach((key, stats) -> merged.merge(key, stats, PartialStats::merge));
        }
        return merged;
    }

    /** Whether {@code error} (or any of its causes) means that the cluster topology changed mid-way. */
    public static boolean isTopologyChange(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof TopologyChangedException
                    || t instanceof ClusterTopologyException
                    || t instanceof ComputeJobFailoverException
                    || t instanceof ComputeExecutionRejectedException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds one payment to its group(s) if it falls into {@code range}.
     * <ul>
     *   <li>BY_DATE - key {@code yyyy-MM-dd}</li>
     *   <li>BY_BANK - every payment has two sides: {@code -value} for the sender, {@code +value} for the
     *   receiver, so a bank's {@code sum} is its net position</li>
     *   <li>BY_CONNECTION - key {@code sender -> receiver}; the direction matters</li>
     * </ul>
     */
    public static void accumulate(AggregationType aggregation, TimeRange range, Payment payment,
                                  Map<String, PartialStats> groups) {
        LocalDateTime dateTime = payment.getDateTime();
        if (!range.contains(dateTime)) {
            return;
        }
        BigDecimal value = payment.getValue();
        switch (aggregation) {
            case BY_DATE -> group(groups, dateTime.toLocalDate().toString()).add(value, dateTime);
            case BY_BANK -> {
                group(groups, payment.getSender()).add(value.negate(), dateTime);
                group(groups, payment.getReceiver()).add(value, dateTime);
            }
            case BY_CONNECTION -> group(groups, connectionKey(payment.getSender(), payment.getReceiver())).add(value, dateTime);
        }
    }

    /**
     * Group key of a connection, e.g. {@code Bank A -> Bank B}. Unambiguous because bank names may not contain
     * {@value #CONNECTION_ARROW} (enforced when payments are uploaded).
     */
    public static String connectionKey(String sender, String receiver) {
        return sender + " " + CONNECTION_ARROW + " " + receiver;
    }

    private static PartialStats group(Map<String, PartialStats> groups, String key) {
        return groups.computeIfAbsent(key, k -> new PartialStats());
    }

    /** Scans the given partitions on the node it is sent to and aggregates them. */
    public static final class PartitionScanJob extends ComputeJobAdapter {

        @Serial
        private static final long serialVersionUID = 1L;

        private final int[] partitions;
        private final AggregationType aggregation;
        private final TimeRange range;

        @IgniteInstanceResource
        private transient Ignite ignite;

        public PartitionScanJob(int[] partitions, AggregationType aggregation, TimeRange range) {
            this.partitions = partitions;
            this.aggregation = aggregation;
            this.range = range;
        }

        /** The partitions this job scans. */
        public int[] partitions() {
            return partitions.clone();
        }

        @Override
        public Map<String, PartialStats> execute() {
            IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
            Map<String, PartialStats> groups = new HashMap<>();
            for (int partition : partitions) {
                if (isCancelled()) {
                    // Typically the node is shutting down: let the task run again on the remaining nodes.
                    throw new ComputeJobFailoverException("Statistics job on node " + ignite.name() + " was cancelled");
                }
                scan(cache, partition, groups);
            }
            return groups;
        }

        private void scan(IgniteCache<String, Payment> cache, int partition, Map<String, PartialStats> groups) {
            ScanQuery<String, Payment> query = new ScanQuery<String, Payment>().setPartition(partition).setLocal(true);
            try (QueryCursor<Cache.Entry<String, Payment>> cursor = cache.query(query)) {
                for (Cache.Entry<String, Payment> entry : cursor) {
                    accumulate(aggregation, range, entry.getValue(), groups);
                }
            } catch (CacheException | IgniteException e) {
                // Ignite refuses local scans of partitions this node doesn't own - the partition has moved.
                throw new ComputeJobFailoverException(
                        "Partition " + partition + " is not available on node " + ignite.name() + ": " + e.getMessage(), e);
            }
        }
    }
}
