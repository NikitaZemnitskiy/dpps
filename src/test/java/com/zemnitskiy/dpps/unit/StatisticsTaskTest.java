package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.compute.PartialStats;
import com.zemnitskiy.dpps.compute.StatisticsTask.PartitionScanJob;
import com.zemnitskiy.dpps.compute.StatisticsTask;
import com.zemnitskiy.dpps.compute.TopologyChangedException;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.support.ListQueryCursor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobFailoverException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.zemnitskiy.dpps.support.TestData.at;
import static com.zemnitskiy.dpps.support.TestData.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** StatisticsTask: partition assignment, aggregation rules, failure classification and the reduce step. */
@DisplayName("StatisticsTask")
@Tag("unit")
class StatisticsTaskTest {

    private static final List<Payment> PAYMENTS = List.of(
            payment("p1", "2026-02-20T10:00", "BankA", "BankB", "100"),
            payment("p2", "2026-02-20T14:00", "BankA", "BankC", "200.50"),
            payment("p3", "2026-02-21T09:00", "BankB", "BankA", "150"),
            payment("p4", "2026-02-21T16:00", "BankC", "BankB", "300"));

    private static Map<String, PartialStats> aggregate(AggregationType aggregation, TimeRange range) {
        Map<String, PartialStats> groups = new HashMap<>();
        PAYMENTS.forEach(p -> StatisticsTask.accumulate(aggregation, range, p, groups));
        return groups;
    }

    @Nested
    @DisplayName("Aggregation rules")
    class Aggregation {

        @Test
        @DisplayName("BY_DATE groups by calendar day")
        void byDate() {
            Map<String, PartialStats> groups = aggregate(AggregationType.BY_DATE, TimeRange.unbounded());

            assertThat(groups).containsOnlyKeys("2026-02-20", "2026-02-21");
            assertThat(groups.get("2026-02-20").getCount()).isEqualTo(2);
            assertThat(groups.get("2026-02-20").getSum()).isEqualByComparingTo("300.50");
            assertThat(groups.get("2026-02-21").getSum()).isEqualByComparingTo("450");
        }

        @Test
        @DisplayName("BY_BANK books -value on the sender and +value on the receiver")
        void byBank() {
            Map<String, PartialStats> groups = aggregate(AggregationType.BY_BANK, TimeRange.unbounded());

            assertThat(groups).containsOnlyKeys("BankA", "BankB", "BankC");
            // BankA: -100 - 200.50 + 150; BankB: +100 - 150 + 300; BankC: +200.50 - 300
            assertThat(groups.get("BankA").getSum()).isEqualByComparingTo("-150.50");
            assertThat(groups.get("BankB").getSum()).isEqualByComparingTo("250");
            assertThat(groups.get("BankC").getSum()).isEqualByComparingTo("-99.50");
            assertThat(groups.get("BankA").getCount()).isEqualTo(3);
            assertThat(groups.get("BankA").getMin()).isEqualByComparingTo("-200.50");
            assertThat(groups.get("BankA").getMax()).isEqualByComparingTo("150");
        }

        @Test
        @DisplayName("BY_CONNECTION groups by direction-sensitive sender-receiver pair")
        void byConnection() {
            Map<String, PartialStats> groups = aggregate(AggregationType.BY_CONNECTION, TimeRange.unbounded());

            assertThat(groups).containsOnlyKeys("BankA-BankB", "BankA-BankC", "BankB-BankA", "BankC-BankB");
            assertThat(groups.get("BankA-BankB").getSum()).isEqualByComparingTo("100");
            assertThat(groups.get("BankB-BankA").getSum()).isEqualByComparingTo("150");
        }

        @Test
        @DisplayName("only payments inside the range are counted, bounds included")
        void range() {
            Map<String, PartialStats> groups = aggregate(AggregationType.BY_DATE,
                    TimeRange.of(at("2026-02-20T14:00"), at("2026-02-21T09:00")));

            assertThat(groups.get("2026-02-20").getCount()).isEqualTo(1);
            assertThat(groups.get("2026-02-21").getCount()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("map")
    class Map_ {

        @Test
        @DisplayName("sends each node one job, and every partition is assigned exactly once")
        @SuppressWarnings("unchecked")
        void everyPartitionExactlyOnce() {
            List<ClusterNode> nodes = List.of(node(), node(), node());
            int partitions = 64;
            Map<Integer, ClusterNode> primaries = new HashMap<>();
            IntStream.range(0, partitions).forEach(p -> primaries.put(p, nodes.get(p % 3)));
            StatisticsTask task = taskWithAffinity(partitions, primaries);

            Map<? extends ComputeJob, ClusterNode> jobs = task.map(nodes, null);

            assertThat(jobs).hasSize(3);
            assertThat(jobs.values()).containsExactlyInAnyOrderElementsOf(nodes);
            List<Integer> assigned = new ArrayList<>();
            jobs.forEach((job, node) -> Arrays.stream(((PartitionScanJob) job).partitions()).forEach(p -> {
                assertThat(primaries.get(p)).as("partition %d goes to its primary", p).isSameAs(node);
                assigned.add(p);
            }));
            assertThat(assigned).containsExactlyInAnyOrderElementsOf(IntStream.range(0, partitions).boxed().toList());
        }

        @Test
        @DisplayName("fails as a topology change when a primary is not part of the cluster any more")
        void primaryLeft() {
            ClusterNode alive = node();
            ClusterNode gone = node();
            StatisticsTask task = taskWithAffinity(2, Map.of(0, alive, 1, gone));

            assertThatThrownBy(() -> task.map(List.of(alive), null)).isInstanceOf(TopologyChangedException.class);
        }

        @SuppressWarnings("unchecked")
        private StatisticsTask taskWithAffinity(int partitions, Map<Integer, ClusterNode> primaries) {
            Ignite ignite = mock(Ignite.class);
            Affinity<String> affinity = mock(Affinity.class);
            doReturn(affinity).when(ignite).affinity(IgniteConfig.PAYMENTS_CACHE);
            when(affinity.partitions()).thenReturn(partitions);
            when(affinity.mapPartitionsToNodes(anyCollection())).thenReturn(primaries);
            StatisticsTask task = new StatisticsTask(AggregationType.BY_DATE, TimeRange.unbounded());
            ReflectionTestUtils.setField(task, "ignite", ignite);
            return task;
        }
    }

    @Nested
    @DisplayName("result and reduce")
    class ResultAndReduce {

        private final StatisticsTask task = new StatisticsTask(AggregationType.BY_DATE, TimeRange.unbounded());

        @Test
        @DisplayName("successful jobs are awaited")
        void success() {
            assertThat(task.result(jobResult(null), List.of())).isEqualTo(ComputeJobResultPolicy.WAIT);
        }

        @Test
        @DisplayName("a node that left or a moved partition fails the task as a retryable topology change")
        void topologyChange() {
            assertThatThrownBy(() -> task.result(jobResult(new ClusterTopologyException("node left")), List.of()))
                    .isInstanceOf(TopologyChangedException.class);
            assertThatThrownBy(() -> task.result(jobResult(new IgniteException(new ComputeJobFailoverException("moved"))), List.of()))
                    .isInstanceOf(TopologyChangedException.class);
        }

        @Test
        @DisplayName("any other failure is passed through and not retried")
        void otherFailure() {
            IgniteException bug = new IgniteException("NullPointerException in job");

            assertThatThrownBy(() -> task.result(jobResult(bug), List.of())).isSameAs(bug);
            assertThat(StatisticsTask.isTopologyChange(bug)).isFalse();
        }

        @Test
        @DisplayName("reduce merges the groups of all jobs")
        void reduce() {
            Map<String, PartialStats> node1 = new HashMap<>(Map.of("2026-02-20", stats("100")));
            Map<String, PartialStats> node2 = new HashMap<>(Map.of("2026-02-20", stats("50"), "2026-02-21", stats("7")));

            Map<String, PartialStats> merged = task.reduce(List.of(jobData(node1), jobData(node2)));

            assertThat(merged).containsOnlyKeys("2026-02-20", "2026-02-21");
            assertThat(merged.get("2026-02-20").getCount()).isEqualTo(2);
            assertThat(merged.get("2026-02-20").getSum()).isEqualByComparingTo("150");
        }

        private ComputeJobResult jobResult(IgniteException exception) {
            ComputeJobResult result = mock(ComputeJobResult.class);
            when(result.getException()).thenReturn(exception);
            ClusterNode node = node();
            when(result.getNode()).thenReturn(node);
            return result;
        }

        private ComputeJobResult jobData(Map<String, PartialStats> data) {
            ComputeJobResult result = mock(ComputeJobResult.class);
            when(result.getData()).thenReturn(data);
            return result;
        }

        private PartialStats stats(String value) {
            PartialStats stats = new PartialStats();
            stats.add(new BigDecimal(value), at("2026-02-20T12:00"));
            return stats;
        }
    }

    @Nested
    @DisplayName("PartitionScanJob")
    class Job {

        @Test
        @DisplayName("runs a local scan per assigned partition and aggregates the entries")
        @SuppressWarnings("unchecked")
        void scansAssignedPartitionsLocally() {
            IgniteCache<String, Payment> cache = mock(IgniteCache.class);
            List<Integer> scanned = new ArrayList<>();
            when(cache.query(any(ScanQuery.class))).thenAnswer(invocation -> {
                ScanQuery<String, Payment> query = invocation.getArgument(0);
                assertThat(query.isLocal()).isTrue();
                scanned.add(query.getPartition());
                List<Cache.Entry<String, Payment>> entries = query.getPartition() == 3
                        ? List.of(ListQueryCursor.entry("p1", PAYMENTS.get(0)), ListQueryCursor.entry("p2", PAYMENTS.get(1)))
                        : List.of(ListQueryCursor.entry("p3", PAYMENTS.get(2)));
                return ListQueryCursor.ofEntries(entries);
            });

            Map<String, PartialStats> groups = job(cache, 3, 7).execute();

            assertThat(scanned).containsExactly(3, 7);
            assertThat(groups.get("2026-02-20").getCount()).isEqualTo(2);
            assertThat(groups.get("2026-02-21").getCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("a partition the node doesn't own fails the job for a retry")
        @SuppressWarnings("unchecked")
        void partitionNotOwned() {
            IgniteCache<String, Payment> cache = mock(IgniteCache.class);
            when(cache.query(any(ScanQuery.class))).thenThrow(new CacheException("No queryable nodes for partition 3"));

            assertThatThrownBy(() -> job(cache, 3).execute())
                    .isInstanceOf(ComputeJobFailoverException.class)
                    .hasMessageContaining("Partition 3 is not available");
        }

        private PartitionScanJob job(IgniteCache<String, Payment> cache, int... partitions) {
            Ignite ignite = mock(Ignite.class);
            doReturn(cache).when(ignite).cache(IgniteConfig.PAYMENTS_CACHE);
            when(ignite.name()).thenReturn("test-node");
            PartitionScanJob job = new PartitionScanJob(partitions, AggregationType.BY_DATE, TimeRange.unbounded());
            ReflectionTestUtils.setField(job, "ignite", ignite);
            return job;
        }
    }

    private static ClusterNode node() {
        ClusterNode node = mock(ClusterNode.class);
        UUID id = UUID.randomUUID();
        when(node.id()).thenReturn(id);
        return node;
    }
}
