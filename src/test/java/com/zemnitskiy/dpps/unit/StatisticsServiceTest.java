package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.compute.PartialStats;
import com.zemnitskiy.dpps.compute.StatisticsTask;
import com.zemnitskiy.dpps.compute.TopologyChangedException;
import com.zemnitskiy.dpps.config.AppProperties;
import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.dto.StatisticsResponse.GroupStats;
import com.zemnitskiy.dpps.exception.ClusterUnavailableException;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.MetricCategory;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.service.StatisticsService;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.zemnitskiy.dpps.support.TestData.at;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** StatisticsService: shaping of the response and the retry policy around the MapReduce task. */
@DisplayName("StatisticsService")
@Tag("unit")
class StatisticsServiceTest {

    private static final int MAX_ATTEMPTS = 3;

    private final IgniteCompute compute = mock(IgniteCompute.class);
    private StatisticsService service;

    @BeforeEach
    void setUp() {
        Ignite ignite = mock(Ignite.class);
        IgniteCluster cluster = mock(IgniteCluster.class);
        ClusterGroup servers = mock(ClusterGroup.class);
        when(ignite.cluster()).thenReturn(cluster);
        when(cluster.forServers()).thenReturn(servers);
        when(ignite.compute(servers)).thenReturn(compute);
        when(compute.withNoFailover()).thenReturn(compute);
        when(compute.withTimeout(anyLong())).thenReturn(compute);

        AppProperties properties = new AppProperties(
                new AppProperties.Upload(1000, 0.85, Duration.ofSeconds(1)),
                new AppProperties.Statistics(Duration.ofSeconds(60), MAX_ATTEMPTS, Duration.ZERO));
        service = new StatisticsService(ignite, properties);
    }

    private void taskReturns(Map<String, PartialStats> groups) {
        when(compute.execute(any(StatisticsTask.class), isNull())).thenReturn(groups);
    }

    private static PartialStats stats(String... valueAndDateTimePairs) {
        PartialStats stats = new PartialStats();
        for (int i = 0; i < valueAndDateTimePairs.length; i += 2) {
            stats.add(new BigDecimal(valueAndDateTimePairs[i]), at(valueAndDateTimePairs[i + 1]));
        }
        return stats;
    }

    private StatisticsResponse calculate(Set<MetricCategory> metrics) {
        return service.calculateStatistics(AggregationType.BY_DATE, metrics, TimeRange.unbounded());
    }

    @Nested
    @DisplayName("Response")
    class Response {

        @Test
        @DisplayName("contains every metric section, computed from the merged partial result")
        void allMetrics() {
            taskReturns(Map.of("2026-02-20", stats("100", "2026-02-20T10:00", "200.5", "2026-02-20T14:00")));

            GroupStats group = calculate(EnumSet.allOf(MetricCategory.class)).data().get("2026-02-20");

            assertThat(group.general().count()).isEqualTo(2);
            assertThat(group.value().min()).isEqualByComparingTo("100");
            assertThat(group.value().max()).isEqualByComparingTo("200.5");
            assertThat(group.value().sum()).isEqualByComparingTo("300.5");
            assertThat(group.value().average()).isEqualByComparingTo("150.25");
            assertThat(group.dateTime().min()).isEqualTo(at("2026-02-20T10:00"));
            assertThat(group.dateTime().max()).isEqualTo(at("2026-02-20T14:00"));
            assertThat(group.dateTime().average()).isEqualTo(at("2026-02-20T12:00"));
        }

        @Test
        @DisplayName("contains only the requested sections")
        void onlyRequestedSections() {
            taskReturns(Map.of("2026-02-20", stats("100", "2026-02-20T10:00")));

            GroupStats group = calculate(EnumSet.of(MetricCategory.VALUE)).data().get("2026-02-20");

            assertThat(group.value()).isNotNull();
            assertThat(group.general()).isNull();
            assertThat(group.dateTime()).isNull();
        }

        @Test
        @DisplayName("is sorted by group key")
        void sorted() {
            Map<String, PartialStats> groups = new HashMap<>();
            groups.put("2026-02-22", stats("1", "2026-02-22T00:00"));
            groups.put("2026-02-20", stats("1", "2026-02-20T00:00"));
            groups.put("2026-02-21", stats("1", "2026-02-21T00:00"));
            taskReturns(groups);

            assertThat(calculate(EnumSet.of(MetricCategory.GENERAL)).data().keySet())
                    .containsExactly("2026-02-20", "2026-02-21", "2026-02-22");
        }

        @Test
        @DisplayName("is empty when there is no data")
        void empty() {
            taskReturns(Map.of());

            assertThat(calculate(EnumSet.of(MetricCategory.GENERAL)).data()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Retries")
    class Retries {

        @Test
        @DisplayName("a topology change is retried on a fresh snapshot")
        void retriedAfterTopologyChange() {
            when(compute.execute(any(StatisticsTask.class), isNull()))
                    .thenThrow(new IgniteException(new TopologyChangedException("node left", null)))
                    .thenReturn(Map.of("2026-02-20", stats("5", "2026-02-20T10:00")));

            StatisticsResponse response = calculate(EnumSet.of(MetricCategory.GENERAL));

            assertThat(response.data().get("2026-02-20").general().count()).isEqualTo(1);
            verify(compute, times(2)).execute(any(StatisticsTask.class), isNull());
        }

        @Test
        @DisplayName("gives up with 503-style error after the configured number of attempts")
        void givesUp() {
            when(compute.execute(any(StatisticsTask.class), isNull()))
                    .thenThrow(new TopologyChangedException("node left", null));

            assertThatThrownBy(() -> calculate(EnumSet.of(MetricCategory.GENERAL)))
                    .isInstanceOf(ClusterUnavailableException.class)
                    .hasMessageContaining("topology kept changing");
            verify(compute, times(MAX_ATTEMPTS)).execute(any(StatisticsTask.class), isNull());
        }

        @Test
        @DisplayName("other failures are not retried")
        void otherFailuresNotRetried() {
            IgniteException failure = new IgniteException("bug");
            when(compute.execute(any(StatisticsTask.class), isNull())).thenThrow(failure);

            assertThatThrownBy(() -> calculate(EnumSet.of(MetricCategory.GENERAL))).isSameAs(failure);
            verify(compute, times(1)).execute(any(StatisticsTask.class), isNull());
        }

        @Test
        @DisplayName("a timeout is reported as the cluster being unavailable")
        void timeout() {
            when(compute.execute(any(StatisticsTask.class), isNull()))
                    .thenThrow(new ComputeTaskTimeoutException("timed out"));

            assertThatThrownBy(() -> calculate(EnumSet.of(MetricCategory.GENERAL)))
                    .isInstanceOf(ClusterUnavailableException.class)
                    .hasMessageContaining("did not finish within 60 s");
        }
    }
}
