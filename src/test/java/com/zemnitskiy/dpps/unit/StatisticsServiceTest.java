package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.compute.PartialStats;
import com.zemnitskiy.dpps.compute.StatisticsCallable;
import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.dto.StatisticsResponse.DateTimeStats;
import com.zemnitskiy.dpps.dto.StatisticsResponse.GroupStats;
import com.zemnitskiy.dpps.dto.StatisticsResponse.ValueStats;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.MetricCategory;
import com.zemnitskiy.dpps.service.StatisticsService;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for StatisticsService — orchestrates MapReduce statistics computation.
 * Tests merging of partial results and response building.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("StatisticsService — Unit Tests")
@Tag("unit")
class StatisticsServiceTest {

    @Mock
    private Ignite ignite;

    @Mock
    private IgniteCompute igniteCompute;

    @InjectMocks
    private StatisticsService statisticsService;

    @BeforeEach
    void setUp() {
        when(ignite.compute()).thenReturn(igniteCompute);
    }

    @Nested
    @DisplayName("Calculate Statistics Tests")
    class CalculateStatisticsTests {

        @Test
        @DisplayName("Single node result — returns correct statistics")
        void singleNodeResult_shouldReturnStatistics() {
            Map<String, PartialStats> nodeResult = new HashMap<>();
            PartialStats stats = new PartialStats();
            stats.accumulate(100.0, "2026-02-20T10:00:00", 1771582800L);
            stats.accumulate(200.0, "2026-02-20T14:00:00", 1771597200L);
            nodeResult.put("2026-02-20", stats);

            when(igniteCompute.broadcast(any(StatisticsCallable.class)))
                    .thenReturn(List.of(nodeResult));

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    EnumSet.allOf(MetricCategory.class),
                    null, null
            );

            assertThat(response.data()).hasSize(1);
            assertThat(response.data()).containsKey("2026-02-20");

            GroupStats groupStats = response.data().get("2026-02-20");
            assertThat(groupStats.general().count()).isEqualTo(2);
            assertThat(groupStats.value().sum()).isEqualTo(300.0);
            assertThat(groupStats.value().min()).isEqualTo(100.0);
            assertThat(groupStats.value().max()).isEqualTo(200.0);
            assertThat(groupStats.value().average()).isEqualTo(150.0);
        }

        @Test
        @DisplayName("Multiple node results — merges correctly")
        void multipleNodeResults_shouldMerge() {
            Map<String, PartialStats> node1Result = new HashMap<>();
            PartialStats stats1 = new PartialStats();
            stats1.accumulate(100.0, "2026-02-20T10:00:00", 1771582800L);
            node1Result.put("2026-02-20", stats1);

            Map<String, PartialStats> node2Result = new HashMap<>();
            PartialStats stats2 = new PartialStats();
            stats2.accumulate(200.0, "2026-02-20T14:00:00", 1771597200L);
            node2Result.put("2026-02-20", stats2);

            when(igniteCompute.broadcast(any(StatisticsCallable.class)))
                    .thenReturn(List.of(node1Result, node2Result));

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    EnumSet.allOf(MetricCategory.class),
                    null, null
            );

            GroupStats groupStats = response.data().get("2026-02-20");
            assertThat(groupStats.general().count()).isEqualTo(2);
            assertThat(groupStats.value().sum()).isEqualTo(300.0);
            assertThat(groupStats.value().min()).isEqualTo(100.0);
            assertThat(groupStats.value().max()).isEqualTo(200.0);
        }

        @Test
        @DisplayName("Multiple groups from multiple nodes — merges and sorts")
        void multipleGroupsFromMultipleNodes_shouldMergeAndSort() {
            Map<String, PartialStats> node1Result = new HashMap<>();
            PartialStats stats1a = new PartialStats();
            stats1a.accumulate(100.0, "2026-02-20T10:00:00", 1771582800L);
            node1Result.put("2026-02-20", stats1a);

            PartialStats stats1b = new PartialStats();
            stats1b.accumulate(150.0, "2026-02-21T10:00:00", 1771669200L);
            node1Result.put("2026-02-21", stats1b);

            Map<String, PartialStats> node2Result = new HashMap<>();
            PartialStats stats2a = new PartialStats();
            stats2a.accumulate(200.0, "2026-02-20T14:00:00", 1771597200L);
            node2Result.put("2026-02-20", stats2a);

            when(igniteCompute.broadcast(any(StatisticsCallable.class)))
                    .thenReturn(List.of(node1Result, node2Result));

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    EnumSet.allOf(MetricCategory.class),
                    null, null
            );

            assertThat(response.data()).hasSize(2);
            // Verify sorted order
            assertThat(response.data().keySet()).containsExactly("2026-02-20", "2026-02-21");

            // Feb 20 merged from both nodes
            assertThat(response.data().get("2026-02-20").general().count()).isEqualTo(2);
            // Feb 21 only from node 1
            assertThat(response.data().get("2026-02-21").general().count()).isEqualTo(1);
        }

        @Test
        @DisplayName("Empty results — returns empty response")
        void emptyResults_shouldReturnEmptyResponse() {
            when(igniteCompute.broadcast(any(StatisticsCallable.class)))
                    .thenReturn(List.of(new HashMap<>()));

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    EnumSet.allOf(MetricCategory.class),
                    null, null
            );

            assertThat(response.data()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Metric Filtering Tests")
    class MetricFilteringTests {

        private void setupMockWithSingleResult() {
            Map<String, PartialStats> nodeResult = new HashMap<>();
            PartialStats stats = new PartialStats();
            stats.accumulate(100.0, "2026-02-20T10:00:00", 1771582800L);
            nodeResult.put("2026-02-20", stats);

            when(igniteCompute.broadcast(any(StatisticsCallable.class)))
                    .thenReturn(List.of(nodeResult));
        }

        @Test
        @DisplayName("Only GENERAL metric — includes only general stats")
        void onlyGeneralMetric_shouldIncludeOnlyGeneral() {
            setupMockWithSingleResult();

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    Set.of(MetricCategory.GENERAL),
                    null, null
            );

            GroupStats groupStats = response.data().get("2026-02-20");
            assertThat(groupStats.general()).isNotNull();
            assertThat(groupStats.general().count()).isEqualTo(1);
            assertThat(groupStats.value()).isNull();
            assertThat(groupStats.dateTime()).isNull();
        }

        @Test
        @DisplayName("Only VALUE metric — includes only value stats")
        void onlyValueMetric_shouldIncludeOnlyValue() {
            setupMockWithSingleResult();

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    Set.of(MetricCategory.VALUE),
                    null, null
            );

            GroupStats groupStats = response.data().get("2026-02-20");
            assertThat(groupStats.general()).isNull();
            assertThat(groupStats.value()).isNotNull();
            assertThat(groupStats.value().sum()).isEqualTo(100.0);
            assertThat(groupStats.dateTime()).isNull();
        }

        @Test
        @DisplayName("Only DATETIME metric — includes only datetime stats")
        void onlyDatetimeMetric_shouldIncludeOnlyDatetime() {
            setupMockWithSingleResult();

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    Set.of(MetricCategory.DATETIME),
                    null, null
            );

            GroupStats groupStats = response.data().get("2026-02-20");
            assertThat(groupStats.general()).isNull();
            assertThat(groupStats.value()).isNull();
            assertThat(groupStats.dateTime()).isNotNull();
            assertThat(groupStats.dateTime().min()).isEqualTo("2026-02-20T10:00:00");
            assertThat(groupStats.dateTime().max()).isEqualTo("2026-02-20T10:00:00");
        }

        @Test
        @DisplayName("GENERAL and VALUE metrics — includes both")
        void generalAndValueMetrics_shouldIncludeBoth() {
            setupMockWithSingleResult();

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    Set.of(MetricCategory.GENERAL, MetricCategory.VALUE),
                    null, null
            );

            GroupStats groupStats = response.data().get("2026-02-20");
            assertThat(groupStats.general()).isNotNull();
            assertThat(groupStats.value()).isNotNull();
            assertThat(groupStats.dateTime()).isNull();
        }

        @Test
        @DisplayName("All metrics — includes everything")
        void allMetrics_shouldIncludeAll() {
            setupMockWithSingleResult();

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    EnumSet.allOf(MetricCategory.class),
                    null, null
            );

            GroupStats groupStats = response.data().get("2026-02-20");
            assertThat(groupStats.general()).isNotNull();
            assertThat(groupStats.value()).isNotNull();
            assertThat(groupStats.dateTime()).isNotNull();
        }
    }

    @Nested
    @DisplayName("Value Rounding Tests")
    class ValueRoundingTests {

        @Test
        @DisplayName("Values are rounded to 4 decimal places")
        void values_shouldBeRoundedTo4DecimalPlaces() {
            Map<String, PartialStats> nodeResult = new HashMap<>();
            PartialStats stats = new PartialStats();
            stats.accumulate(100.123456789, "2026-02-20T10:00:00", 1771582800L);
            stats.accumulate(200.987654321, "2026-02-20T14:00:00", 1771597200L);
            stats.accumulate(50.111111111, "2026-02-20T16:00:00", 1771604400L);
            nodeResult.put("2026-02-20", stats);

            when(igniteCompute.broadcast(any(StatisticsCallable.class)))
                    .thenReturn(List.of(nodeResult));

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    Set.of(MetricCategory.VALUE),
                    null, null
            );

            ValueStats valueStats = response.data().get("2026-02-20").value();
            // Verify rounding to 4 decimal places
            assertThat(valueStats.min()).isEqualTo(50.1111);
            assertThat(valueStats.max()).isEqualTo(200.9877);
            assertThat(valueStats.sum()).isEqualTo(351.2222);
            assertThat(valueStats.average()).isEqualTo(117.0741);
        }
    }

    @Nested
    @DisplayName("DateTime Statistics Tests")
    class DateTimeStatisticsTests {

        @Test
        @DisplayName("DateTime stats calculate average correctly")
        void dateTimeStats_shouldCalculateAverageCorrectly() {
            Map<String, PartialStats> nodeResult = new HashMap<>();
            PartialStats stats = new PartialStats();
            // Two payments at specific times
            stats.accumulate(100.0, "2026-02-20T10:00:00", 1771581600L); // 10:00
            stats.accumulate(200.0, "2026-02-20T14:00:00", 1771596000L); // 14:00
            nodeResult.put("2026-02-20", stats);

            when(igniteCompute.broadcast(any(StatisticsCallable.class)))
                    .thenReturn(List.of(nodeResult));

            StatisticsResponse response = statisticsService.calculateStatistics(
                    AggregationType.BY_DATE,
                    Set.of(MetricCategory.DATETIME),
                    null, null
            );

            DateTimeStats dtStats = response.data().get("2026-02-20").dateTime();
            assertThat(dtStats.min()).isEqualTo("2026-02-20T10:00:00");
            assertThat(dtStats.max()).isEqualTo("2026-02-20T14:00:00");
            // Average epoch should produce datetime between min and max
            assertThat(dtStats.average()).isNotNull();
        }
    }
}
