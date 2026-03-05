package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.compute.PartialStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PartialStats — accumulator for MapReduce statistics.
 * Tests accumulation, merging, and edge cases.
 */
@DisplayName("PartialStats — Unit Tests")
@Tag("unit")
class PartialStatsTest {

    private PartialStats stats;

    @BeforeEach
    void setUp() {
        stats = new PartialStats();
    }

    @Nested
    @DisplayName("Accumulate Tests")
    class AccumulateTests {

        @Test
        @DisplayName("Single accumulate — sets all fields correctly")
        void singleAccumulate_shouldSetAllFields() {
            stats.accumulate(100.0, "2026-02-20T12:00", 1771588800L);

            assertThat(stats.getCount()).isEqualTo(1);
            assertThat(stats.getSumValue()).isEqualTo(100.0);
            assertThat(stats.getMinValue()).isEqualTo(100.0);
            assertThat(stats.getMaxValue()).isEqualTo(100.0);
            assertThat(stats.getMinDateTime()).isEqualTo("2026-02-20T12:00");
            assertThat(stats.getMaxDateTime()).isEqualTo("2026-02-20T12:00");
            assertThat(stats.getDateTimeSumEpochSeconds()).isEqualTo(1771588800L);
        }

        @Test
        @DisplayName("Multiple accumulates — updates min/max/sum correctly")
        void multipleAccumulates_shouldUpdateMinMaxSum() {
            stats.accumulate(100.0, "2026-02-20T12:00", 1771588800L);
            stats.accumulate(50.0, "2026-02-19T10:00", 1771495200L);
            stats.accumulate(200.0, "2026-02-21T14:00", 1771682400L);

            assertThat(stats.getCount()).isEqualTo(3);
            assertThat(stats.getSumValue()).isEqualTo(350.0);
            assertThat(stats.getMinValue()).isEqualTo(50.0);
            assertThat(stats.getMaxValue()).isEqualTo(200.0);
            assertThat(stats.getMinDateTime()).isEqualTo("2026-02-19T10:00");
            assertThat(stats.getMaxDateTime()).isEqualTo("2026-02-21T14:00");
        }

        @Test
        @DisplayName("Accumulate negative values — handled correctly for BY_BANK scenario")
        void accumulateNegativeValues_shouldTrackCorrectly() {
            stats.accumulate(-100.0, "2026-02-20T12:00", 1771588800L);
            stats.accumulate(50.0, "2026-02-20T14:00", 1771596000L);

            assertThat(stats.getCount()).isEqualTo(2);
            assertThat(stats.getSumValue()).isEqualTo(-50.0);
            assertThat(stats.getMinValue()).isEqualTo(-100.0);
            assertThat(stats.getMaxValue()).isEqualTo(50.0);
        }
    }

    @Nested
    @DisplayName("Merge Tests")
    class MergeTests {

        @Test
        @DisplayName("Merge two non-empty stats — combines correctly")
        void mergeTwoNonEmpty_shouldCombine() {
            stats.accumulate(100.0, "2026-02-20T12:00", 1771588800L);
            stats.accumulate(150.0, "2026-02-20T14:00", 1771596000L);

            PartialStats other = new PartialStats();
            other.accumulate(50.0, "2026-02-19T10:00", 1771495200L);
            other.accumulate(300.0, "2026-02-22T18:00", 1771783200L);

            PartialStats merged = stats.merge(other);

            assertThat(merged.getCount()).isEqualTo(4);
            assertThat(merged.getSumValue()).isEqualTo(600.0);
            assertThat(merged.getMinValue()).isEqualTo(50.0);
            assertThat(merged.getMaxValue()).isEqualTo(300.0);
            assertThat(merged.getMinDateTime()).isEqualTo("2026-02-19T10:00");
            assertThat(merged.getMaxDateTime()).isEqualTo("2026-02-22T18:00");
        }

        @Test
        @DisplayName("Merge with null — returns this")
        void mergeWithNull_shouldReturnThis() {
            stats.accumulate(100.0, "2026-02-20T12:00", 1771588800L);

            PartialStats result = stats.merge(null);

            assertThat(result).isSameAs(stats);
            assertThat(result.getCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("Merge with empty stats — returns this")
        void mergeWithEmpty_shouldReturnThis() {
            stats.accumulate(100.0, "2026-02-20T12:00", 1771588800L);
            PartialStats empty = new PartialStats();

            PartialStats result = stats.merge(empty);

            assertThat(result).isSameAs(stats);
            assertThat(result.getCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("Merge empty with non-empty — returns other")
        void mergeEmptyWithNonEmpty_shouldReturnOther() {
            PartialStats other = new PartialStats();
            other.accumulate(100.0, "2026-02-20T12:00", 1771588800L);

            PartialStats result = stats.merge(other);

            assertThat(result).isSameAs(other);
            assertThat(result.getCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("Merge preserves epoch seconds sum for average calculation")
        void merge_shouldPreserveEpochSecondsSum() {
            stats.accumulate(100.0, "2026-02-20T12:00", 1000L);
            stats.accumulate(100.0, "2026-02-20T14:00", 2000L);

            PartialStats other = new PartialStats();
            other.accumulate(100.0, "2026-02-21T10:00", 3000L);

            stats.merge(other);

            assertThat(stats.getDateTimeSumEpochSeconds()).isEqualTo(6000L);
            assertThat(stats.getAverageEpochSeconds()).isEqualTo(2000L);
        }
    }

    @Nested
    @DisplayName("Average Calculation Tests")
    class AverageTests {

        @Test
        @DisplayName("Average value — calculates correctly")
        void averageValue_shouldCalculateCorrectly() {
            stats.accumulate(100.0, "2026-02-20T12:00", 1000L);
            stats.accumulate(200.0, "2026-02-20T14:00", 2000L);
            stats.accumulate(300.0, "2026-02-20T16:00", 3000L);

            assertThat(stats.getAverageValue()).isEqualTo(200.0);
        }

        @Test
        @DisplayName("Average value on empty stats — returns zero")
        void averageValueOnEmpty_shouldReturnZero() {
            assertThat(stats.getAverageValue()).isEqualTo(0.0);
        }

        @Test
        @DisplayName("Average epoch seconds — calculates correctly")
        void averageEpochSeconds_shouldCalculateCorrectly() {
            stats.accumulate(100.0, "2026-02-20T12:00", 1000L);
            stats.accumulate(100.0, "2026-02-20T14:00", 3000L);

            assertThat(stats.getAverageEpochSeconds()).isEqualTo(2000L);
        }

        @Test
        @DisplayName("Average epoch seconds on empty stats — returns zero")
        void averageEpochSecondsOnEmpty_shouldReturnZero() {
            assertThat(stats.getAverageEpochSeconds()).isZero();
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("Initial state — has default values")
        void initialState_shouldHaveDefaults() {
            assertThat(stats.getCount()).isZero();
            assertThat(stats.getSumValue()).isZero();
            assertThat(stats.getMinValue()).isEqualTo(Double.MAX_VALUE);
            assertThat(stats.getMaxValue()).isEqualTo(-Double.MAX_VALUE);
            assertThat(stats.getMinDateTime()).isNull();
            assertThat(stats.getMaxDateTime()).isNull();
            assertThat(stats.getDateTimeSumEpochSeconds()).isZero();
        }

        @Test
        @DisplayName("Same datetime for all records — min and max are equal")
        void sameDateTimeForAll_shouldHaveEqualMinMax() {
            String dateTime = "2026-02-20T12:00";
            stats.accumulate(100.0, dateTime, 1771588800L);
            stats.accumulate(200.0, dateTime, 1771588800L);

            assertThat(stats.getMinDateTime()).isEqualTo(dateTime);
            assertThat(stats.getMaxDateTime()).isEqualTo(dateTime);
        }
    }
}
