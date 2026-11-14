package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.compute.PartialStats;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.zemnitskiy.dpps.support.TestData.at;
import static org.assertj.core.api.Assertions.assertThat;

/** PartialStats: the mergeable per-group accumulator of the MapReduce. */
@DisplayName("PartialStats")
@Tag("unit")
class PartialStatsTest {

    private static PartialStats stats(String... valueAndDateTimePairs) {
        PartialStats stats = new PartialStats();
        for (int i = 0; i < valueAndDateTimePairs.length; i += 2) {
            stats.add(new BigDecimal(valueAndDateTimePairs[i]), at(valueAndDateTimePairs[i + 1]));
        }
        return stats;
    }

    @Nested
    @DisplayName("add")
    class Add {

        @Test
        @DisplayName("tracks count, exact sum, min, max and the date-time span")
        void tracksEverything() {
            PartialStats stats = stats("100", "2026-02-20T12:00", "50", "2026-02-19T10:00", "200", "2026-02-21T14:00");

            assertThat(stats.getCount()).isEqualTo(3);
            assertThat(stats.getSum()).isEqualByComparingTo("350");
            assertThat(stats.getMin()).isEqualByComparingTo("50");
            assertThat(stats.getMax()).isEqualByComparingTo("200");
            assertThat(stats.getEarliest()).isEqualTo(at("2026-02-19T10:00"));
            assertThat(stats.getLatest()).isEqualTo(at("2026-02-21T14:00"));
        }

        @Test
        @DisplayName("sums decimals exactly (no floating-point drift)")
        void exactDecimals() {
            PartialStats stats = new PartialStats();
            for (int i = 0; i < 10; i++) {
                stats.add(new BigDecimal("0.1"), at("2026-02-20T12:00"));
            }

            assertThat(stats.getSum()).isEqualTo(new BigDecimal("1.0"));
        }

        @Test
        @DisplayName("handles negative values (the sender side of BY_BANK)")
        void negativeValues() {
            PartialStats stats = stats("-100", "2026-02-20T12:00", "50", "2026-02-20T14:00");

            assertThat(stats.getSum()).isEqualByComparingTo("-50");
            assertThat(stats.getMin()).isEqualByComparingTo("-100");
            assertThat(stats.getMax()).isEqualByComparingTo("50");
        }
    }

    @Nested
    @DisplayName("merge")
    class Merge {

        @Test
        @DisplayName("combines two partial results")
        void combines() {
            PartialStats left = stats("100", "2026-02-20T12:00", "150", "2026-02-20T14:00");
            PartialStats right = stats("50", "2026-02-19T10:00", "300", "2026-02-22T18:00");

            PartialStats merged = left.merge(right);

            assertThat(merged).isSameAs(left);
            assertThat(merged.getCount()).isEqualTo(4);
            assertThat(merged.getSum()).isEqualByComparingTo("600");
            assertThat(merged.getMin()).isEqualByComparingTo("50");
            assertThat(merged.getMax()).isEqualByComparingTo("300");
            assertThat(merged.getEarliest()).isEqualTo(at("2026-02-19T10:00"));
            assertThat(merged.getLatest()).isEqualTo(at("2026-02-22T18:00"));
        }

        @Test
        @DisplayName("is order-independent")
        void orderIndependent() {
            PartialStats a = stats("1.5", "2026-02-20T12:00", "2", "2026-02-21T12:00");
            PartialStats b = stats("7", "2026-02-19T12:00");
            PartialStats a2 = stats("1.5", "2026-02-20T12:00", "2", "2026-02-21T12:00");
            PartialStats b2 = stats("7", "2026-02-19T12:00");

            PartialStats ab = a.merge(b);
            PartialStats ba = b2.merge(a2);

            assertThat(ab).usingRecursiveComparison().isEqualTo(ba);
        }

        @Test
        @DisplayName("ignores null and empty partials")
        void emptyPartials() {
            PartialStats stats = stats("100", "2026-02-20T12:00");

            assertThat(stats.merge(null)).isSameAs(stats);
            assertThat(stats.merge(new PartialStats())).isSameAs(stats);
            assertThat(new PartialStats().merge(stats)).isSameAs(stats);
            assertThat(stats.getCount()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("averages")
    class Averages {

        @Test
        @DisplayName("average amount is rounded half-up to 4 decimals without trailing zeros")
        void averageAmount() {
            assertThat(stats("100", "2026-02-20T12:00", "200", "2026-02-20T12:00").average()).isEqualTo(new BigDecimal("150"));
            assertThat(stats("1", "2026-02-20T12:00", "1", "2026-02-20T12:00", "0.5", "2026-02-20T12:00").average())
                    .isEqualTo(new BigDecimal("0.8333"));
            assertThat(stats("0.00005", "2026-02-20T12:00").average()).isEqualTo(new BigDecimal("0.0001"));
        }

        @Test
        @DisplayName("average date-time is the mean timestamp, to the second")
        void averageDateTime() {
            PartialStats stats = stats("1", "2026-02-20T12:00", "1", "2026-02-20T14:00", "1", "2026-02-20T14:00:01");

            assertThat(stats.averageDateTime()).isEqualTo(at("2026-02-20T13:20:00"));
        }

        @Test
        @DisplayName("average date-time rounds towards the past, also before 1970")
        void averageDateTimeBeforeEpoch() {
            PartialStats stats = stats("1", "1969-12-31T23:59:59", "1", "1969-12-31T23:59:58");

            assertThat(stats.averageDateTime()).isEqualTo(at("1969-12-31T23:59:58"));
        }

        @Test
        @DisplayName("are null for an empty group")
        void empty() {
            assertThat(new PartialStats().average()).isNull();
            assertThat(new PartialStats().averageDateTime()).isNull();
        }
    }
}
