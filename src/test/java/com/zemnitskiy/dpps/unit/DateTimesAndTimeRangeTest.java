package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.exception.InvalidRequestException;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.util.DateTimes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.format.DateTimeParseException;

import static com.zemnitskiy.dpps.support.TestData.at;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("DateTimes and TimeRange")
@Tag("unit")
class DateTimesAndTimeRangeTest {

    @Nested
    @DisplayName("DateTimes.parse")
    class Parse {

        @ParameterizedTest(name = "\"{0}\" -> {1}")
        @CsvSource(delimiter = '|', value = {
                "2026-02-20T12:00           | 2026-02-20T12:00",
                "2026-02-20T12:00:30        | 2026-02-20T12:00:30",
                "2026-02-20T12:00:30.25     | 2026-02-20T12:00:30.250",
                "2026-02-20 12:00           | 2026-02-20T12:00",
                "'  2026-02-20T12:00  '     | 2026-02-20T12:00"})
        @DisplayName("accepts ISO-8601 local date-times, with 'T' or a space")
        void accepted(String text, String expected) {
            assertThat(DateTimes.parse(text)).isEqualTo(at(expected));
        }

        @ParameterizedTest(name = "\"{0}\"")
        @ValueSource(strings = {"2026-02-20", "2026-02-20  12:00", "2026-02-30T12:00", "2026-13-01T00:00",
                "2026-02-20T24:00", "2026-02-20T12:00Z", "2026-02-20T12:00+02:00", "20.02.2026 12:00", "", "now"})
        @DisplayName("rejects anything else, including impossible dates and zoned values")
        void rejected(String text) {
            assertThatThrownBy(() -> DateTimes.parse(text)).isInstanceOf(DateTimeParseException.class);
        }
    }

    @Nested
    @DisplayName("TimeRange")
    class Range {

        private final TimeRange feb20 = TimeRange.of(at("2026-02-20T00:00"), at("2026-02-20T23:59:59"));

        @Test
        @DisplayName("includes both bounds")
        void boundsIncluded() {
            assertThat(feb20.contains(at("2026-02-20T00:00"))).isTrue();
            assertThat(feb20.contains(at("2026-02-20T23:59:59"))).isTrue();
            assertThat(feb20.contains(at("2026-02-20T23:59:59.001"))).isFalse();
            assertThat(feb20.contains(at("2026-02-19T23:59:59.999"))).isFalse();
        }

        @Test
        @DisplayName("compares real timestamps, not strings: 23:59 means 23:59:00")
        void precisionIndependent() {
            TimeRange untilMinute = TimeRange.of(at("2026-02-20T00:00"), at("2026-02-20T23:59"));

            assertThat(untilMinute.contains(at("2026-02-20T23:59:00"))).isTrue();
            assertThat(untilMinute.contains(at("2026-02-20T23:59:30"))).isFalse();
        }

        @Test
        @DisplayName("may be open on either side or both")
        void openEnded() {
            assertThat(TimeRange.of(at("2026-02-20T00:00"), null).contains(at("2099-01-01T00:00"))).isTrue();
            assertThat(TimeRange.of(null, at("2026-02-20T00:00")).contains(at("1999-01-01T00:00"))).isTrue();
            assertThat(TimeRange.of(null, null)).isSameAs(TimeRange.unbounded());
            assertThat(TimeRange.unbounded().contains(at("2026-02-20T00:00"))).isTrue();
            assertThat(TimeRange.unbounded().isUnbounded()).isTrue();
        }

        @Test
        @DisplayName("rejects 'from' after 'to' but allows a single instant")
        void ordering() {
            assertThatThrownBy(() -> TimeRange.of(at("2026-02-21T00:00"), at("2026-02-20T00:00")))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("must not be after");
            assertThat(TimeRange.of(at("2026-02-20T00:00"), at("2026-02-20T00:00")).length()).isZero();
        }

        @Test
        @DisplayName("knows its length only when bounded")
        void length() {
            assertThat(feb20.length()).isEqualTo(Duration.ofDays(1).minusSeconds(1));
            assertThatThrownBy(() -> TimeRange.of(at("2026-02-20T00:00"), null).length())
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        @DisplayName("never contains a missing date-time")
        void nullDateTime() {
            assertThat(TimeRange.unbounded().contains(null)).isFalse();
        }
    }
}
