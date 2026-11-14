package com.zemnitskiy.dpps.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * The one date-time format of the API, used for CSV values and query parameters alike:
 * an ISO-8601 local date-time with optional seconds and fraction, e.g. {@code 2026-02-20T12:00},
 * {@code 2026-02-20T12:00:30} or {@code 2026-02-20T12:00:30.250}. A space is accepted instead of {@code T}.
 * Offsets and zones are rejected: the system works with local date-times only.
 */
public final class DateTimes {

    /** Human-readable description used in error messages. */
    public static final String EXPECTED_FORMAT =
            "an ISO-8601 local date-time such as 2026-02-20T12:00 or 2026-02-20T12:00:30";

    private static final int DATE_LENGTH = "yyyy-MM-dd".length();

    private DateTimes() {
    }

    /**
     * @throws DateTimeParseException if {@code text} is not in the accepted format or is not a valid date-time
     */
    public static LocalDateTime parse(String text) {
        String value = text.strip();
        if (value.length() > DATE_LENGTH && value.charAt(DATE_LENGTH) == ' ') {
            value = value.substring(0, DATE_LENGTH) + 'T' + value.substring(DATE_LENGTH + 1);
        }
        // ISO_LOCAL_DATE_TIME resolves strictly: 2026-02-30 or 24:00 are rejected, not adjusted.
        return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
}
