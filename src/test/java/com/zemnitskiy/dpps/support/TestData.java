package com.zemnitskiy.dpps.support;

import com.zemnitskiy.dpps.model.Payment;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** Shared test data. */
public final class TestData {

    /**
     * 15 payments across 2 days (2026-02-20 and 2026-02-21) between banks A, B and C; amounts 50 to 300.
     * Totals: 5 payments / 500 on the 20th, 10 payments / 1350 on the 21st.
     */
    public static final String TEST_CSV = """
            DateTime,Sender,Receiver,Amount,ID
            2026-02-20T12:00,Bank A,Bank B,100,1
            2026-02-20T16:00,Bank B,Bank C,50,2
            2026-02-20T20:00,Bank A,Bank C,200,3
            2026-02-20T21:00,Bank C,Bank A,50,4
            2026-02-20T22:00,Bank B,Bank A,100,5
            2026-02-21T02:00,Bank A,Bank B,300,6
            2026-02-21T03:00,Bank B,Bank C,150,7
            2026-02-21T06:00,Bank C,Bank A,200,8
            2026-02-21T07:00,Bank A,Bank C,100,9
            2026-02-21T10:00,Bank C,Bank B,50,10
            2026-02-21T11:00,Bank B,Bank A,50,11
            2026-02-21T15:00,Bank A,Bank B,100,12
            2026-02-21T16:00,Bank B,Bank C,200,13
            2026-02-21T22:00,Bank C,Bank A,150,14
            2026-02-21T23:00,Bank A,Bank B,50,15
            """;

    public static final String HEADER = "DateTime,Sender,Receiver,Amount,ID\n";

    private TestData() {
    }

    public static Payment payment(String id, String dateTime, String sender, String receiver, String value) {
        return new Payment(id, LocalDateTime.parse(dateTime), sender, receiver, new BigDecimal(value));
    }

    public static LocalDateTime at(String dateTime) {
        return LocalDateTime.parse(dateTime);
    }

    /** CSV with {@code rows} distinct payments, one per minute from 2026-03-01T00:00, amount {@code 1.25} each. */
    public static String generatedCsv(int rows, String idPrefix) {
        StringBuilder csv = new StringBuilder(HEADER);
        LocalDateTime start = LocalDateTime.of(2026, 3, 1, 0, 0);
        for (int i = 0; i < rows; i++) {
            csv.append(start.plusMinutes(i)).append(",Bank ").append((char) ('A' + i % 5))
                    .append(",Bank ").append((char) ('A' + (i + 1) % 5))
                    .append(",1.25,").append(idPrefix).append(i).append('\n');
        }
        return csv.toString();
    }
}
