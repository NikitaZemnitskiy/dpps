package com.zemnitskiy.dpps;

import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.service.CsvParsingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for CsvParsingService — no Spring context, no Ignite.
 * Tests pure CSV parsing and validation logic.
 */
@DisplayName("CsvParsingService — Unit Tests")
class CsvParsingServiceTest {

    private CsvParsingService csvParsingService;

    @BeforeEach
    void setUp() {
        csvParsingService = new CsvParsingService();
    }

    @Test
    @DisplayName("Valid CSV — parses all rows into Payment objects")
    void validCsv_shouldParseAllRows() {
        String csv = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-20T12:00,Bank A,Bank B,100,1
                2026-02-20T16:00,Bank B,Bank C,50,2
                2026-02-20T20:00,Bank A,Bank C,200,3
                """;

        UploadResult result = new UploadResult();
        List<Payment> payments = csvParsingService.parse(toStream(csv), result);

        assertThat(payments).hasSize(3);
        assertThat(result.getErrors()).isEmpty();

        Payment first = payments.get(0);
        assertThat(first.getId()).isEqualTo("1");
        assertThat(first.getDateTime()).isEqualTo(LocalDateTime.of(2026, 2, 20, 12, 0));
        assertThat(first.getSender()).isEqualTo("Bank A");
        assertThat(first.getReceiver()).isEqualTo("Bank B");
        assertThat(first.getValue()).isEqualTo(100.0);
    }

    @Test
    @DisplayName("Missing fields — skipped with specific error types")
    void missingFields_shouldReportErrors() {
        // Row 1: missing sender, Row 2: missing datetime, Row 3: missing value
        String csv = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-20T12:00,,Bank B,100,1
                ,Bank A,Bank B,100,2
                2026-02-20T14:00,Bank A,Bank B,,3
                """;

        UploadResult result = new UploadResult();
        List<Payment> payments = csvParsingService.parse(toStream(csv), result);

        assertThat(payments).isEmpty();
        assertThat(result.getErrors())
                .containsEntry("missing_sender", 1)
                .containsEntry("missing_datetime", 1)
                .containsEntry("missing_value", 1);
    }

    @Test
    @DisplayName("Invalid amounts (negative, zero, NaN) — reports invalid_value")
    void invalidAmounts_shouldReportInvalidValue() {
        String csv = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-20T12:00,Bank A,Bank B,-50,1
                2026-02-20T13:00,Bank A,Bank B,0,2
                2026-02-20T14:00,Bank A,Bank B,abc,3
                """;

        UploadResult result = new UploadResult();
        List<Payment> payments = csvParsingService.parse(toStream(csv), result);

        assertThat(payments).isEmpty();
        assertThat(result.getErrors()).containsEntry("invalid_value", 3);
    }

    @Test
    @DisplayName("Header-only CSV — returns empty list, no errors")
    void headerOnly_shouldReturnEmptyList() {
        String csv = "DateTime,Sender,Receiver,Amount,ID\n";

        UploadResult result = new UploadResult();
        List<Payment> payments = csvParsingService.parse(toStream(csv), result);

        assertThat(payments).isEmpty();
        assertThat(result.getErrors()).isEmpty();
    }

    @Test
    @DisplayName("'Value' column name works as alias for 'Amount'")
    void valueColumnAlias_shouldWork() {
        String csv = """
                DateTime,Sender,Receiver,Value,ID
                2026-02-20T12:00,Bank A,Bank B,999,1
                """;

        UploadResult result = new UploadResult();
        List<Payment> payments = csvParsingService.parse(toStream(csv), result);

        assertThat(payments).hasSize(1);
        assertThat(payments.get(0).getValue()).isEqualTo(999.0);
    }

    @Test
    @DisplayName("Fields are trimmed of whitespace")
    void fieldsTrimmed_shouldReturnCleanData() {
        String csv = """
                DateTime,Sender,Receiver,Amount,ID
                 2026-02-20T12:00 , Bank A , Bank B , 100 , X1\s
                """;

        UploadResult result = new UploadResult();
        List<Payment> payments = csvParsingService.parse(toStream(csv), result);

        assertThat(payments).hasSize(1);
        Payment p = payments.get(0);
        assertThat(p.getId()).isEqualTo("X1");
        assertThat(p.getDateTime()).isEqualTo(LocalDateTime.of(2026, 2, 20, 12, 0));
        assertThat(p.getSender()).isEqualTo("Bank A");
        assertThat(p.getReceiver()).isEqualTo("Bank B");
        assertThat(p.getValue()).isEqualTo(100.0);
    }

    @Test
    @DisplayName("Empty input — returns empty list")
    void emptyInput_shouldReturnEmptyList() {
        UploadResult result = new UploadResult();
        List<Payment> payments = csvParsingService.parse(toStream(""), result);

        assertThat(payments).isEmpty();
        assertThat(result.getErrors()).isEmpty();
    }

    private InputStream toStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
