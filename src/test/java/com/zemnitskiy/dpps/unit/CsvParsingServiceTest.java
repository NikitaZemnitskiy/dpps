package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.exception.CsvParsingException;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.service.CsvParsingService;
import com.zemnitskiy.dpps.service.UploadReport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.zemnitskiy.dpps.support.TestData.HEADER;
import static com.zemnitskiy.dpps.support.TestData.at;
import static com.zemnitskiy.dpps.support.TestData.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** CsvParsingService: CSV syntax, header resolution and row validation. No Spring, no Ignite. */
@DisplayName("CsvParsingService")
@Tag("unit")
class CsvParsingServiceTest {

    private final CsvParsingService parser = new CsvParsingService();
    private final UploadReport report = new UploadReport();
    private final List<Payment> parsed = new ArrayList<>();

    private List<Payment> parse(String csv) {
        try {
            parser.parse(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), report, parsed::add);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return parsed;
    }

    private UploadResult result() {
        return report.toResult();
    }

    @Nested
    @DisplayName("Valid rows")
    class ValidRows {

        @Test
        @DisplayName("parses every row into an exact, typed payment")
        void validCsv_parsesAllRows() {
            List<Payment> payments = parse(HEADER + """
                    2026-02-20T12:00,Bank A,Bank B,100,1
                    2026-02-20T16:00:30,Bank B,Bank C,50.25,2
                    """);

            assertThat(payments).containsExactly(
                    payment("1", "2026-02-20T12:00", "Bank A", "Bank B", "100"),
                    payment("2", "2026-02-20T16:00:30", "Bank B", "Bank C", "50.25"));
            assertThat(result().rejected()).isZero();
            assertThat(result().errors()).isEmpty();
        }

        @Test
        @DisplayName("keeps the amount exactly as written, including its scale")
        void amount_keepsScale() {
            Payment payment = parse(HEADER + "2026-02-20T12:00,A,B,100.50,1\n").get(0);

            assertThat(payment.getValue()).isEqualTo(new BigDecimal("100.50"));
        }

        @Test
        @DisplayName("accepts a space instead of 'T' and fractional seconds")
        void dateTime_variants() {
            List<Payment> payments = parse(HEADER + """
                    2026-02-20 12:00,A,B,1,1
                    2026-02-20T12:00:30.250,A,B,1,2
                    """);

            assertThat(payments).extracting(Payment::getDateTime)
                    .containsExactly(at("2026-02-20T12:00"), at("2026-02-20T12:00:30.250"));
        }

        @Test
        @DisplayName("trims whitespace around values")
        void values_areTrimmed() {
            Payment payment = parse(HEADER + " 2026-02-20T12:00 , Bank A , Bank B , 100 , X1 \n").get(0);

            assertThat(payment).isEqualTo(payment("X1", "2026-02-20T12:00", "Bank A", "Bank B", "100"));
        }

        @Test
        @DisplayName("supports quoted fields with commas and keeps backslashes literally")
        void quotedFields() {
            Payment payment = parse(HEADER + "2026-02-20T12:00,\"Bank, Ltd\",C:\\bank,7,\"q1\"\n").get(0);

            assertThat(payment.getSender()).isEqualTo("Bank, Ltd");
            assertThat(payment.getReceiver()).isEqualTo("C:\\bank");
        }

        @Test
        @DisplayName("skips blank lines without counting them as errors")
        void blankLines_skipped() {
            List<Payment> payments = parse(HEADER + "\n2026-02-20T12:00,A,B,1,1\n\n   \n2026-02-20T13:00,A,B,2,2\n");

            assertThat(payments).hasSize(2);
            assertThat(result().rejected()).isZero();
        }

        @Test
        @DisplayName("accepts CRLF line endings")
        void crlfLineEndings() {
            List<Payment> payments = parse("DateTime,Sender,Receiver,Amount,ID\r\n2026-02-20T12:00,A,B,1,1\r\n");

            assertThat(payments).hasSize(1);
            assertThat(payments.get(0).getId()).isEqualTo("1");
        }

        @Test
        @DisplayName("tolerates empty trailing cells")
        void emptyTrailingCells_tolerated() {
            assertThat(parse(HEADER + "2026-02-20T12:00,A,B,1,1,,\n")).hasSize(1);
        }
    }

    @Nested
    @DisplayName("Header")
    class Header {

        @Test
        @DisplayName("is matched case-insensitively and in any column order; extra columns are ignored")
        void anyOrderAndCase() {
            Payment payment = parse("""
                    id,AMOUNT,receiver,Comment,SENDER,datetime
                    7,12.5,Bank B,ignored,Bank A,2026-02-20T12:00
                    """).get(0);

            assertThat(payment).isEqualTo(payment("7", "2026-02-20T12:00", "Bank A", "Bank B", "12.5"));
        }

        @Test
        @DisplayName("accepts 'Value' as the amount column")
        void valueAlias() {
            Payment payment = parse("DateTime,Sender,Receiver,Value,ID\n2026-02-20T12:00,A,B,999,1\n").get(0);

            assertThat(payment.getValue()).isEqualByComparingTo("999");
        }

        @Test
        @DisplayName("ignores a UTF-8 byte order mark (Excel exports)")
        void byteOrderMark() {
            assertThat(parse("\uFEFF" + HEADER + "2026-02-20T12:00,A,B,1,1\n")).hasSize(1);
        }

        @Test
        @DisplayName("missing required columns fail the whole file with a clear message")
        void missingColumns() {
            assertThatThrownBy(() -> parse("Date,From,To,Amount,ID\n2026-02-20T12:00,A,B,1,1\n"))
                    .isInstanceOf(CsvParsingException.class)
                    .hasMessageContaining("missing required column(s): DateTime, Sender, Receiver");
            assertThat(parsed).isEmpty();
        }

        @Test
        @DisplayName("header-only and empty input produce no payments and no errors")
        void headerOnlyAndEmpty() {
            assertThat(parse(HEADER)).isEmpty();
            assertThat(parse("")).isEmpty();
            assertThat(result()).isEqualTo(new UploadResult(0, 0, null));
        }
    }

    @Nested
    @DisplayName("Invalid rows are skipped and counted")
    class InvalidRows {

        @Test
        @DisplayName("missing fields are reported per field")
        void missingFields() {
            parse(HEADER + """
                    2026-02-20T12:00,,Bank B,100,1
                    ,Bank A,Bank B,100,2
                    2026-02-20T14:00,Bank A,Bank B,,3
                    2026-02-20T15:00,Bank A,,100,4
                    2026-02-20T16:00,Bank A,Bank B,100,
                    """);

            assertThat(parsed).isEmpty();
            assertThat(result().rejected()).isEqualTo(5);
            assertThat(result().errors()).containsOnly(
                    entry("missing_sender", 1), entry("missing_datetime", 1), entry("missing_value", 1),
                    entry("missing_receiver", 1), entry("missing_id", 1));
        }

        @Test
        @DisplayName("one row with several problems counts as one rejected row with several error codes")
        void severalProblemsInOneRow() {
            parse(HEADER + ",,Bank B,-1,1\n");

            assertThat(result().rejected()).isEqualTo(1);
            assertThat(result().errors()).containsOnly(
                    entry("missing_datetime", 1), entry("missing_sender", 1), entry("invalid_value", 1));
        }

        @ParameterizedTest(name = "\"{0}\"")
        @ValueSource(strings = {"2026-02-20", "20.02.2026 12:00", "2026-02-30T12:00", "2026-02-20T24:00",
                "2026-02-20T12:00Z", "2026-02-20T12:00+02:00", "yesterday", "2026"})
        @DisplayName("unparseable or impossible date-times are rejected as invalid_datetime")
        void invalidDateTime(String dateTime) {
            parse(HEADER + dateTime + ",A,B,1,1\n");

            assertThat(parsed).isEmpty();
            assertThat(result().errors()).containsOnly(entry("invalid_datetime", 1));
        }

        @ParameterizedTest(name = "\"{0}\"")
        @ValueSource(strings = {"-50", "0", "0.00", "abc", "1e3", "1,000", "NaN", "Infinity", "0x10", "100d", "--1",
                "123456789012345678901234567890123"})
        @DisplayName("non-positive, non-numeric, exponent or absurdly long amounts are rejected as invalid_value")
        void invalidAmount(String amount) {
            parse(HEADER + "2026-02-20T12:00,A,B,\"" + amount + "\",1\n");

            assertThat(parsed).isEmpty();
            assertThat(result().errors()).containsOnly(entry("invalid_value", 1));
        }

        @Test
        @DisplayName("IDs and bank names longer than 255 characters are rejected")
        void tooLongText() {
            String longName = "B".repeat(256);
            parse(HEADER + "2026-02-20T12:00," + longName + ",Bank B,1,1\n");

            assertThat(result().errors()).containsOnly(entry("invalid_sender", 1));
        }

        @Test
        @DisplayName("bank names containing '->' are rejected (it separates sender and receiver in BY_CONNECTION keys)")
        void arrowInBankName() {
            parse(HEADER + "2026-02-20T12:00,Bank A->B,Bank C,1,1\n2026-02-20T12:00,Bank A,Bank->C,1,2\n");

            assertThat(parsed).isEmpty();
            assertThat(result().errors()).containsOnly(entry("invalid_sender", 1), entry("invalid_receiver", 1));
        }

        @Test
        @DisplayName("rows with too few or too many columns are rejected as invalid_csv_row")
        void wrongColumnCount() {
            parse(HEADER + """
                    2026-02-20T12:00,Bank A
                    2026-02-20T12:00,A,B,1,1,unexpected
                    2026-02-20T12:00,A,B,1,3
                    """);

            assertThat(parsed).extracting(Payment::getId).containsExactly("3");
            assertThat(result().errors()).containsOnly(entry("invalid_csv_row", 2));
        }
    }

    @Nested
    @DisplayName("Broken CSV syntax")
    class BrokenSyntax {

        @Test
        @DisplayName("an unterminated quote fails with the line number; earlier rows were already delivered")
        void unterminatedQuote() {
            String csv = HEADER + """
                    2026-02-20T12:00,A,B,1,1
                    2026-02-20T12:00,A,B,1,2
                    2026-02-20T12:00,"Bank A,B,1,3
                    2026-02-20T12:00,A,B,1,4
                    """;

            assertThatThrownBy(() -> parse(csv))
                    .isInstanceOf(CsvParsingException.class)
                    .hasMessage("Malformed CSV at line 4: unterminated quoted field");
            assertThat(parsed).extracting(Payment::getId).containsExactly("1", "2");
        }

        @Test
        @DisplayName("an absurdly long line fails fast instead of being read into memory")
        void lineTooLong() {
            String csv = HEADER + "2026-02-20T12:00,A,B,1,1\n" + "x".repeat(70_000) + "\n2026-02-20T12:00,A,B,1,3\n";

            assertThatThrownBy(() -> parse(csv))
                    .isInstanceOf(CsvParsingException.class)
                    .hasMessage("Malformed CSV at line 3: the line is longer than 65536 characters");
            assertThat(parsed).extracting(Payment::getId).containsExactly("1");
        }

        @Test
        @DisplayName("an unterminated quote on the last line fails too")
        void unterminatedQuoteAtEndOfFile() {
            assertThatThrownBy(() -> parse(HEADER + "2026-02-20T12:00,\"A,B,1,1\n"))
                    .isInstanceOf(CsvParsingException.class)
                    .hasMessageContaining("unterminated quoted field");
        }
    }

    private static Map.Entry<String, Integer> entry(String code, int count) {
        return Map.entry(code, count);
    }
}
