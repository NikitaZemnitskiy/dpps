package com.zemnitskiy.dpps.service;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.exceptions.CsvMalformedLineException;
import com.opencsv.exceptions.CsvMultilineLimitBrokenException;
import com.opencsv.exceptions.CsvValidationException;
import com.zemnitskiy.dpps.exception.CsvParsingException;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.util.DateTimes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Streams a payments CSV row by row, validates every row and hands valid payments to a consumer.
 * <p>
 * Expected header (any column order, case-insensitive, extra columns ignored):
 * {@code DateTime, Sender, Receiver, Amount, ID} - {@code Value} is accepted instead of {@code Amount}.
 * Invalid rows are skipped and counted; only a file that isn't a payments CSV at all, or whose CSV syntax is
 * broken, fails the upload. Pure parsing logic - no dependency on Ignite.
 */
@Service
public class CsvParsingService {

    static final String INVALID_ROW = "invalid_csv_row";

    /** Longest ID or bank name accepted; protects the cluster from absurd values. */
    static final int MAX_TEXT_LENGTH = 255;
    private static final int MAX_AMOUNT_LENGTH = 32;
    /** Plain decimal notation only: no sign other than '+', no exponent, no thousands separators. */
    private static final Pattern PLAIN_DECIMAL = Pattern.compile("\\+?(\\d+(\\.\\d*)?|\\.\\d+)");
    private static final String BYTE_ORDER_MARK = "﻿";

    /**
     * Parses {@code input} lazily - the file is never held in memory as a whole.
     *
     * @param input    CSV content (UTF-8): a header row followed by data rows
     * @param report   receives the rejected rows and their validation errors
     * @param consumer called for every valid payment, in file order
     * @throws CsvParsingException if required columns are missing from the header or the CSV syntax is broken;
     *                             rows before the problem have already been passed to {@code consumer}
     * @throws IOException         if reading {@code input} fails
     */
    public void parse(InputStream input, UploadReport report, Consumer<Payment> consumer) throws IOException {
        CSVReader reader = new CSVReaderBuilder(new InputStreamReader(input, StandardCharsets.UTF_8))
                // RFC 4180 quoting; a backslash is an ordinary character.
                .withCSVParser(new CSVParserBuilder().withEscapeChar(ICSVParser.NULL_CHARACTER).build())
                // Payment fields never span lines; an unterminated quote must fail fast instead of
                // swallowing the rest of the file into a single field.
                .withMultilineLimit(1)
                .build();
        try (reader) {
            String[] header = reader.readNext();
            if (header == null) {
                return;
            }
            Columns columns = Columns.of(header);
            String[] row;
            while ((row = reader.readNext()) != null) {
                if (isBlank(row)) {
                    continue;
                }
                Payment payment = toPayment(row, columns, report);
                if (payment != null) {
                    consumer.accept(payment);
                }
            }
        } catch (CsvMalformedLineException e) {
            throw new CsvParsingException("Malformed CSV at line " + e.getLineNumber() + ": unterminated quoted field", e);
        } catch (CsvMultilineLimitBrokenException e) {
            throw new CsvParsingException("Malformed CSV at line " + e.getRow() + ": unterminated quoted field", e);
        } catch (CsvValidationException e) {
            throw new CsvParsingException("Malformed CSV at line " + reader.getLinesRead() + ": " + e.getMessage(), e);
        }
    }

    private static Payment toPayment(String[] row, Columns columns, UploadReport report) {
        if (!columns.fits(row)) {
            report.reject(List.of(INVALID_ROW));
            return null;
        }
        List<String> errors = new ArrayList<>(2);
        String id = text(row[columns.id()], "id", errors);
        String sender = text(row[columns.sender()], "sender", errors);
        String receiver = text(row[columns.receiver()], "receiver", errors);
        LocalDateTime dateTime = dateTime(row[columns.dateTime()], errors);
        BigDecimal value = amount(row[columns.amount()], errors);
        if (!errors.isEmpty()) {
            report.reject(errors);
            return null;
        }
        return new Payment(id, dateTime, sender, receiver, value);
    }

    private static String text(String raw, String field, List<String> errors) {
        String value = raw.strip();
        if (value.isEmpty()) {
            errors.add("missing_" + field);
            return null;
        }
        if (value.length() > MAX_TEXT_LENGTH) {
            errors.add("invalid_" + field);
            return null;
        }
        return value;
    }

    private static LocalDateTime dateTime(String raw, List<String> errors) {
        if (raw.isBlank()) {
            errors.add("missing_datetime");
            return null;
        }
        try {
            return DateTimes.parse(raw);
        } catch (DateTimeParseException e) {
            errors.add("invalid_datetime");
            return null;
        }
    }

    private static BigDecimal amount(String raw, List<String> errors) {
        String value = raw.strip();
        if (value.isEmpty()) {
            errors.add("missing_value");
            return null;
        }
        if (value.length() > MAX_AMOUNT_LENGTH || !PLAIN_DECIMAL.matcher(value).matches()) {
            errors.add("invalid_value");
            return null;
        }
        BigDecimal amount = new BigDecimal(value);
        if (amount.signum() <= 0) {
            errors.add("invalid_value");
            return null;
        }
        return amount;
    }

    private static boolean isBlank(String[] row) {
        for (String cell : row) {
            if (!cell.isBlank()) {
                return false;
            }
        }
        return true;
    }

    /** Positions of the required columns, resolved from the header row. */
    private record Columns(int dateTime, int sender, int receiver, int amount, int id, int width) {

        static Columns of(String[] header) {
            Map<String, Integer> positions = new HashMap<>();
            for (int i = 0; i < header.length; i++) {
                String name = header[i];
                if (i == 0 && name.startsWith(BYTE_ORDER_MARK)) {
                    name = name.substring(1);
                }
                positions.putIfAbsent(name.strip().toLowerCase(Locale.ROOT), i);
            }
            List<String> missing = new ArrayList<>();
            int dateTime = position(positions, "datetime", "DateTime", missing);
            int sender = position(positions, "sender", "Sender", missing);
            int receiver = position(positions, "receiver", "Receiver", missing);
            int amount = positions.containsKey("amount")
                    ? positions.get("amount")
                    : position(positions, "value", "Amount", missing);
            int id = position(positions, "id", "ID", missing);
            if (!missing.isEmpty()) {
                throw new CsvParsingException("CSV header is missing required column(s): " + String.join(", ", missing)
                        + ". Expected columns: DateTime, Sender, Receiver, Amount (or Value), ID");
            }
            return new Columns(dateTime, sender, receiver, amount, id, header.length);
        }

        private static int position(Map<String, Integer> positions, String key, String label, List<String> missing) {
            Integer position = positions.get(key);
            if (position == null) {
                missing.add(label);
                return -1;
            }
            return position;
        }

        /** A row must have a cell for every header column; trailing extra cells are tolerated only if empty. */
        boolean fits(String[] row) {
            if (row.length < width) {
                return false;
            }
            for (int i = width; i < row.length; i++) {
                if (!row[i].isBlank()) {
                    return false;
                }
            }
            return true;
        }
    }
}
