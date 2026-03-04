package com.zemnitskiy.dpps.service;

import com.opencsv.bean.CsvToBeanBuilder;
import com.zemnitskiy.dpps.dto.CsvPaymentRecord;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.exception.CsvParsingException;
import com.zemnitskiy.dpps.model.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Parses CSV files into Payment objects with validation.
 * Pure parsing logic — no dependency on Ignite or cache.
 */
@Service
@Slf4j
public class CsvParsingService {

    private static final String FIELD_ID = "id";
    private static final String FIELD_SENDER = "sender";
    private static final String FIELD_RECEIVER = "receiver";
    private static final String FIELD_DATETIME = "datetime";
    private static final String FIELD_VALUE = "value";
    private static final String FIELD_CSV_ROW = "csv_row";

    /**
     * Parses CSV input stream into a list of valid Payment objects.
     * Invalid rows are skipped and their errors are recorded in the UploadResult.
     *
     * @param input  CSV input stream (header + data rows)
     * @param result UploadResult to accumulate validation errors
     * @return list of successfully parsed Payment objects
     */
    public List<Payment> parse(InputStream input, UploadResult result) {
        try (var reader = new InputStreamReader(input, StandardCharsets.UTF_8)) {

            var csvToBean = new CsvToBeanBuilder<CsvPaymentRecord>(reader)
                    .withType(CsvPaymentRecord.class)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withThrowExceptions(false)
                    .build();

            List<CsvPaymentRecord> records = csvToBean.parse();

            csvToBean.getCapturedExceptions()
                    .forEach(e -> result.incrementInvalid(FIELD_CSV_ROW));

            List<Payment> payments = records.stream()
                    .map(r -> toPayment(r, result))
                    .filter(Objects::nonNull)
                    .toList();

            log.info("CSV parsed: {} valid payments, {} error(s)",
                    payments.size(), result.getErrors().values().stream().mapToInt(Integer::intValue).sum());

            return payments;

        } catch (Exception e) {
            log.error("CSV parsing failed: {}", e.getMessage(), e);
            throw new CsvParsingException("Failed to process CSV file: " + e.getMessage(), e);
        }
    }

    private Payment toPayment(CsvPaymentRecord csvPayment, UploadResult result) {
        boolean valid = true;

        if (isBlank(csvPayment.getId())) {
            result.incrementMissing(FIELD_ID);
            valid = false;
        }
        if (isBlank(csvPayment.getSender())) {
            result.incrementMissing(FIELD_SENDER);
            valid = false;
        }
        if (isBlank(csvPayment.getReceiver())) {
            result.incrementMissing(FIELD_RECEIVER);
            valid = false;
        }

        LocalDateTime dateTime = null;
        if (isBlank(csvPayment.getDateTime())) {
            result.incrementMissing(FIELD_DATETIME);
            valid = false;
        } else {
            try {
                dateTime = LocalDateTime.parse(csvPayment.getDateTime().trim());
            } catch (DateTimeParseException e) {
                result.incrementInvalid(FIELD_DATETIME);
                valid = false;
            }
        }

        String amountStr = csvPayment.getAmountValue();
        double value = 0;
        if (isBlank(amountStr)) {
            result.incrementMissing(FIELD_VALUE);
            valid = false;
        } else {
            try {
                value = Double.parseDouble(amountStr.trim());
                if (value <= 0 || !Double.isFinite(value)) {
                    result.incrementInvalid(FIELD_VALUE);
                    valid = false;
                }
            } catch (NumberFormatException e) {
                result.incrementInvalid(FIELD_VALUE);
                valid = false;
            }
        }

        if (!valid) return null;

        return new Payment(
                csvPayment.getId().trim(),
                dateTime,
                csvPayment.getSender().trim(),
                csvPayment.getReceiver().trim(),
                value
        );
    }
}
