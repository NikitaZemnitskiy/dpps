package com.zemnitskiy.dpps.service;

import com.opencsv.bean.CsvToBean;
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
import java.util.function.Consumer;

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
     * Parses CSV input stream lazily, validating each row and passing valid payments
     * to the consumer one at a time. Never loads the entire file into memory.
     *
     * @param input    CSV input stream (header + data rows)
     * @param result   UploadResult to accumulate validation errors
     * @param consumer callback invoked for each successfully validated payment
     */
    public void parse(InputStream input, UploadResult result, Consumer<Payment> consumer) {
        int validCount = 0;

        try (var reader = new InputStreamReader(input, StandardCharsets.UTF_8)) {

            CsvToBean<CsvPaymentRecord> csvToBean = new CsvToBeanBuilder<CsvPaymentRecord>(reader)
                    .withType(CsvPaymentRecord.class)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withThrowExceptions(false)
                    .build();

            for (CsvPaymentRecord record : csvToBean) {
                Payment payment = toPayment(record, result);
                if (payment != null) {
                    consumer.accept(payment);
                    validCount++;
                }
            }

            csvToBean.getCapturedExceptions()
                    .forEach(e -> result.incrementInvalid(FIELD_CSV_ROW));

        } catch (CsvParsingException e) {
            throw e;
        } catch (Exception e) {
            log.error("CSV parsing failed: {}", e.getMessage(), e);
            throw new CsvParsingException("Failed to process CSV file: " + e.getMessage(), e);
        }

        log.info("CSV parsed: {} valid payments, {} error(s)",
                validCount, result.getErrors().values().stream().mapToInt(Integer::intValue).sum());
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

        String dateTime = null;
        if (isBlank(csvPayment.getDateTime())) {
            result.incrementMissing(FIELD_DATETIME);
            valid = false;
        } else {
            dateTime = csvPayment.getDateTime().trim();
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
