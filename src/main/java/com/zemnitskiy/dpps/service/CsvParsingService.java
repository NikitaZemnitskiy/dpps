package com.zemnitskiy.dpps.service;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.model.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses CSV files into Payment objects with validation.
 * Pure parsing logic — no dependency on Ignite or cache.
 */
@Service
@Slf4j
public class CsvParsingService {

    /**
     * Parses CSV input stream into a list of valid Payment objects.
     * Invalid rows are skipped and their errors are recorded in the UploadResult.
     *
     * @param input  CSV input stream (header + data rows)
     * @param result UploadResult to accumulate validation errors
     * @return list of successfully parsed Payment objects
     */
    public List<Payment> parse(InputStream input, UploadResult result) {
        List<Payment> payments = new ArrayList<>();

        try (CSVReader reader = new CSVReaderBuilder(
                new InputStreamReader(input, StandardCharsets.UTF_8))
                .build()) {

            String[] header = reader.readNext();
            if (header == null) {
                return payments;
            }

            Map<String, Integer> columnIndex = new HashMap<>();
            for (int i = 0; i < header.length; i++) {
                columnIndex.put(header[i].trim().toLowerCase(), i);
            }

            int dateTimeIdx = resolveColumnIndex(columnIndex, "datetime");
            int senderIdx = resolveColumnIndex(columnIndex, "sender");
            int receiverIdx = resolveColumnIndex(columnIndex, "receiver");
            int valueIdx = resolveValueColumnIndex(columnIndex);
            int idIdx = resolveColumnIndex(columnIndex, "id");

            String[] row;
            while ((row = reader.readNext()) != null) {
                Payment payment = parseRow(row, dateTimeIdx, senderIdx, receiverIdx, valueIdx, idIdx, result);
                if (payment != null) {
                    payments.add(payment);
                }
            }

        } catch (Exception e) {
            log.error("Error processing CSV file", e);
            throw new RuntimeException("Failed to process CSV file: " + e.getMessage(), e);
        }

        return payments;
    }

    private Payment parseRow(String[] row, int dateTimeIdx, int senderIdx, int receiverIdx,
                             int valueIdx, int idIdx, UploadResult result) {
        String dateTime = getField(row, dateTimeIdx);
        String sender = getField(row, senderIdx);
        String receiver = getField(row, receiverIdx);
        String valueStr = getField(row, valueIdx);
        String id = getField(row, idIdx);

        boolean valid = true;

        if (isBlank(dateTime)) {
            result.incrementError("datetime");
            valid = false;
        }
        if (isBlank(sender)) {
            result.incrementError("sender");
            valid = false;
        }
        if (isBlank(receiver)) {
            result.incrementError("receiver");
            valid = false;
        }
        if (isBlank(id)) {
            result.incrementError("id");
            valid = false;
        }

        if (isBlank(valueStr)) {
            result.incrementError("value");
            valid = false;
        } else {
            try {
                double val = Double.parseDouble(valueStr.trim());
                if (val <= 0) {
                    result.incrementInvalidValue();
                    valid = false;
                }
            } catch (NumberFormatException e) {
                result.incrementInvalidValue();
                valid = false;
            }
        }

        if (!valid) return null;

        return new Payment(
                id.trim(),
                dateTime.trim(),
                sender.trim(),
                receiver.trim(),
                Double.parseDouble(valueStr.trim())
        );
    }

    private int resolveColumnIndex(Map<String, Integer> columnIndex, String name) {
        Integer idx = columnIndex.get(name);
        return idx != null ? idx : -1;
    }

    private int resolveValueColumnIndex(Map<String, Integer> columnIndex) {
        Integer idx = columnIndex.get("value");
        if (idx != null) return idx;
        idx = columnIndex.get("amount");
        return idx != null ? idx : -1;
    }

    private String getField(String[] row, int idx) {
        if (idx < 0 || idx >= row.length) return null;
        return row[idx];
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
