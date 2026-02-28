package com.zemnitskiy.dpps.service;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.filter.PaymentTimeRangeFilter;
import com.zemnitskiy.dpps.model.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.cache.Cache;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private static final Duration MAX_RANGE = Duration.ofDays(7);

    private final Ignite ignite;

    private IgniteCache<String, Payment> getCache() {
        return ignite.cache(IgniteConfig.PAYMENTS_CACHE);
    }

    public UploadResult uploadPayments(MultipartFile file) {
        UploadResult result = new UploadResult();

        try (CSVReader reader = new CSVReaderBuilder(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))
                .build()) {

            String[] header = reader.readNext();
            if (header == null) {
                return result;
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

            Map<String, Payment> batch = new HashMap<>();
            String[] row;

            while ((row = reader.readNext()) != null) {
                Payment payment = parseRow(row, dateTimeIdx, senderIdx, receiverIdx, valueIdx, idIdx, result);
                if (payment != null) {
                    batch.put(payment.getId(), payment);
                    if (batch.size() >= 1000) {
                        getCache().putAll(batch);
                        result.setSuccessfullyLoaded(result.getSuccessfullyLoaded() + batch.size());
                        batch.clear();
                    }
                }
            }
            log.info("Batch with " + batch.size() + " elements uploaded");
            if (!batch.isEmpty()) {
                getCache().putAll(batch);
                result.setSuccessfullyLoaded(result.getSuccessfullyLoaded() + batch.size());
            }

        } catch (Exception e) {
            log.error("Error processing CSV file", e);
            throw new RuntimeException("Failed to process CSV file: " + e.getMessage(), e);
        }

        return result;
    }

    public List<Payment> getPayments(String from, String to) {
        log.info("In getPayments " + ignite.name());
        validateTimeRange(from, to);

        ScanQuery<String, Payment> query = new ScanQuery<>(new PaymentTimeRangeFilter(from, to));

        try (QueryCursor<Cache.Entry<String, Payment>> cursor = getCache().query(query)) {
            return cursor.getAll().stream()
                    .map(Cache.Entry::getValue)
                    .collect(Collectors.toList());
        }
    }

    public DeleteResult deletePayments(String from, String to) {
        IgniteCache<String, Payment> cache = getCache();

        if (from != null && to != null) {
            ScanQuery<String, Payment> query = new ScanQuery<>(new PaymentTimeRangeFilter(from, to));

            Set<String> keysToDelete;
            try (QueryCursor<Cache.Entry<String, Payment>> cursor = cache.query(query)) {
                keysToDelete = cursor.getAll().stream()
                        .map(Cache.Entry::getKey)
                        .collect(Collectors.toSet());
            }

            if (!keysToDelete.isEmpty()) {
                cache.removeAll(keysToDelete);
            }
            return new DeleteResult(keysToDelete.size());
        } else {
            int size = cache.size();
            cache.clear();
            return new DeleteResult(size);
        }
    }

    private void validateTimeRange(String from, String to) {
        LocalDateTime fromDt = LocalDateTime.parse(from);
        LocalDateTime toDt = LocalDateTime.parse(to);

        if (fromDt.isAfter(toDt)) {
            throw new IllegalArgumentException("'from' must be before 'to'");
        }

        Duration duration = Duration.between(fromDt, toDt);
        if (duration.compareTo(MAX_RANGE) > 0) {
            throw new IllegalArgumentException("Time range must not exceed 1 week");
        }
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

    private String getField(String[] row, int idx) {
        if (idx < 0 || idx >= row.length) return null;
        return row[idx];
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
