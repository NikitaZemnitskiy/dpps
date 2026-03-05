package com.zemnitskiy.dpps.service;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.exception.PaymentProcessingException;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Core service for payment storage operations using the Ignite distributed cache.
 * Handles CSV upload (with batching), retrieval by time range, and deletion.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private static final int BATCH_SIZE = 1000;
    private static final Duration MAX_RANGE = Duration.ofDays(7);

    private final Ignite ignite;
    private final CsvParsingService csvParsingService;

    /**
     * Parses a CSV file and stores payments in the distributed cache in batches.
     * Uses lazy iteration — never loads the entire file into memory.
     *
     * @param file uploaded CSV file
     * @return result with loaded/new/updated counts and per-field error counts
     * @throws PaymentProcessingException if an I/O or unexpected error occurs
     */
    public UploadResult uploadPayments(MultipartFile file) {
        log.info("Starting payment upload, file size: {} bytes", file.getSize());
        UploadResult result = new UploadResult();

        try {
            Map<String, Payment> batch = new HashMap<>();

            csvParsingService.parse(file.getInputStream(), result, payment -> {
                batch.put(payment.getId(), payment);
                if (batch.size() >= BATCH_SIZE) {
                    saveBatch(batch, result);
                    batch.clear();
                }
            });

            if (!batch.isEmpty()) {
                saveBatch(batch, result);
            }
        } catch (Exception e) {
            log.error("Error uploading payments", e);
            throw new PaymentProcessingException("Failed to upload payments: " + e.getMessage(), e);
        }

        log.info("Upload complete: {} loaded, {} errors",
                result.getSuccessfullyLoaded(), result.getErrors().size());
        return result;
    }

    /**
     * Deletes payments from the cache. If a time range is provided, uses a ScanQuery
     * to find matching keys in batches; otherwise clears the entire cache.
     *
     * @param from start of the range, or {@code null} to delete all
     * @param to   end of the range, or {@code null} to delete all
     * @return result with the number of deleted records
     */
    public DeleteResult deletePayments(String from, String to) {
        IgniteCache<String, Payment> cache = getCache();

        if (from != null && to != null) {
            ScanQuery<String, Payment> query = new ScanQuery<>(new PaymentTimeRangeFilter(from, to));

            int total = 0;
            Set<String> batch = new HashSet<>();

            try (QueryCursor<Cache.Entry<String, Payment>> cursor = cache.query(query)) {
                for (Cache.Entry<String, Payment> entry : cursor) {
                    batch.add(entry.getKey());
                    if (batch.size() >= BATCH_SIZE) {
                        cache.removeAll(batch);
                        total += batch.size();
                        batch.clear();
                    }
                }
            }

            if (!batch.isEmpty()) {
                cache.removeAll(batch);
                total += batch.size();
            }

            log.info("Deleted {} payments in range [{} — {}]", total, from, to);
            return new DeleteResult(total);
        } else {
            int size = cache.size();
            cache.clear();
            log.info("Cleared all {} payments from cache", size);
            return new DeleteResult(size);
        }
    }

    /**
     * Streams payments within the given time range to a consumer callback.
     * Iterates the Ignite cursor lazily — never accumulates all entries in memory.
     *
     * @param from     start of the range (ISO 8601)
     * @param to       end of the range (ISO 8601)
     * @param consumer callback invoked for each matching payment
     */
    public void streamPayments(String from, String to, Consumer<Payment> consumer) {
        ScanQuery<String, Payment> query = new ScanQuery<>(new PaymentTimeRangeFilter(from, to));

        try (QueryCursor<Cache.Entry<String, Payment>> cursor = getCache().query(query)) {
            for (Cache.Entry<String, Payment> entry : cursor) {
                consumer.accept(entry.getValue());
            }
        }
    }

    private void saveBatch(Map<String, Payment> batch, UploadResult result) {
        IgniteCache<String, Payment> cache = getCache();
        cache.putAll(batch);
        result.setSuccessfullyLoaded(result.getSuccessfullyLoaded() + batch.size());
        log.info("Batch uploaded: {} payments", batch.size());
    }

    private IgniteCache<String, Payment> getCache() {
        return ignite.cache(IgniteConfig.PAYMENTS_CACHE);
    }

    /**
     * Validates that the time range is well-formed, from &lt; to, and does not exceed 1 week.
     * Must be called before streaming to ensure errors produce proper 400 responses.
     *
     * @throws IllegalArgumentException  if from &gt; to or range exceeds 1 week
     * @throws java.time.format.DateTimeParseException if from/to cannot be parsed
     */
    public void validateTimeRange(String from, String to) {
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
}
