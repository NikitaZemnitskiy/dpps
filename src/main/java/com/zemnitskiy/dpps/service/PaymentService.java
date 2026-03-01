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
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private static final int BATCH_SIZE = 1000;
    private static final Duration MAX_RANGE = Duration.ofDays(7);

    private final Ignite ignite;
    private final CsvParsingService csvParsingService;

    private IgniteCache<String, Payment> getCache() {
        return ignite.cache(IgniteConfig.PAYMENTS_CACHE);
    }

    public UploadResult uploadPayments(MultipartFile file) {
        UploadResult result = new UploadResult();

        try {
            List<Payment> payments = csvParsingService.parse(file.getInputStream(), result);

            Map<String, Payment> batch = new HashMap<>();
            for (Payment payment : payments) {
                batch.put(payment.getId(), payment);
                if (batch.size() >= BATCH_SIZE) {
                    getCache().putAll(batch);
                    result.setSuccessfullyLoaded(result.getSuccessfullyLoaded() + batch.size());
                    log.info("Batch with {} elements uploaded", batch.size());
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                getCache().putAll(batch);
                result.setSuccessfullyLoaded(result.getSuccessfullyLoaded() + batch.size());
                log.info("Batch with {} elements uploaded", batch.size());
            }
        } catch (Exception e) {
            log.error("Error uploading payments", e);
            throw new PaymentProcessingException("Failed to upload payments: " + e.getMessage(), e);
        }

        return result;
    }

    public List<Payment> getPayments(String from, String to) {
        log.info("In getPayments {}", ignite.name());
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
}
