package com.zemnitskiy.dpps.service;

import com.zemnitskiy.dpps.compute.PaymentIdTransformer;
import com.zemnitskiy.dpps.compute.PaymentTimeRangeFilter;
import com.zemnitskiy.dpps.config.AppProperties;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.exception.ClusterUnavailableException;
import com.zemnitskiy.dpps.exception.CsvParsingException;
import com.zemnitskiy.dpps.exception.InsufficientStorageException;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.model.TimeRange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Payment storage on the Ignite cluster: CSV upload, retrieval by time range and deletion.
 * All cluster access goes through the key-value / scan API (no SQL).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private static final String REUPLOAD_HINT =
            "Re-uploading the file is safe: payments are stored by ID, so rows that were already saved are simply replaced.";

    private final Ignite ignite;
    private final CsvParsingService csvParsingService;
    private final ClusterMemoryGuard memoryGuard;
    private final AppProperties properties;

    /**
     * Parses the CSV and stores valid payments in batches. Rows are upserted by ID (the last row with a given ID
     * wins); invalid rows are skipped and reported.
     *
     * @throws CsvParsingException           if the file isn't a usable payments CSV; valid rows before the problem
     *                                       are stored and the message says how many
     * @throws InsufficientStorageException  if the cluster memory is nearly full (checked before every batch)
     * @throws ClusterUnavailableException   if writing to the cluster fails
     */
    public UploadResult uploadPayments(MultipartFile file) {
        long started = System.nanoTime();
        UploadReport report = new UploadReport();
        BatchWriter writer = new BatchWriter(cache(), properties.upload().batchSize(), report, memoryGuard);

        CsvParsingException csvError = null;
        try (InputStream input = file.getInputStream()) {
            writer.ensureCapacity();
            try {
                csvParsingService.parse(input, report, writer::add);
            } catch (CsvParsingException e) {
                csvError = e; // still store the valid rows that preceded the broken line
            }
            writer.flush();
        } catch (CacheException | IgniteException e) {
            log.error("Storing payments failed after {} rows", report.loaded(), e);
            throw new ClusterUnavailableException("Storing payments failed after " + payments(report.loaded())
                    + " had been saved, because the cluster is unavailable; try again later. " + REUPLOAD_HINT, e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read the uploaded file", e);
        }

        if (csvError != null) {
            throw new CsvParsingException(csvError.getMessage() + ". " + storedSoFar(report)
                    + " Fix the file and upload it again. " + REUPLOAD_HINT, csvError);
        }

        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        log.info("Upload finished: {} payments stored, {} rows rejected in {} ms ({} rows/s)",
                report.loaded(), report.rejected(), millis, millis == 0 ? report.loaded() : report.loaded() * 1000L / millis);
        return report.toResult();
    }

    /**
     * Streams the payments within {@code range} to {@code consumer}, entry by entry, without collecting them.
     * The order is unspecified.
     */
    public void streamPayments(TimeRange range, Consumer<Payment> consumer) {
        ScanQuery<String, Payment> query = new ScanQuery<>(new PaymentTimeRangeFilter(range));
        try (QueryCursor<Cache.Entry<String, Payment>> cursor = cache().query(query)) {
            for (Cache.Entry<String, Payment> entry : cursor) {
                consumer.accept(entry.getValue());
            }
        }
    }

    /**
     * Deletes the payments within {@code range}; an unbounded range clears the whole cache.
     *
     * @return number of deleted payments
     */
    public long deletePayments(TimeRange range) {
        IgniteCache<String, Payment> cache = cache();
        if (range.isUnbounded()) {
            long size = cache.sizeLong();
            cache.clear();
            log.info("Deleted all {} payments", size);
            return size;
        }

        int batchSize = properties.upload().batchSize();
        long deleted = 0;
        Set<String> batch = new HashSet<>();
        ScanQuery<String, Payment> query = new ScanQuery<>(new PaymentTimeRangeFilter(range));
        try (QueryCursor<String> ids = cache.query(query, new PaymentIdTransformer())) {
            for (String id : ids) {
                batch.add(id);
                if (batch.size() >= batchSize) {
                    cache.removeAll(batch);
                    deleted += batch.size();
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            cache.removeAll(batch);
            deleted += batch.size();
        }
        log.info("Deleted {} payments in {}", deleted, range);
        return deleted;
    }

    private IgniteCache<String, Payment> cache() {
        return ignite.cache(IgniteConfig.PAYMENTS_CACHE);
    }

    private static String storedSoFar(UploadReport report) {
        int stored = report.loaded();
        return stored == 0
                ? "No payments were stored."
                : "The " + payments(stored) + " before that point " + (stored == 1 ? "was" : "were") + " stored.";
    }

    private static String payments(int count) {
        return count == 1 ? "1 payment" : count + " payments";
    }

    /** Collects payments into batches and writes each batch with one {@code putAll}. */
    private static final class BatchWriter {

        private final IgniteCache<String, Payment> cache;
        private final int batchSize;
        private final UploadReport report;
        private final ClusterMemoryGuard memoryGuard;
        private final Map<String, Payment> pending = new HashMap<>();
        private int pendingRows;

        BatchWriter(IgniteCache<String, Payment> cache, int batchSize, UploadReport report, ClusterMemoryGuard memoryGuard) {
            this.cache = cache;
            this.batchSize = batchSize;
            this.report = report;
            this.memoryGuard = memoryGuard;
        }

        void add(Payment payment) {
            pending.put(payment.getId(), payment); // a later row with the same ID replaces the earlier one
            pendingRows++;
            if (pendingRows >= batchSize) {
                flush();
            }
        }

        void flush() {
            if (pendingRows == 0) {
                return;
            }
            ensureCapacity();
            cache.putAll(pending);
            report.addLoaded(pendingRows);
            log.debug("Stored a batch of {} payments ({} in total)", pendingRows, report.loaded());
            pending.clear();
            pendingRows = 0;
        }

        void ensureCapacity() {
            ClusterMemoryGuard.Status status = memoryGuard.status();
            if (status.nearlyFull()) {
                throw new InsufficientStorageException("The cluster is running out of memory: " + status.describe()
                        + ". The upload was stopped: " + payments(report.loaded()) + " stored, the rest of the"
                        + " file was not processed. Delete old payments or give the nodes more memory"
                        + " (IGNITE_DATA_REGION_MAX_SIZE), then upload the file again. " + REUPLOAD_HINT);
            }
        }
    }
}
