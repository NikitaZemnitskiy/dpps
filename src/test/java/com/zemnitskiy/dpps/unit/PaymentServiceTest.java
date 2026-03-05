package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.exception.PaymentProcessingException;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.service.CsvParsingService;
import com.zemnitskiy.dpps.service.PaymentService;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.multipart.MultipartFile;

import javax.cache.Cache;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for PaymentService — core service for payment storage operations.
 * Tests upload, delete, stream, and validation logic.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentService — Unit Tests")
class PaymentServiceTest {

    @Mock
    private Ignite ignite;

    @Mock
    private IgniteCache<String, Payment> cache;

    @Mock
    private CsvParsingService csvParsingService;

    @Mock
    private MultipartFile multipartFile;

    @Mock
    private QueryCursor<Cache.Entry<String, Payment>> queryCursor;

    @InjectMocks
    private PaymentService paymentService;

    @Captor
    private ArgumentCaptor<Map<String, Payment>> batchCaptor;

    private void setupCacheMock() {
        doReturn(cache).when(ignite).cache(IgniteConfig.PAYMENTS_CACHE);
    }

    @Nested
    @DisplayName("Upload Payments Tests")
    class UploadPaymentsTests {

        @Test
        @DisplayName("Upload valid CSV — stores payments and returns correct counts")
        void uploadValidCsv_shouldStorePaymentsAndReturnCounts() throws IOException {
            setupCacheMock();
            when(multipartFile.getSize()).thenReturn(1000L);
            when(multipartFile.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

            List<Payment> payments = List.of(
                    new Payment("1", "2026-02-20T10:00:00", "BankA", "BankB", 100.0),
                    new Payment("2", "2026-02-20T11:00:00", "BankA", "BankC", 200.0)
            );

            doAnswer(invocation -> {
                invocation.getArgument(1);
                Consumer<Payment> consumer = invocation.getArgument(2);
                payments.forEach(consumer);
                return null;
            }).when(csvParsingService).parse(any(InputStream.class), any(UploadResult.class), any());

            UploadResult result = paymentService.uploadPayments(multipartFile);

            verify(cache).putAll(batchCaptor.capture());
            Map<String, Payment> savedBatch = batchCaptor.getValue();

            assertThat(savedBatch).hasSize(2);
            assertThat(savedBatch).containsKeys("1", "2");
            assertThat(result.getSuccessfullyLoaded()).isEqualTo(2);
        }

        @Test
        @DisplayName("Upload triggers batch save when batch size reached")
        void uploadLargeFile_shouldTriggerMultipleBatches() throws IOException {
            setupCacheMock();
            when(multipartFile.getSize()).thenReturn(100000L);
            when(multipartFile.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

            // Generate 2500 payments (should trigger 3 batches: 1000, 1000, 500)
            List<Payment> payments = new ArrayList<>();
            for (int i = 0; i < 2500; i++) {
                payments.add(new Payment(String.valueOf(i), "2026-02-20T10:00:00", "BankA", "BankB", 100.0));
            }

            doAnswer(invocation -> {
                invocation.getArgument(1);
                Consumer<Payment> consumer = invocation.getArgument(2);
                payments.forEach(consumer);
                return null;
            }).when(csvParsingService).parse(any(InputStream.class), any(UploadResult.class), any());

            UploadResult result = paymentService.uploadPayments(multipartFile);

            verify(cache, times(3)).putAll(anyMap());
            assertThat(result.getSuccessfullyLoaded()).isEqualTo(2500);
        }

        @Test
        @DisplayName("Upload empty file — returns zero counts")
        void uploadEmptyFile_shouldReturnZeroCounts() throws IOException {
            when(multipartFile.getSize()).thenReturn(0L);
            when(multipartFile.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

            doAnswer(invocation -> null)
                    .when(csvParsingService).parse(any(InputStream.class), any(UploadResult.class), any());

            UploadResult result = paymentService.uploadPayments(multipartFile);

            assertThat(result.getSuccessfullyLoaded()).isZero();
        }

        @Test
        @DisplayName("Upload with IO error — throws PaymentProcessingException")
        void uploadWithIoError_shouldThrowException() throws IOException {
            when(multipartFile.getSize()).thenReturn(1000L);
            when(multipartFile.getInputStream()).thenThrow(new IOException("Read error"));

            assertThatThrownBy(() -> paymentService.uploadPayments(multipartFile))
                    .isInstanceOf(PaymentProcessingException.class)
                    .hasMessageContaining("Failed to upload payments");
        }

        @Test
        @DisplayName("Upload with parsing errors — returns error counts")
        void uploadWithParsingErrors_shouldReturnErrorCounts() throws IOException {
            setupCacheMock();
            when(multipartFile.getSize()).thenReturn(1000L);
            when(multipartFile.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

            doAnswer(invocation -> {
                UploadResult result = invocation.getArgument(1);
                Consumer<Payment> consumer = invocation.getArgument(2);

                // Simulate 1 valid payment
                consumer.accept(new Payment("1", "2026-02-20T10:00:00", "BankA", "BankB", 100.0));

                // Simulate parsing errors
                result.incrementMissing("sender");
                result.incrementMissing("sender");
                result.incrementInvalid("value");

                return null;
            }).when(csvParsingService).parse(any(InputStream.class), any(UploadResult.class), any());

            UploadResult result = paymentService.uploadPayments(multipartFile);

            assertThat(result.getSuccessfullyLoaded()).isEqualTo(1);
            assertThat(result.getErrors())
                    .containsEntry("missing_sender", 2)
                    .containsEntry("invalid_value", 1);
        }
    }

    @Nested
    @DisplayName("Delete Payments Tests")
    class DeletePaymentsTests {

        @Test
        @DisplayName("Delete all — clears cache and returns count")
        void deleteAll_shouldClearCacheAndReturnCount() {
            setupCacheMock();
            when(cache.size()).thenReturn(500);

            DeleteResult result = paymentService.deletePayments(null, null);

            verify(cache).clear();
            assertThat(result.deletedCount()).isEqualTo(500);
        }

        @Test
        @DisplayName("Delete by time range — removes matching payments")
        @SuppressWarnings("unchecked")
        void deleteByTimeRange_shouldRemoveMatchingPayments() {
            setupCacheMock();
            List<Payment> matchingPayments = List.of(
                    new Payment("1", "2026-02-20T10:00:00", "BankA", "BankB", 100.0),
                    new Payment("2", "2026-02-20T11:00:00", "BankA", "BankC", 200.0)
            );

            List<Cache.Entry<String, Payment>> entries = matchingPayments.stream()
                    .map(p -> createCacheEntry(p.getId(), p))
                    .toList();

            doReturn(queryCursor).when(cache).query(any(ScanQuery.class));
            when(queryCursor.iterator()).thenReturn(entries.iterator());

            DeleteResult result = paymentService.deletePayments("2026-02-20T00:00:00", "2026-02-20T23:59:59");

            verify(cache).removeAll(anySet());
            assertThat(result.deletedCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("Delete by time range with no matches — returns zero")
        @SuppressWarnings("unchecked")
        void deleteByTimeRangeNoMatches_shouldReturnZero() {
            setupCacheMock();
            doReturn(queryCursor).when(cache).query(any(ScanQuery.class));
            when(queryCursor.iterator()).thenReturn(Collections.emptyIterator());

            DeleteResult result = paymentService.deletePayments("2026-02-20T00:00:00", "2026-02-20T23:59:59");

            verify(cache, never()).removeAll(anySet());
            assertThat(result.deletedCount()).isZero();
        }

        @Test
        @DisplayName("Delete by time range — processes in batches")
        @SuppressWarnings("unchecked")
        void deleteByTimeRange_shouldProcessInBatches() {
            setupCacheMock();
            // Generate 2500 payments (should trigger 3 batches)
            List<Cache.Entry<String, Payment>> entries = new ArrayList<>();
            for (int i = 0; i < 2500; i++) {
                Payment p = new Payment(String.valueOf(i), "2026-02-20T10:00:00", "BankA", "BankB", 100.0);
                entries.add(createCacheEntry(p.getId(), p));
            }

            doReturn(queryCursor).when(cache).query(any(ScanQuery.class));
            when(queryCursor.iterator()).thenReturn(entries.iterator());

            DeleteResult result = paymentService.deletePayments("2026-02-20T00:00:00", "2026-02-20T23:59:59");

            verify(cache, times(3)).removeAll(anySet());
            assertThat(result.deletedCount()).isEqualTo(2500);
        }
    }

    @Nested
    @DisplayName("Stream Payments Tests")
    class StreamPaymentsTests {

        @Test
        @DisplayName("Stream payments — invokes consumer for each match")
        @SuppressWarnings("unchecked")
        void streamPayments_shouldInvokeConsumerForEachMatch() {
            setupCacheMock();
            List<Payment> payments = List.of(
                    new Payment("1", "2026-02-20T10:00:00", "BankA", "BankB", 100.0),
                    new Payment("2", "2026-02-20T11:00:00", "BankA", "BankC", 200.0),
                    new Payment("3", "2026-02-20T12:00:00", "BankB", "BankA", 150.0)
            );

            List<Cache.Entry<String, Payment>> entries = payments.stream()
                    .map(p -> createCacheEntry(p.getId(), p))
                    .toList();

            doReturn(queryCursor).when(cache).query(any(ScanQuery.class));
            when(queryCursor.iterator()).thenReturn(entries.iterator());

            List<Payment> streamed = new ArrayList<>();
            paymentService.streamPayments("2026-02-20T00:00:00", "2026-02-20T23:59:59", streamed::add);

            assertThat(streamed).hasSize(3);
            assertThat(streamed).extracting(Payment::getId).containsExactly("1", "2", "3");
        }

        @Test
        @DisplayName("Stream with no matches — consumer not invoked")
        @SuppressWarnings("unchecked")
        void streamWithNoMatches_shouldNotInvokeConsumer() {
            setupCacheMock();
            doReturn(queryCursor).when(cache).query(any(ScanQuery.class));
            when(queryCursor.iterator()).thenReturn(Collections.emptyIterator());

            List<Payment> streamed = new ArrayList<>();
            paymentService.streamPayments("2026-02-20T00:00:00", "2026-02-20T23:59:59", streamed::add);

            assertThat(streamed).isEmpty();
        }
    }

    @Nested
    @DisplayName("Validate Time Range Tests")
    class ValidateTimeRangeTests {

        @Test
        @DisplayName("Valid range within 1 week — no exception")
        void validRangeWithinOneWeek_shouldNotThrow() {
            paymentService.validateTimeRange("2026-02-20T00:00:00", "2026-02-25T00:00:00");
            // No exception means success
        }

        @Test
        @DisplayName("Range exactly 1 week — no exception")
        void rangeExactlyOneWeek_shouldNotThrow() {
            paymentService.validateTimeRange("2026-02-20T00:00:00", "2026-02-27T00:00:00");
            // No exception means success
        }

        @Test
        @DisplayName("Range exceeds 1 week — throws IllegalArgumentException")
        void rangeExceedsOneWeek_shouldThrowException() {
            assertThatThrownBy(() -> paymentService.validateTimeRange("2026-02-20T00:00:00", "2026-02-28T00:00:01"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must not exceed 1 week");
        }

        @Test
        @DisplayName("From after to — throws IllegalArgumentException")
        void fromAfterTo_shouldThrowException() {
            assertThatThrownBy(() -> paymentService.validateTimeRange("2026-02-25T00:00:00", "2026-02-20T00:00:00"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'from' must be before 'to'");
        }

        @Test
        @DisplayName("Invalid date format — throws DateTimeParseException")
        void invalidDateFormat_shouldThrowException() {
            assertThatThrownBy(() -> paymentService.validateTimeRange("invalid-date", "2026-02-20T00:00:00"))
                    .isInstanceOf(DateTimeParseException.class);
        }

        @Test
        @DisplayName("Same from and to — valid (zero duration)")
        void sameFromAndTo_shouldNotThrow() {
            paymentService.validateTimeRange("2026-02-20T12:00:00", "2026-02-20T12:00:00");
            // No exception means success
        }
    }

    private Cache.Entry<String, Payment> createCacheEntry(String key, Payment value) {
        return new Cache.Entry<>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Payment getValue() {
                return value;
            }

            @Override
            public <T> T unwrap(Class<T> clazz) {
                return null;
            }
        };
    }
}
