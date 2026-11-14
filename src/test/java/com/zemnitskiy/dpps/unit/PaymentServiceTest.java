package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.compute.PaymentIdTransformer;
import com.zemnitskiy.dpps.config.AppProperties;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.exception.ClusterUnavailableException;
import com.zemnitskiy.dpps.exception.CsvParsingException;
import com.zemnitskiy.dpps.exception.InsufficientStorageException;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.service.ClusterMemoryGuard;
import com.zemnitskiy.dpps.service.CsvParsingService;
import com.zemnitskiy.dpps.service.PaymentService;
import com.zemnitskiy.dpps.support.ListQueryCursor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.web.MockMultipartFile;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.zemnitskiy.dpps.support.TestData.HEADER;
import static com.zemnitskiy.dpps.support.TestData.at;
import static com.zemnitskiy.dpps.support.TestData.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** PaymentService with the real CSV parser and a mocked cache. */
@DisplayName("PaymentService")
@Tag("unit")
class PaymentServiceTest {

    private static final int BATCH_SIZE = 2;

    private final IgniteCache<String, Payment> cache = mockCache();
    private final ClusterMemoryGuard memoryGuard = mock(ClusterMemoryGuard.class);
    private final List<Map<String, Payment>> writtenBatches = new ArrayList<>();
    private PaymentService service;

    @SuppressWarnings("unchecked")
    private static IgniteCache<String, Payment> mockCache() {
        return mock(IgniteCache.class);
    }

    @BeforeEach
    void setUp() {
        Ignite ignite = mock(Ignite.class);
        doReturn(cache).when(ignite).cache(IgniteConfig.PAYMENTS_CACHE);
        doAnswer(invocation -> writtenBatches.add(new HashMap<>(invocation.<Map<String, Payment>>getArgument(0))))
                .when(cache).putAll(anyMap());
        memoryHas(0.10);
        AppProperties properties = new AppProperties(
                new AppProperties.Upload(BATCH_SIZE, 0.85, Duration.ofSeconds(1)),
                new AppProperties.Statistics(Duration.ofSeconds(60), 3, Duration.ZERO));
        service = new PaymentService(ignite, new CsvParsingService(), memoryGuard, properties);
    }

    private void memoryHas(double usage) {
        when(memoryGuard.status()).thenReturn(new ClusterMemoryGuard.Status(usage, 0.85));
    }

    private static MockMultipartFile csv(String content) {
        return new MockMultipartFile("file", "payments.csv", "text/csv", content.getBytes());
    }

    @Nested
    @DisplayName("Upload")
    class Upload {

        @Test
        @DisplayName("stores valid rows in batches and reports rejected ones")
        void storesInBatches() {
            UploadResult result = service.uploadPayments(csv(HEADER + """
                    2026-02-20T10:00,A,B,100,1
                    2026-02-20T11:00,A,C,200,2
                    2026-02-20T12:00,,C,200,3
                    2026-02-20T13:00,B,C,300,4
                    """));

            assertThat(writtenBatches).hasSize(2);
            assertThat(writtenBatches.get(0)).containsOnlyKeys("1", "2");
            assertThat(writtenBatches.get(1)).containsOnlyKeys("4");
            assertThat(writtenBatches.get(0).get("1")).isEqualTo(payment("1", "2026-02-20T10:00", "A", "B", "100"));
            assertThat(result).isEqualTo(new UploadResult(3, 1, Map.of("missing_sender", 1)));
        }

        @Test
        @DisplayName("counts every stored row, and the last row with a repeated ID wins")
        void repeatedIds() {
            UploadResult result = service.uploadPayments(csv(HEADER + """
                    2026-02-20T10:00,A,B,1,same
                    2026-02-20T11:00,A,B,2,same
                    2026-02-20T12:00,A,B,3,same
                    """));

            assertThat(result.successfullyLoaded()).isEqualTo(3);
            assertThat(writtenBatches.get(0)).containsOnlyKeys("same");
            assertThat(writtenBatches.get(0).get("same").getValue()).isEqualByComparingTo("2");
            assertThat(writtenBatches.get(1).get("same").getValue()).isEqualByComparingTo("3");
        }

        @Test
        @DisplayName("broken CSV: valid rows before the problem are stored and the error says how many")
        void brokenCsvStoresPrecedingRows() {
            String content = HEADER + """
                    2026-02-20T10:00,A,B,1,1
                    2026-02-20T11:00,A,B,1,2
                    2026-02-20T12:00,A,B,1,3
                    2026-02-20T13:00,"A,B,1,4
                    2026-02-20T14:00,A,B,1,5
                    """;

            assertThatThrownBy(() -> service.uploadPayments(csv(content)))
                    .isInstanceOf(CsvParsingException.class)
                    .hasMessageContaining("Malformed CSV at line 5")
                    .hasMessageContaining("The 3 payments before that point were stored")
                    .hasMessageContaining("Re-uploading the file is safe");
            List<String> stored = writtenBatches.stream().flatMap(batch -> batch.keySet().stream()).toList();
            assertThat(stored).containsExactlyInAnyOrder("1", "2", "3");
        }

        @Test
        @DisplayName("a file without the required columns stores nothing")
        void wrongHeader() {
            assertThatThrownBy(() -> service.uploadPayments(csv("a,b,c\n1,2,3\n")))
                    .isInstanceOf(CsvParsingException.class)
                    .hasMessageContaining("missing required column(s)")
                    .hasMessageContaining("No payments were stored");
            verify(cache, never()).putAll(anyMap());
        }

        @Test
        @DisplayName("a cluster failure is reported with the number of rows already stored")
        void clusterFailure() {
            doAnswer(invocation -> writtenBatches.add(invocation.getArgument(0)))
                    .doThrow(new CacheException("node left"))
                    .when(cache).putAll(anyMap());

            assertThatThrownBy(() -> service.uploadPayments(csv(TestCsv.rows(5))))
                    .isInstanceOf(ClusterUnavailableException.class)
                    .hasMessageContaining("after 2 payments had been saved");
        }

        @Test
        @DisplayName("refuses to start when the cluster memory is nearly full")
        void memoryFullUpFront() {
            memoryHas(0.90);

            assertThatThrownBy(() -> service.uploadPayments(csv(TestCsv.rows(3))))
                    .isInstanceOf(InsufficientStorageException.class)
                    .hasMessageContaining("fullest node uses 90% of its data region")
                    .hasMessageContaining("0 payments stored");
            verify(cache, never()).putAll(anyMap());
        }

        @Test
        @DisplayName("stops when the cluster memory fills up during the upload")
        void memoryFullMidway() {
            when(memoryGuard.status()).thenReturn(
                    new ClusterMemoryGuard.Status(0.5, 0.85),
                    new ClusterMemoryGuard.Status(0.6, 0.85),
                    new ClusterMemoryGuard.Status(0.86, 0.85));

            assertThatThrownBy(() -> service.uploadPayments(csv(TestCsv.rows(10))))
                    .isInstanceOf(InsufficientStorageException.class)
                    .hasMessageContaining("2 payments stored");
            verify(cache, times(1)).putAll(anyMap());
        }
    }

    @Nested
    @DisplayName("Delete")
    class Delete {

        @Test
        @DisplayName("an unbounded range clears the cache")
        void deleteAll() {
            when(cache.sizeLong()).thenReturn(500L);

            assertThat(service.deletePayments(TimeRange.unbounded())).isEqualTo(500);
            verify(cache).clear();
        }

        @Test
        @DisplayName("a range scans only keys and removes them in batches")
        @SuppressWarnings("unchecked")
        void deleteRange() {
            List<String> ids = IntStream.range(0, 5).mapToObj(String::valueOf).toList();
            when(cache.query(any(ScanQuery.class), any(PaymentIdTransformer.class))).thenReturn(new ListQueryCursor<>(ids));

            long deleted = service.deletePayments(TimeRange.of(at("2026-02-20T00:00"), at("2026-02-20T23:59:59")));

            assertThat(deleted).isEqualTo(5);
            ArgumentCaptor<Set<String>> removed = ArgumentCaptor.forClass(Set.class);
            verify(cache, times(3)).removeAll(removed.capture());
            verify(cache, never()).clear();
        }
    }

    @Test
    @DisplayName("Stream passes every matching payment to the consumer and closes the cursor")
    @SuppressWarnings("unchecked")
    void stream() {
        Payment p1 = payment("1", "2026-02-20T10:00", "A", "B", "1");
        Payment p2 = payment("2", "2026-02-20T11:00", "A", "B", "2");
        ListQueryCursor<Cache.Entry<String, Payment>> cursor = ListQueryCursor.ofEntries(
                List.of(ListQueryCursor.entry("1", p1), ListQueryCursor.entry("2", p2)));
        doReturn(cursor).when(cache).query(any(ScanQuery.class));

        List<Payment> streamed = new ArrayList<>();
        service.streamPayments(TimeRange.of(at("2026-02-20T00:00"), at("2026-02-20T23:59")), streamed::add);

        assertThat(streamed).containsExactly(p1, p2);
        assertThat(cursor.isClosed()).isTrue();
    }

    /** Small CSV generator for this test. */
    private static final class TestCsv {
        static String rows(int count) {
            StringBuilder csv = new StringBuilder(HEADER);
            for (int i = 0; i < count; i++) {
                csv.append("2026-02-20T10:00,A,B,1,id").append(i).append('\n');
            }
            return csv.toString();
        }
    }
}
