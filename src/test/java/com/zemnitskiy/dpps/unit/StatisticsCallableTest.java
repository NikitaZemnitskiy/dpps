package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.compute.PartialStats;
import com.zemnitskiy.dpps.compute.StatisticsCallable;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.model.AggregationType;
import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.cache.Cache;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Unit tests for StatisticsCallable — MapReduce task executed on each cluster node.
 * Tests aggregation logic by date, bank, and connection.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("StatisticsCallable — Unit Tests")
@Tag("unit")
class StatisticsCallableTest {

    @Mock
    private Ignite ignite;

    @Mock
    private IgniteCache<String, Payment> cache;

    private List<Payment> testPayments;

    @BeforeEach
    void setUp() {
        testPayments = List.of(
                new Payment("p1", "2026-02-20T10:00:00", "BankA", "BankB", 100.0),
                new Payment("p2", "2026-02-20T14:00:00", "BankA", "BankC", 200.0),
                new Payment("p3", "2026-02-21T09:00:00", "BankB", "BankA", 150.0),
                new Payment("p4", "2026-02-21T16:00:00", "BankC", "BankB", 300.0)
        );
    }

    private StatisticsCallable createCallable(AggregationType aggregation, String from, String to) throws Exception {
        StatisticsCallable callable = new StatisticsCallable(aggregation, from, to);

        Field igniteField = StatisticsCallable.class.getDeclaredField("ignite");
        igniteField.setAccessible(true);
        igniteField.set(callable, ignite);

        return callable;
    }

    private void mockCacheWithPayments(List<Payment> payments) {
        doReturn(cache).when(ignite).cache(IgniteConfig.PAYMENTS_CACHE);
        when(ignite.name()).thenReturn("test-node");

        Iterable<Cache.Entry<String, Payment>> entries = () -> payments.stream()
                .<Cache.Entry<String, Payment>>map(p -> new Cache.Entry<>() {
                    @Override
                    public String getKey() {
                        return p.getId();
                    }

                    @Override
                    public Payment getValue() {
                        return p;
                    }

                    @Override
                    public <T> T unwrap(Class<T> clazz) {
                        return null;
                    }
                })
                .iterator();

        doReturn(entries).when(cache).localEntries(CachePeekMode.PRIMARY);
    }

    @Nested
    @DisplayName("BY_DATE Aggregation Tests")
    class ByDateAggregationTests {

        @Test
        @DisplayName("Groups payments by date correctly")
        void byDate_shouldGroupByDate() throws Exception {
            mockCacheWithPayments(testPayments);
            StatisticsCallable callable = createCallable(AggregationType.BY_DATE, null, null);

            Map<String, PartialStats> result = callable.call();

            assertThat(result).hasSize(2);
            assertThat(result).containsKeys("2026-02-20", "2026-02-21");

            PartialStats feb20Stats = result.get("2026-02-20");
            assertThat(feb20Stats.getCount()).isEqualTo(2);
            assertThat(feb20Stats.getSumValue()).isEqualTo(300.0);

            PartialStats feb21Stats = result.get("2026-02-21");
            assertThat(feb21Stats.getCount()).isEqualTo(2);
            assertThat(feb21Stats.getSumValue()).isEqualTo(450.0);
        }

        @Test
        @DisplayName("Empty cache returns empty map")
        void byDate_emptyCache_shouldReturnEmptyMap() throws Exception {
            mockCacheWithPayments(List.of());
            StatisticsCallable callable = createCallable(AggregationType.BY_DATE, null, null);

            Map<String, PartialStats> result = callable.call();

            assertThat(result).isEmpty();
        }
    }

    @Nested
    @DisplayName("BY_BANK Aggregation Tests")
    class ByBankAggregationTests {

        @Test
        @DisplayName("Groups payments by bank with positive/negative values")
        void byBank_shouldTrackSenderAndReceiver() throws Exception {
            List<Payment> payments = List.of(
                    new Payment("p1", "2026-02-20T10:00:00", "BankA", "BankB", 100.0)
            );
            mockCacheWithPayments(payments);
            StatisticsCallable callable = createCallable(AggregationType.BY_BANK, null, null);

            Map<String, PartialStats> result = callable.call();

            assertThat(result).hasSize(2);
            assertThat(result).containsKeys("BankA", "BankB");

            // Sender gets negative value (money out)
            PartialStats bankAStats = result.get("BankA");
            assertThat(bankAStats.getCount()).isEqualTo(1);
            assertThat(bankAStats.getSumValue()).isEqualTo(-100.0);

            // Receiver gets positive value (money in)
            PartialStats bankBStats = result.get("BankB");
            assertThat(bankBStats.getCount()).isEqualTo(1);
            assertThat(bankBStats.getSumValue()).isEqualTo(100.0);
        }

        @Test
        @DisplayName("Multiple transactions for same bank accumulate correctly")
        void byBank_multipleTransactions_shouldAccumulate() throws Exception {
            mockCacheWithPayments(testPayments);
            StatisticsCallable callable = createCallable(AggregationType.BY_BANK, null, null);

            Map<String, PartialStats> result = callable.call();

            assertThat(result).hasSize(3);
            assertThat(result).containsKeys("BankA", "BankB", "BankC");

            // BankA: sent 100+200, received 150 => -100-200+150 = -150
            PartialStats bankA = result.get("BankA");
            assertThat(bankA.getSumValue()).isEqualTo(-150.0);

            // BankB: sent 150, received 100+300 => -150+100+300 = 250
            PartialStats bankB = result.get("BankB");
            assertThat(bankB.getSumValue()).isEqualTo(250.0);

            // BankC: sent 300, received 200 => -300+200 = -100
            PartialStats bankC = result.get("BankC");
            assertThat(bankC.getSumValue()).isEqualTo(-100.0);
        }
    }

    @Nested
    @DisplayName("BY_CONNECTION Aggregation Tests")
    class ByConnectionAggregationTests {

        @Test
        @DisplayName("Groups payments by sender-receiver connection")
        void byConnection_shouldGroupByConnection() throws Exception {
            mockCacheWithPayments(testPayments);
            StatisticsCallable callable = createCallable(AggregationType.BY_CONNECTION, null, null);

            Map<String, PartialStats> result = callable.call();

            assertThat(result).hasSize(4);
            assertThat(result).containsKeys("BankA-BankB", "BankA-BankC", "BankB-BankA", "BankC-BankB");

            assertThat(result.get("BankA-BankB").getSumValue()).isEqualTo(100.0);
            assertThat(result.get("BankA-BankC").getSumValue()).isEqualTo(200.0);
            assertThat(result.get("BankB-BankA").getSumValue()).isEqualTo(150.0);
            assertThat(result.get("BankC-BankB").getSumValue()).isEqualTo(300.0);
        }

        @Test
        @DisplayName("Same connection multiple times accumulates")
        void byConnection_sameConnection_shouldAccumulate() throws Exception {
            List<Payment> payments = List.of(
                    new Payment("p1", "2026-02-20T10:00:00", "BankA", "BankB", 100.0),
                    new Payment("p2", "2026-02-20T14:00:00", "BankA", "BankB", 200.0)
            );
            mockCacheWithPayments(payments);
            StatisticsCallable callable = createCallable(AggregationType.BY_CONNECTION, null, null);

            Map<String, PartialStats> result = callable.call();

            assertThat(result).hasSize(1);
            PartialStats stats = result.get("BankA-BankB");
            assertThat(stats.getCount()).isEqualTo(2);
            assertThat(stats.getSumValue()).isEqualTo(300.0);
        }
    }

    @Nested
    @DisplayName("Time Range Filter Tests")
    class TimeRangeFilterTests {

        @Test
        @DisplayName("Filters payments by 'from' date")
        void filter_byFromDate_shouldExcludeEarlier() throws Exception {
            mockCacheWithPayments(testPayments);
            StatisticsCallable callable = createCallable(AggregationType.BY_DATE, "2026-02-21T00:00:00", null);

            Map<String, PartialStats> result = callable.call();

            assertThat(result).hasSize(1);
            assertThat(result).containsKey("2026-02-21");
            assertThat(result.get("2026-02-21").getCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("Filters payments by 'to' date")
        void filter_byToDate_shouldExcludeLater() throws Exception {
            mockCacheWithPayments(testPayments);
            StatisticsCallable callable = createCallable(AggregationType.BY_DATE, null, "2026-02-20T23:59:59");

            Map<String, PartialStats> result = callable.call();

            assertThat(result).hasSize(1);
            assertThat(result).containsKey("2026-02-20");
            assertThat(result.get("2026-02-20").getCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("Filters payments by date range")
        void filter_byDateRange_shouldIncludeOnlyInRange() throws Exception {
            mockCacheWithPayments(testPayments);
            StatisticsCallable callable = createCallable(
                    AggregationType.BY_DATE,
                    "2026-02-20T12:00:00",
                    "2026-02-21T12:00:00"
            );

            Map<String, PartialStats> result = callable.call();

            // Should include p2 (14:00 on 20th) and p3 (09:00 on 21st)
            assertThat(result).hasSize(2);
            assertThat(result.get("2026-02-20").getCount()).isEqualTo(1);
            assertThat(result.get("2026-02-21").getCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("No filter returns all payments")
        void filter_noFilter_shouldReturnAll() throws Exception {
            mockCacheWithPayments(testPayments);
            StatisticsCallable callable = createCallable(AggregationType.BY_DATE, null, null);

            Map<String, PartialStats> result = callable.call();

            long totalCount = result.values().stream().mapToLong(PartialStats::getCount).sum();
            assertThat(totalCount).isEqualTo(4);
        }
    }
}
