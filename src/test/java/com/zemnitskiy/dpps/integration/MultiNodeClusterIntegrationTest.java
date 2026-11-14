package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.config.IgniteProperties;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.service.ClusterMemoryGuard;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;

import static com.zemnitskiy.dpps.support.TestData.generatedCsv;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Runs the application's node together with two more Ignite server nodes in the same JVM - a real three-node
 * cluster with one backup per partition. Jobs, scan filters and partial results really travel between nodes.
 */
@DisplayName("Three-node cluster")
@Tag("integration")
class MultiNodeClusterIntegrationTest extends BaseIntegrationTest {

    /** 3,000 generated payments of 1.25 (= 3,750) plus the 15 test payments (= 1,850). */
    private static final long TOTAL_COUNT = 3_015;
    private static final BigDecimal TOTAL_SUM = new BigDecimal("5600.00");

    @Autowired
    private IgniteProperties igniteProperties;

    private final List<String> extraNodes = new ArrayList<>();

    @BeforeEach
    void startCluster() throws Exception {
        startNode("dpps-test-node-2");
        startNode("dpps-test-node-3");
        awaitServerNodes(3);
        upload(generatedCsv(3_000, "gen-"));
        loadTestData();
        awaitBalanced();
    }

    @AfterEach
    void stopExtraNodes() {
        if (cache().lostPartitions().isEmpty()) {
            cache().clear();
        }
        for (String name : extraNodes) {
            Ignition.stop(name, true);
        }
        extraNodes.clear();
        awaitServerNodes(1);
        // Stopping both extra nodes at once can leave (empty) partitions marked as lost; don't leak that state.
        ignite.resetLostPartitions(List.of(IgniteConfig.PAYMENTS_CACHE));
        cache().clear();
    }

    @Test
    @DisplayName("payments are spread over all nodes, each with a backup copy")
    void dataIsDistributed() {
        for (Ignite node : nodes()) {
            assertThat(localPrimaries(node)).as("primary payments on %s", node.name()).isPositive();
        }
        assertThat(sumOverNodes(MultiNodeClusterIntegrationTest::localPrimaries)).isEqualTo(TOTAL_COUNT);
        assertThat(sumOverNodes(MultiNodeClusterIntegrationTest::localCopies)).isEqualTo(2 * TOTAL_COUNT);
    }

    @Test
    @DisplayName("statistics count every payment exactly once - backups are never double-counted")
    void statisticsCountEachPaymentOnce() throws Exception {
        StatisticsResponse byDate = statistics(get("/api/statistics")
                .param("aggregation", "BY_DATE").param("metrics", "GENERAL", "VALUE"));

        assertThat(byDate.data().values().stream().mapToLong(g -> g.general().count()).sum()).isEqualTo(TOTAL_COUNT);
        assertThat(byDate.data().values().stream().map(g -> g.value().sum()).reduce(BigDecimal.ZERO, BigDecimal::add))
                .isEqualByComparingTo(TOTAL_SUM);
        assertThat(byDate.data().get("2026-02-20").general().count()).isEqualTo(5);

        StatisticsResponse byBank = statistics(get("/api/statistics")
                .param("aggregation", "BY_BANK").param("metrics", "GENERAL", "VALUE"));
        // Every payment has a sending and a receiving side, and net positions cancel out.
        assertThat(byBank.data().values().stream().mapToLong(g -> g.general().count()).sum()).isEqualTo(2 * TOTAL_COUNT);
        assertThat(byBank.data().values().stream().map(g -> g.value().sum()).reduce(BigDecimal.ZERO, BigDecimal::add))
                .isEqualByComparingTo("0");
    }

    @Test
    @DisplayName("GET and range DELETE see the data of all nodes exactly once")
    void readAndDeleteAcrossNodes() throws Exception {
        List<Payment> week = getPayments("2026-03-01T00:00", "2026-03-03T23:59:59");
        assertThat(week).hasSize(3_000);
        assertThat(week).extracting(Payment::getId).doesNotHaveDuplicates();

        MvcResult result = mockMvc.perform(delete("/api/payments")
                        .param("from", "2026-02-20T00:00").param("to", "2026-02-21T23:59:59"))
                .andExpect(status().isOk())
                .andReturn();

        assertThat(parse(result, DeleteResult.class).deletedCount()).isEqualTo(15);
        assertThat(cache().sizeLong()).isEqualTo(3_000);
    }

    @Test
    @DisplayName("statistics stay exact while nodes leave the cluster")
    void statisticsSurviveNodesLeaving() throws Exception {
        StatisticsResponse before = byConnection();

        stopNode("dpps-test-node-2");
        // Asked right away, while partitions are being promoted and rebalanced.
        assertThat(byConnection()).isEqualTo(before);

        // With two nodes and one backup each node ends up holding every payment.
        await().atMost(Duration.ofSeconds(60)).until(() ->
                cache().localSizeLong(CachePeekMode.PRIMARY, CachePeekMode.BACKUP) == TOTAL_COUNT);
        stopNode("dpps-test-node-3");

        assertThat(byConnection()).isEqualTo(before);
        assertThat(cache().sizeLong()).isEqualTo(TOTAL_COUNT);
    }

    @Test
    @DisplayName("losing two nodes at once: incomplete data is refused until all payments are deleted")
    void twoNodesLostAtOnce() throws Exception {
        Thread stop2 = new Thread(() -> Ignition.stop("dpps-test-node-2", true));
        Thread stop3 = new Thread(() -> Ignition.stop("dpps-test-node-3", true));
        stop2.start();
        stop3.start();
        stop2.join();
        stop3.join();
        extraNodes.clear();
        awaitServerNodes(1);
        await().atMost(Duration.ofSeconds(30)).until(() -> !cache().lostPartitions().isEmpty());

        // Statistics, reads and uploads refuse to work on incomplete data and explain how to recover.
        mockMvc.perform(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.message").value(containsString("Payments were lost")));
        mockMvc.perform(get("/api/payments").param("from", "2026-03-01T00:00").param("to", "2026-03-02T00:00"))
                .andExpect(status().isServiceUnavailable());
        mockMvc.perform(multipart("/api/payments/upload").file(csvFile(TEST_CSV)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.message").value(containsString("DELETE /api/payments")));
        mockMvc.perform(get("/actuator/health"))
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.components.ignite.details.lostPartitions").isNumber());

        // Deleting everything acknowledges the loss; afterwards the cluster works normally again.
        mockMvc.perform(delete("/api/payments")).andExpect(status().isOk());
        assertThat(cache().lostPartitions()).isEmpty();
        assertThat(upload(TEST_CSV).successfullyLoaded()).isEqualTo(15);
        assertThat(statistics(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL"))
                .data().values().stream().mapToLong(g -> g.general().count()).sum()).isEqualTo(15);
    }

    @Test
    @DisplayName("the memory guard sees all nodes")
    void memoryGuardSeesAllNodes() {
        ClusterMemoryGuard.Status status = new ClusterMemoryGuard(ignite, 0.85, Duration.ZERO).status();

        assertThat(status.usage()).isBetween(0.0, 0.85);
        assertThat(status.nearlyFull()).isFalse();
    }

    private StatisticsResponse byConnection() throws Exception {
        StatisticsResponse response = statistics(get("/api/statistics")
                .param("aggregation", "BY_CONNECTION").param("metrics", "GENERAL", "VALUE", "DATETIME"));
        assertThat(response.data().values().stream().mapToLong(g -> g.general().count()).sum()).isEqualTo(TOTAL_COUNT);
        return response;
    }

    /** Waits until rebalancing is over: every node is primary for some data and there are exactly two copies. */
    private void awaitBalanced() {
        await().atMost(Duration.ofSeconds(60)).until(() ->
                nodes().stream().allMatch(node -> localPrimaries(node) > 0)
                        && sumOverNodes(MultiNodeClusterIntegrationTest::localCopies) == 2 * TOTAL_COUNT);
    }

    private static long localPrimaries(Ignite node) {
        return node.cache(IgniteConfig.PAYMENTS_CACHE).localSizeLong(CachePeekMode.PRIMARY);
    }

    private static long localCopies(Ignite node) {
        return node.cache(IgniteConfig.PAYMENTS_CACHE).localSizeLong(CachePeekMode.PRIMARY, CachePeekMode.BACKUP);
    }

    private void startNode(String name) {
        Ignition.start(IgniteConfig.nodeConfiguration(igniteProperties.withInstanceName(name)));
        extraNodes.add(name);
    }

    private void stopNode(String name) {
        Ignition.stop(name, true);
        extraNodes.remove(name);
        awaitServerNodes(1 + extraNodes.size());
    }

    private void awaitServerNodes(int count) {
        await().atMost(Duration.ofSeconds(60))
                .until(() -> ignite.cluster().forServers().nodes().size() == count);
    }

    private List<Ignite> nodes() {
        List<Ignite> nodes = new ArrayList<>(List.of(ignite));
        extraNodes.forEach(name -> nodes.add(Ignition.ignite(name)));
        return nodes;
    }

    private long sumOverNodes(ToLongFunction<Ignite> perNode) {
        return nodes().stream().mapToLong(perNode).sum();
    }
}
