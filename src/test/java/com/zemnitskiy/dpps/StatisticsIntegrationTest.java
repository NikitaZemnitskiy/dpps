package com.zemnitskiy.dpps;

import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.dto.StatisticsResponse.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("GET /api/statistics")
class StatisticsIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("BY_DATE with GENERAL + VALUE — correct per-day aggregation")
    void byDate_generalAndValue() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE")
                        .param("metrics", "GENERAL", "VALUE"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = parseResponse(result, StatisticsResponse.class);
        Map<String, GroupStats> data = response.data();
        assertThat(data).hasSize(2).containsKeys("2026-02-20", "2026-02-21");

        GroupStats day20 = data.get("2026-02-20");
        assertThat(day20.general().count()).isEqualTo(5);
        assertThat(day20.value().min()).isEqualTo(50);
        assertThat(day20.value().max()).isEqualTo(200);
        assertThat(day20.value().sum()).isEqualTo(500);
        assertThat(day20.value().average()).isEqualTo(100);
        assertThat(day20.dateTime()).isNull();

        GroupStats day21 = data.get("2026-02-21");
        assertThat(day21.general().count()).isEqualTo(10);
        assertThat(day21.value().min()).isEqualTo(50);
        assertThat(day21.value().max()).isEqualTo(300);
        assertThat(day21.value().sum()).isEqualTo(1350);
        assertThat(day21.value().average()).isEqualTo(135);
    }

    @Test
    @DisplayName("BY_DATE with DATETIME — correct min/max/average datetime")
    void byDate_dateTime() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE")
                        .param("metrics", "DATETIME"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = parseResponse(result, StatisticsResponse.class);
        Map<String, GroupStats> data = response.data();

        DateTimeStats dt20 = data.get("2026-02-20").dateTime();
        assertThat(dt20.min()).isEqualTo("2026-02-20T12:00:00");
        assertThat(dt20.max()).isEqualTo("2026-02-20T22:00:00");
        assertThat(dt20.average()).isEqualTo("2026-02-20T18:12:00");

        DateTimeStats dt21 = data.get("2026-02-21").dateTime();
        assertThat(dt21.min()).isEqualTo("2026-02-21T02:00:00");
        assertThat(dt21.max()).isEqualTo("2026-02-21T23:00:00");
        assertThat(dt21.average()).isEqualTo("2026-02-21T11:30:00");

        assertThat(data.get("2026-02-20").general()).isNull();
        assertThat(data.get("2026-02-20").value()).isNull();
    }

    @Test
    @DisplayName("BY_BANK with all metrics — sender loses, receiver gains")
    void byBank_allMetrics() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_BANK")
                        .param("metrics", "GENERAL", "VALUE", "DATETIME"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = parseResponse(result, StatisticsResponse.class);
        Map<String, GroupStats> data = response.data();
        assertThat(data).hasSize(3).containsKeys("Bank A", "Bank B", "Bank C");

        GroupStats bankA = data.get("Bank A");
        assertThat(bankA.general().count()).isEqualTo(11);
        assertThat(bankA.value().sum()).isEqualTo(-300);
        assertThat(bankA.value().min()).isEqualTo(-300);
        assertThat(bankA.value().max()).isEqualTo(200);
        assertThat(bankA.dateTime().min()).isEqualTo("2026-02-20T12:00:00");
        assertThat(bankA.dateTime().max()).isEqualTo("2026-02-21T23:00:00");

        GroupStats bankB = data.get("Bank B");
        assertThat(bankB.general().count()).isEqualTo(10);
        assertThat(bankB.value().sum()).isEqualTo(50);

        GroupStats bankC = data.get("Bank C");
        assertThat(bankC.general().count()).isEqualTo(9);
        assertThat(bankC.value().sum()).isEqualTo(250);
    }

    @Test
    @DisplayName("BY_CONNECTION with GENERAL — correct pair counts")
    void byConnection_general() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_CONNECTION")
                        .param("metrics", "GENERAL"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = parseResponse(result, StatisticsResponse.class);
        Map<String, GroupStats> data = response.data();
        assertThat(data).hasSize(6);

        assertThat(data.get("Bank A-Bank B").general().count()).isEqualTo(4);
        assertThat(data.get("Bank A-Bank C").general().count()).isEqualTo(2);
        assertThat(data.get("Bank B-Bank A").general().count()).isEqualTo(2);
        assertThat(data.get("Bank B-Bank C").general().count()).isEqualTo(3);
        assertThat(data.get("Bank C-Bank A").general().count()).isEqualTo(3);
        assertThat(data.get("Bank C-Bank B").general().count()).isEqualTo(1);
    }

    @Test
    @DisplayName("BY_CONNECTION with VALUE — correct sums per direction")
    void byConnection_value() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_CONNECTION")
                        .param("metrics", "VALUE"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = parseResponse(result, StatisticsResponse.class);
        Map<String, GroupStats> data = response.data();

        assertThat(data.get("Bank A-Bank B").value().sum()).isEqualTo(550);
        assertThat(data.get("Bank B-Bank C").value().sum()).isEqualTo(400);
        assertThat(data.get("Bank C-Bank A").value().sum()).isEqualTo(400);
    }

    @Test
    @DisplayName("Statistics with time range filter — only matching records")
    void withTimeRange_shouldFilterData() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE")
                        .param("metrics", "GENERAL", "VALUE")
                        .param("from", "2026-02-21T00:00")
                        .param("to", "2026-02-21T12:00"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = parseResponse(result, StatisticsResponse.class);
        Map<String, GroupStats> data = response.data();

        assertThat(data).hasSize(1).containsKey("2026-02-21");

        GroupStats day21 = data.get("2026-02-21");
        assertThat(day21.general().count()).isEqualTo(6);
        assertThat(day21.value().sum()).isEqualTo(850);
        assertThat(day21.value().min()).isEqualTo(50);
        assertThat(day21.value().max()).isEqualTo(300);
        assertThat(day21.value().average()).isEqualTo(141.6667);
    }

    @Test
    @DisplayName("Only VALUE metric — general and dateTime are null")
    void onlyValueMetric_shouldReturnOnlyValue() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE")
                        .param("metrics", "VALUE"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = parseResponse(result, StatisticsResponse.class);
        GroupStats day20 = response.data().get("2026-02-20");

        assertThat(day20.value()).isNotNull();
        assertThat(day20.general()).isNull();
        assertThat(day20.dateTime()).isNull();
    }

    @Test
    @DisplayName("Statistics on empty cache — returns empty data map")
    void emptyCache_shouldReturnEmptyData() throws Exception {
        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE")
                        .param("metrics", "GENERAL"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = parseResponse(result, StatisticsResponse.class);
        assertThat(response.data()).isEmpty();
    }

    @Test
    @DisplayName("400 when aggregation parameter is missing")
    void missingAggregation_shouldReturn400() throws Exception {
        mockMvc.perform(get("/api/statistics")
                        .param("metrics", "GENERAL"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("400 when aggregation value is invalid")
    void invalidAggregation_shouldReturn400() throws Exception {
        mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "INVALID")
                        .param("metrics", "GENERAL"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("400 when metrics parameter is missing")
    void missingMetrics_shouldReturn400() throws Exception {
        mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE"))
                .andExpect(status().isBadRequest());
    }
}
