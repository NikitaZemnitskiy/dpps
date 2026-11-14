package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.dto.StatisticsResponse.DateTimeStats;
import com.zemnitskiy.dpps.dto.StatisticsResponse.GroupStats;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.zemnitskiy.dpps.support.TestData.HEADER;
import static com.zemnitskiy.dpps.support.TestData.at;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("GET /api/statistics")
@Tag("integration")
class StatisticsIntegrationTest extends BaseIntegrationTest {

    @Nested
    @DisplayName("Aggregations")
    class Aggregations {

        @Test
        @DisplayName("BY_DATE with GENERAL + VALUE")
        void byDate() throws Exception {
            loadTestData();

            Map<String, GroupStats> data = statistics(get("/api/statistics")
                    .param("aggregation", "BY_DATE").param("metrics", "GENERAL", "VALUE")).data();

            assertThat(data).containsOnlyKeys("2026-02-20", "2026-02-21");
            GroupStats day20 = data.get("2026-02-20");
            assertThat(day20.general().count()).isEqualTo(5);
            assertThat(day20.value().min()).isEqualByComparingTo("50");
            assertThat(day20.value().max()).isEqualByComparingTo("200");
            assertThat(day20.value().sum()).isEqualByComparingTo("500");
            assertThat(day20.value().average()).isEqualByComparingTo("100");
            assertThat(day20.dateTime()).isNull();
            GroupStats day21 = data.get("2026-02-21");
            assertThat(day21.general().count()).isEqualTo(10);
            assertThat(day21.value().sum()).isEqualByComparingTo("1350");
            assertThat(day21.value().average()).isEqualByComparingTo("135");
        }

        @Test
        @DisplayName("BY_DATE with DATETIME")
        void byDateDateTime() throws Exception {
            loadTestData();

            Map<String, GroupStats> data = statistics(get("/api/statistics")
                    .param("aggregation", "BY_DATE").param("metrics", "DATETIME")).data();

            DateTimeStats day20 = data.get("2026-02-20").dateTime();
            assertThat(day20.min()).isEqualTo(at("2026-02-20T12:00"));
            assertThat(day20.max()).isEqualTo(at("2026-02-20T22:00"));
            assertThat(day20.average()).isEqualTo(at("2026-02-20T18:12:00"));
            assertThat(data.get("2026-02-21").dateTime().average()).isEqualTo(at("2026-02-21T11:30:00"));
            assertThat(data.get("2026-02-20").general()).isNull();
            assertThat(data.get("2026-02-20").value()).isNull();
        }

        @Test
        @DisplayName("BY_BANK with all metrics: the sender side is negative, the receiver side positive")
        void byBank() throws Exception {
            loadTestData();

            Map<String, GroupStats> data = statistics(get("/api/statistics")
                    .param("aggregation", "BY_BANK").param("metrics", "GENERAL", "VALUE", "DATETIME")).data();

            assertThat(data).containsOnlyKeys("Bank A", "Bank B", "Bank C");
            GroupStats bankA = data.get("Bank A");
            assertThat(bankA.general().count()).isEqualTo(11);
            assertThat(bankA.value().sum()).isEqualByComparingTo("-300");
            assertThat(bankA.value().min()).isEqualByComparingTo("-300");
            assertThat(bankA.value().max()).isEqualByComparingTo("200");
            assertThat(bankA.dateTime().min()).isEqualTo(at("2026-02-20T12:00"));
            assertThat(bankA.dateTime().max()).isEqualTo(at("2026-02-21T23:00"));
            assertThat(data.get("Bank B").general().count()).isEqualTo(10);
            assertThat(data.get("Bank B").value().sum()).isEqualByComparingTo("50");
            assertThat(data.get("Bank C").general().count()).isEqualTo(9);
            assertThat(data.get("Bank C").value().sum()).isEqualByComparingTo("250");
        }

        @Test
        @DisplayName("BY_CONNECTION with GENERAL and VALUE")
        void byConnection() throws Exception {
            loadTestData();

            Map<String, GroupStats> data = statistics(get("/api/statistics")
                    .param("aggregation", "BY_CONNECTION").param("metrics", "GENERAL,VALUE")).data();

            assertThat(data).hasSize(6);
            assertThat(data.get("Bank A-Bank B").general().count()).isEqualTo(4);
            assertThat(data.get("Bank A-Bank B").value().sum()).isEqualByComparingTo("550");
            assertThat(data.get("Bank A-Bank C").general().count()).isEqualTo(2);
            assertThat(data.get("Bank B-Bank A").general().count()).isEqualTo(2);
            assertThat(data.get("Bank B-Bank C").value().sum()).isEqualByComparingTo("400");
            assertThat(data.get("Bank C-Bank A").general().count()).isEqualTo(3);
            assertThat(data.get("Bank C-Bank B").general().count()).isEqualTo(1);
        }

        @Test
        @DisplayName("amounts are summed exactly: 0.1 + 0.2 = 0.3")
        void exactSums() throws Exception {
            upload(HEADER + "2026-02-20T10:00,A,B,0.1,1\n2026-02-20T11:00,A,B,0.2,2\n");

            mockMvc.perform(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "VALUE"))
                    .andExpect(status().isOk())
                    .andExpect(content().json("""
                            {"data":{"2026-02-20":{"value":{"min":0.1,"max":0.2,"sum":0.3,"average":0.15}}}}
                            """, true));
        }

        @Test
        @DisplayName("an empty cache gives an empty result")
        void emptyCache() throws Exception {
            StatisticsResponse response = statistics(get("/api/statistics")
                    .param("aggregation", "BY_DATE").param("metrics", "GENERAL"));

            assertThat(response.data()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Time range")
    class Range {

        @Test
        @DisplayName("from + to: only payments inside the range, bounds included")
        void bounded() throws Exception {
            loadTestData();

            Map<String, GroupStats> data = statistics(get("/api/statistics")
                    .param("aggregation", "BY_DATE").param("metrics", "GENERAL", "VALUE")
                    .param("from", "2026-02-21T00:00").param("to", "2026-02-21T11:00")).data();

            assertThat(data).containsOnlyKeys("2026-02-21");
            assertThat(data.get("2026-02-21").general().count()).isEqualTo(6);
            assertThat(data.get("2026-02-21").value().sum()).isEqualByComparingTo("850");
            assertThat(data.get("2026-02-21").value().average()).isEqualByComparingTo("141.6667");
        }

        @Test
        @DisplayName("an open-ended range is allowed on either side")
        void openEnded() throws Exception {
            loadTestData();

            assertThat(statistics(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL")
                    .param("from", "2026-02-21T00:00")).data()).containsOnlyKeys("2026-02-21");
            assertThat(statistics(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL")
                    .param("to", "2026-02-20T23:59:59")).data()).containsOnlyKeys("2026-02-20");
        }

        @Test
        @DisplayName("400 for an unparseable bound (instead of silently matching nothing)")
        void invalidBound() throws Exception {
            mockMvc.perform(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL")
                            .param("from", "yesterday").param("to", "today"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value(containsString("Invalid value 'yesterday' for parameter 'from'")));
        }

        @Test
        @DisplayName("400 when 'from' is after 'to'")
        void reversed() throws Exception {
            mockMvc.perform(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL")
                            .param("from", "2026-02-22T00:00").param("to", "2026-02-21T00:00"))
                    .andExpect(status().isBadRequest());
        }
    }

    @Nested
    @DisplayName("Parameter validation")
    class Validation {

        @Test
        @DisplayName("400 when 'aggregation' is missing")
        void missingAggregation() throws Exception {
            mockMvc.perform(get("/api/statistics").param("metrics", "GENERAL"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value("Required parameter 'aggregation' is missing"));
        }

        @Test
        @DisplayName("400 listing the allowed values when 'aggregation' is unknown")
        void unknownAggregation() throws Exception {
            mockMvc.perform(get("/api/statistics").param("aggregation", "BY_PLANET").param("metrics", "GENERAL"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value("Invalid value 'BY_PLANET' for parameter 'aggregation':"
                            + " expected one of BY_DATE, BY_BANK, BY_CONNECTION"));
        }

        @Test
        @DisplayName("400 listing the allowed values when a metric is unknown")
        void unknownMetric() throws Exception {
            mockMvc.perform(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL", "SPEED"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value(containsString("expected one of GENERAL, VALUE, DATETIME")));
        }

        @Test
        @DisplayName("400 when 'metrics' is missing or empty")
        void missingMetrics() throws Exception {
            mockMvc.perform(get("/api/statistics").param("aggregation", "BY_DATE"))
                    .andExpect(status().isBadRequest());
            mockMvc.perform(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", ""))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value(containsString("At least one metric is required")));
        }
    }
}
