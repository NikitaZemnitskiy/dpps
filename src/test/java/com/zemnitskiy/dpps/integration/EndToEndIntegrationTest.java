package com.zemnitskiy.dpps.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.model.Payment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MvcResult;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("End-to-End Flows")
@Tag("integration")
class EndToEndIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("Upload → Get → verify returned data matches uploaded")
    void uploadAndGet_shouldReturnCorrectPayments() throws Exception {
        String csv = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-25T10:00,Alpha,Beta,500,E2E-1
                2026-02-25T14:00,Beta,Gamma,250,E2E-2
                2026-02-25T18:00,Gamma,Alpha,750,E2E-3
                """;

        performUpload(csv);

        MvcResult asyncResult = mockMvc.perform(get("/api/payments")
                        .param("from", "2026-02-25T00:00")
                        .param("to", "2026-02-25T23:59"))
                .andExpect(request().asyncStarted())
                .andReturn();

        MvcResult result = mockMvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isOk())
                .andReturn();

        List<Payment> payments = parseResponse(result, new TypeReference<>() {});
        assertThat(payments).hasSize(3);
        assertThat(payments)
                .extracting(Payment::getId)
                .containsExactlyInAnyOrder("E2E-1", "E2E-2", "E2E-3");
        assertThat(payments)
                .extracting(Payment::getValue)
                .containsExactlyInAnyOrder(500.0, 250.0, 750.0);
    }

    @Test
    @DisplayName("Upload → Delete range → Get → only remaining data visible")
    void uploadDeleteAndGet_shouldShowRemainingData() throws Exception {
        loadTestData();

        mockMvc.perform(delete("/api/payments")
                        .param("from", "2026-02-20T00:00")
                        .param("to", "2026-02-20T23:59"))
                .andExpect(status().isOk());

        MvcResult asyncResult = mockMvc.perform(get("/api/payments")
                        .param("from", "2026-02-21T00:00")
                        .param("to", "2026-02-21T23:59"))
                .andExpect(request().asyncStarted())
                .andReturn();

        MvcResult result = mockMvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isOk())
                .andReturn();

        List<Payment> remaining = parseResponse(result, new TypeReference<>() {});
        assertThat(remaining).hasSize(10);
        assertThat(remaining).allMatch(p -> p.getDateTime().startsWith("2026-02-21"));
    }

    @Test
    @DisplayName("Upload → Statistics → Delete all → Statistics returns empty")
    void fullLifecycle_uploadStatisticsDeleteStatistics() throws Exception {
        loadTestData();

        MvcResult beforeDelete = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE")
                        .param("metrics", "GENERAL"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse before = parseResponse(beforeDelete, StatisticsResponse.class);
        assertThat(before.data()).isNotEmpty();

        mockMvc.perform(delete("/api/payments"))
                .andExpect(status().isOk());

        MvcResult afterDelete = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE")
                        .param("metrics", "GENERAL"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse after = parseResponse(afterDelete, StatisticsResponse.class);
        assertThat(after.data()).isEmpty();
    }
}
