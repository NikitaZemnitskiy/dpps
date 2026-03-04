package com.zemnitskiy.dpps;

import com.fasterxml.jackson.core.type.TypeReference;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.IgniteCache;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MvcResult;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@DisplayName("Payment CRUD — GET & DELETE /api/payments")
class PaymentCrudIntegrationTest extends BaseIntegrationTest {

    // ==================== GET /api/payments ====================

    @Nested
    @DisplayName("GET /api/payments")
    class GetPaymentsTests {

        @Test
        @DisplayName("Returns payments within time range")
        void validRange_shouldReturnPaymentsInRange() throws Exception {
            loadTestData();

            MvcResult result = mockMvc.perform(get("/api/payments")
                            .param("from", "2026-02-20T00:00")
                            .param("to", "2026-02-20T23:59"))
                    .andExpect(status().isOk())
                    .andReturn();

            List<Payment> payments = parseResponse(result, new TypeReference<>() {});
            assertThat(payments)
                    .hasSize(5)
                    .allMatch(p -> p.getDateTime().toLocalDate().equals(LocalDate.of(2026, 2, 20)));
        }

        @Test
        @DisplayName("Returns empty list when no payments match the range")
        void noMatchingPayments_shouldReturnEmptyList() throws Exception {
            loadTestData();

            MvcResult result = mockMvc.perform(get("/api/payments")
                            .param("from", "2025-01-01T00:00")
                            .param("to", "2025-01-02T00:00"))
                    .andExpect(status().isOk())
                    .andReturn();

            List<Payment> payments = parseResponse(result, new TypeReference<>() {});
            assertThat(payments).isEmpty();
        }

        @Test
        @DisplayName("400 when time range exceeds 1 week")
        void rangeExceedingOneWeek_shouldReturn400() throws Exception {
            mockMvc.perform(get("/api/payments")
                            .param("from", "2026-02-01T00:00")
                            .param("to", "2026-02-20T00:00"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("400 when 'from' is after 'to'")
        void fromAfterTo_shouldReturn400() throws Exception {
            mockMvc.perform(get("/api/payments")
                            .param("from", "2026-02-21T00:00")
                            .param("to", "2026-02-20T00:00"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("400 when required 'from' or 'to' param is missing")
        void missingParams_shouldReturn400() throws Exception {
            mockMvc.perform(get("/api/payments")
                            .param("from", "2026-02-20T00:00"))
                    .andExpect(status().isBadRequest());

            mockMvc.perform(get("/api/payments"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("400 when date format is invalid")
        void invalidDateFormat_shouldReturn400() throws Exception {
            mockMvc.perform(get("/api/payments")
                            .param("from", "not-a-date")
                            .param("to", "2026-02-20T23:59"))
                    .andExpect(status().isBadRequest());
        }
    }

    // ==================== DELETE /api/payments ====================

    @Nested
    @DisplayName("DELETE /api/payments")
    class DeletePaymentsTests {

        @Test
        @DisplayName("Deletes only payments within specified range")
        void deleteWithRange_shouldDeleteMatchingPayments() throws Exception {
            loadTestData();

            MvcResult result = mockMvc.perform(delete("/api/payments")
                            .param("from", "2026-02-20T00:00")
                            .param("to", "2026-02-20T23:59"))
                    .andExpect(status().isOk())
                    .andReturn();

            DeleteResult deleteResult = parseResponse(result, DeleteResult.class);
            assertThat(deleteResult.deletedCount()).isEqualTo(5);

            IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
            assertThat(cache.size()).isEqualTo(10);
        }

        @Test
        @DisplayName("Deletes all payments when no range specified")
        void deleteAll_shouldDeleteAllPayments() throws Exception {
            loadTestData();

            MvcResult result = mockMvc.perform(delete("/api/payments"))
                    .andExpect(status().isOk())
                    .andReturn();

            DeleteResult deleteResult = parseResponse(result, DeleteResult.class);
            assertThat(deleteResult.deletedCount()).isEqualTo(15);

            IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
            assertThat(cache.size()).isEqualTo(0);
        }

        @Test
        @DisplayName("Returns zero deleted when range matches nothing")
        void deleteEmptyRange_shouldReturnZero() throws Exception {
            loadTestData();

            MvcResult result = mockMvc.perform(delete("/api/payments")
                            .param("from", "2025-01-01T00:00")
                            .param("to", "2025-01-02T00:00"))
                    .andExpect(status().isOk())
                    .andReturn();

            DeleteResult deleteResult = parseResponse(result, DeleteResult.class);
            assertThat(deleteResult.deletedCount()).isEqualTo(0);

            IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
            assertThat(cache.size()).isEqualTo(15);
        }

        @Test
        @DisplayName("Data is actually removed — GET after DELETE returns empty")
        void deleteAndGet_shouldReturnNoData() throws Exception {
            loadTestData();

            mockMvc.perform(delete("/api/payments")
                            .param("from", "2026-02-20T00:00")
                            .param("to", "2026-02-20T23:59"))
                    .andExpect(status().isOk());

            MvcResult result = mockMvc.perform(get("/api/payments")
                            .param("from", "2026-02-20T00:00")
                            .param("to", "2026-02-20T23:59"))
                    .andExpect(status().isOk())
                    .andReturn();

            List<Payment> payments = parseResponse(result, new TypeReference<>() {});
            assertThat(payments).isEmpty();
        }
    }
}
