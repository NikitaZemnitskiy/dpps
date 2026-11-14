package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.model.Payment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static com.zemnitskiy.dpps.support.TestData.HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("End-to-end flows")
@Tag("integration")
class EndToEndIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("upload -> get: the returned payments match the file")
    void uploadAndGet() throws Exception {
        upload(HEADER + """
                2026-02-25T10:00,Alpha,Beta,500,E2E-1
                2026-02-25T14:00,Beta,Gamma,250.75,E2E-2
                2026-02-25T18:00,Gamma,Alpha,750,E2E-3
                """);

        List<Payment> payments = getPayments("2026-02-25T00:00", "2026-02-25T23:59:59");

        assertThat(payments).extracting(Payment::getId).containsExactlyInAnyOrder("E2E-1", "E2E-2", "E2E-3");
        assertThat(payments).extracting(Payment::getValue)
                .usingElementComparator(BigDecimal::compareTo)
                .containsExactlyInAnyOrder(new BigDecimal("500"), new BigDecimal("250.75"), new BigDecimal("750"));
    }

    @Test
    @DisplayName("upload -> delete a day -> get: only the other day remains")
    void uploadDeleteGet() throws Exception {
        loadTestData();

        mockMvc.perform(delete("/api/payments").param("from", "2026-02-20T00:00").param("to", "2026-02-20T23:59:59"))
                .andExpect(status().isOk());

        List<Payment> remaining = getPayments("2026-02-20T00:00", "2026-02-21T23:59:59");
        assertThat(remaining).hasSize(10)
                .allSatisfy(p -> assertThat(p.getDateTime().toLocalDate()).isEqualTo(LocalDate.of(2026, 2, 21)));
    }

    @Test
    @DisplayName("upload -> statistics -> delete all -> statistics are empty")
    void fullLifecycle() throws Exception {
        loadTestData();
        assertThat(statistics(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL"))
                .data()).isNotEmpty();

        mockMvc.perform(delete("/api/payments")).andExpect(status().isOk());

        assertThat(statistics(get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL"))
                .data()).isEmpty();
    }
}
