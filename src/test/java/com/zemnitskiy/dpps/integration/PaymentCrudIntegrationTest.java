package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.model.Payment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MvcResult;

import java.util.List;

import static com.zemnitskiy.dpps.support.TestData.HEADER;
import static com.zemnitskiy.dpps.support.TestData.at;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("GET & DELETE /api/payments")
@Tag("integration")
class PaymentCrudIntegrationTest extends BaseIntegrationTest {

    @Nested
    @DisplayName("GET /api/payments")
    class GetPayments {

        @Test
        @DisplayName("returns the payments within the range")
        void withinRange() throws Exception {
            loadTestData();

            List<Payment> payments = getPayments("2026-02-20T00:00", "2026-02-20T23:59:59");

            assertThat(payments).hasSize(5)
                    .allSatisfy(p -> assertThat(p.getDateTime().toLocalDate()).isEqualTo(at("2026-02-20T00:00").toLocalDate()));
        }

        @Test
        @DisplayName("serializes date-times in ISO-8601 and amounts as exact JSON numbers")
        void jsonFormat() throws Exception {
            upload(HEADER + "2026-02-20 12:00,Bank A,Bank B,100.50,P-1\n");

            MvcResult started = mockMvc.perform(get("/api/payments")
                            .param("from", "2026-02-20T00:00").param("to", "2026-02-20T23:59:59"))
                    .andExpect(request().asyncStarted())
                    .andReturn();
            mockMvc.perform(asyncDispatch(started))
                    .andExpect(status().isOk())
                    .andExpect(content().json("""
                            [{"id":"P-1","dateTime":"2026-02-20T12:00:00","sender":"Bank A","receiver":"Bank B","value":100.50}]
                            """, true));
        }

        @Test
        @DisplayName("both bounds are inclusive and compared as real timestamps")
        void inclusiveBounds() throws Exception {
            upload(HEADER + """
                    2026-02-20T00:00,A,B,1,first
                    2026-02-20T23:59:30,A,B,1,late
                    2026-02-20T23:59:59.999,A,B,1,last-millisecond
                    2026-02-21T00:00,A,B,1,next-day
                    """);

            assertThat(getPayments("2026-02-20T00:00", "2026-02-20T23:59")).extracting(Payment::getId)
                    .containsExactly("first");
            assertThat(getPayments("2026-02-20T00:00", "2026-02-20T23:59:59.999")).extracting(Payment::getId)
                    .containsExactlyInAnyOrder("first", "late", "last-millisecond");
        }

        @Test
        @DisplayName("accepts a space instead of 'T' in parameters, like in CSV files")
        void spaceSeparatedParameters() throws Exception {
            loadTestData();

            assertThat(getPayments("2026-02-21 00:00", "2026-02-21 23:59:59")).hasSize(10);
        }

        @Test
        @DisplayName("an exactly 7-day range is allowed")
        void sevenDays() throws Exception {
            loadTestData();

            assertThat(getPayments("2026-02-20T00:00", "2026-02-27T00:00")).hasSize(15);
        }

        @Test
        @DisplayName("returns an empty array when nothing matches")
        void noMatches() throws Exception {
            loadTestData();

            assertThat(getPayments("2025-01-01T00:00", "2025-01-02T00:00")).isEmpty();
        }

        @Test
        @DisplayName("400 when the range exceeds 7 days")
        void tooLong() throws Exception {
            mockMvc.perform(get("/api/payments").param("from", "2026-02-20T00:00").param("to", "2026-02-27T00:00:01"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value("The time range must not exceed 7 days"));
        }

        @Test
        @DisplayName("400 when 'from' is after 'to'")
        void fromAfterTo() throws Exception {
            mockMvc.perform(get("/api/payments").param("from", "2026-02-21T00:00").param("to", "2026-02-20T00:00"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value(containsString("must not be after")));
        }

        @Test
        @DisplayName("400 when a bound is missing")
        void missingBound() throws Exception {
            mockMvc.perform(get("/api/payments").param("from", "2026-02-20T00:00"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value("Required parameter 'to' is missing"));
            mockMvc.perform(get("/api/payments").param("from", "").param("to", "2026-02-20T00:00"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("400 with the expected format when a date-time can't be parsed")
        void invalidDateTime() throws Exception {
            mockMvc.perform(get("/api/payments").param("from", "yesterday").param("to", "2026-02-20T23:59"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value(
                            "Invalid value 'yesterday' for parameter 'from': expected an ISO-8601 local date-time"
                                    + " such as 2026-02-20T12:00 or 2026-02-20T12:00:30"));
        }
    }

    @Nested
    @DisplayName("DELETE /api/payments")
    class DeletePayments {

        @Test
        @DisplayName("with a range: deletes only the payments inside it")
        void range() throws Exception {
            loadTestData();

            MvcResult result = mockMvc.perform(delete("/api/payments")
                            .param("from", "2026-02-20T00:00").param("to", "2026-02-20T23:59:59"))
                    .andExpect(status().isOk())
                    .andReturn();

            assertThat(parse(result, DeleteResult.class).deletedCount()).isEqualTo(5);
            assertThat(cache().size()).isEqualTo(10);
            assertThat(getPayments("2026-02-20T00:00", "2026-02-20T23:59:59")).isEmpty();
        }

        @Test
        @DisplayName("without parameters: deletes everything")
        void all() throws Exception {
            loadTestData();

            MvcResult result = mockMvc.perform(delete("/api/payments")).andExpect(status().isOk()).andReturn();

            assertThat(parse(result, DeleteResult.class).deletedCount()).isEqualTo(15);
            assertThat(cache().size()).isZero();
        }

        @Test
        @DisplayName("with only one bound: 400 and nothing is deleted")
        void oneBoundOnly() throws Exception {
            loadTestData();

            mockMvc.perform(delete("/api/payments").param("from", "2026-02-21T00:00"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message").value(
                            "Provide both 'from' and 'to' to delete a time range, or neither to delete all payments"));
            mockMvc.perform(delete("/api/payments").param("to", "2026-02-21T00:00"))
                    .andExpect(status().isBadRequest());

            assertThat(cache().size()).isEqualTo(15);
        }

        @Test
        @DisplayName("with an unparseable bound: 400 and nothing is deleted")
        void invalidBound() throws Exception {
            loadTestData();

            mockMvc.perform(delete("/api/payments").param("from", "0").param("to", "9"))
                    .andExpect(status().isBadRequest());

            assertThat(cache().size()).isEqualTo(15);
        }

        @Test
        @DisplayName("with 'from' after 'to': 400")
        void reversedRange() throws Exception {
            mockMvc.perform(delete("/api/payments").param("from", "2026-02-21T00:00").param("to", "2026-02-20T00:00"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("a range that matches nothing deletes nothing")
        void emptyRange() throws Exception {
            loadTestData();

            MvcResult result = mockMvc.perform(delete("/api/payments")
                            .param("from", "2025-01-01T00:00").param("to", "2025-01-02T00:00"))
                    .andExpect(status().isOk())
                    .andReturn();

            assertThat(parse(result, DeleteResult.class).deletedCount()).isZero();
            assertThat(cache().size()).isEqualTo(15);
        }
    }
}
