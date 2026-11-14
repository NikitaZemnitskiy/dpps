package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.service.ClusterMemoryGuard;
import com.zemnitskiy.dpps.support.TestData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.Map;

import static com.zemnitskiy.dpps.support.TestData.HEADER;
import static com.zemnitskiy.dpps.support.TestData.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("POST /api/payments/upload")
@Tag("integration")
class PaymentUploadIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private ClusterMemoryGuard memoryGuard;

    @Test
    @DisplayName("valid CSV: every row is stored")
    void validCsv() throws Exception {
        UploadResult result = upload(TEST_CSV);

        assertThat(result).isEqualTo(new UploadResult(15, 0, Map.of()));
        assertThat(cache().size()).isEqualTo(15);
        assertThat(cache().get("3")).isEqualTo(payment("3", "2026-02-20T20:00", "Bank A", "Bank C", "200"));
    }

    @Test
    @DisplayName("response JSON lists the counters and omits 'errors' when there are none")
    void responseShape() throws Exception {
        mockMvc.perform(multipart("/api/payments/upload").file(csvFile(TEST_CSV)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.successfullyLoaded").value(15))
                .andExpect(jsonPath("$.rejected").value(0))
                .andExpect(jsonPath("$.errors").doesNotExist());
    }

    @Test
    @DisplayName("invalid rows are skipped and counted per problem")
    void invalidRows() throws Exception {
        UploadResult result = upload(HEADER + """
                2026-02-20T12:00,Bank A,Bank B,100,1
                2026-02-20T13:00,,Bank C,50,2
                2026-02-20T14:00,Bank A,,100,3
                ,Bank A,Bank B,100,4
                2026-02-20T15:00,Bank A,Bank B,100,
                2026-02-20 16:00,Bank A,Bank B,-5,6
                20/02/2026,Bank A,Bank B,7,7
                """);

        assertThat(result.successfullyLoaded()).isEqualTo(1);
        assertThat(result.rejected()).isEqualTo(6);
        assertThat(result.errors()).containsOnly(
                Map.entry("missing_sender", 1), Map.entry("missing_receiver", 1), Map.entry("missing_datetime", 1),
                Map.entry("missing_id", 1), Map.entry("invalid_value", 1), Map.entry("invalid_datetime", 1));
    }

    @Test
    @DisplayName("a row with an unparseable date-time is rejected instead of breaking statistics later")
    void unparseableDateTimeRejected() throws Exception {
        UploadResult result = upload(HEADER + "2026-02-20 13:00,Bank A,Bank B,100,ok\n2026,Bank A,Bank B,100,bad\n");

        assertThat(result.successfullyLoaded()).isEqualTo(1);
        assertThat(result.errors()).containsOnly(Map.entry("invalid_datetime", 1));
        assertThat(cache().containsKey("bad")).isFalse();
    }

    @Test
    @DisplayName("'Value' is accepted as the amount column")
    void valueColumn() throws Exception {
        UploadResult result = upload("DateTime,Sender,Receiver,Value,ID\n2026-02-20T12:00,Bank A,Bank B,100,1\n");

        assertThat(result.successfullyLoaded()).isEqualTo(1);
    }

    @Test
    @DisplayName("a repeated ID replaces the earlier payment (last row wins), also across batches")
    void repeatedIds() throws Exception {
        StringBuilder csv = new StringBuilder(HEADER);
        for (int i = 0; i < 1500; i++) {
            csv.append("2026-02-20T12:00,A,B,").append(i + 1).append(",id-").append(i % 1000).append('\n');
        }

        UploadResult result = upload(csv.toString());

        assertThat(result.successfullyLoaded()).isEqualTo(1500);
        assertThat(cache().size()).isEqualTo(1000);
        assertThat(cache().get("id-0").getValue()).isEqualByComparingTo("1001");
    }

    @Test
    @DisplayName("re-uploading the same file is idempotent")
    void reUpload() throws Exception {
        upload(TEST_CSV);
        UploadResult again = upload(TEST_CSV);

        assertThat(again.successfullyLoaded()).isEqualTo(15);
        assertThat(cache().size()).isEqualTo(15);
    }

    @Test
    @DisplayName("separate uploads accumulate")
    void accumulate() throws Exception {
        upload(TestData.generatedCsv(3, "a-"));
        upload(TestData.generatedCsv(4, "b-"));

        assertThat(cache().size()).isEqualTo(7);
    }

    @Test
    @DisplayName("header only: nothing stored, nothing rejected")
    void headerOnly() throws Exception {
        assertThat(upload(HEADER)).isEqualTo(new UploadResult(0, 0, Map.of()));
    }

    @Test
    @DisplayName("400 for a file without the required columns")
    void wrongHeader() throws Exception {
        mockMvc.perform(multipart("/api/payments/upload").file(csvFile("Date,From,To,Sum\n2026-02-20,A,B,1\n")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("CSV Parsing Error"))
                .andExpect(jsonPath("$.message").value(containsString("missing required column(s)")));
        assertThat(cache().size()).isZero();
    }

    @Test
    @DisplayName("400 for broken CSV syntax; rows before the broken line are kept and the response says so")
    void malformedCsv() throws Exception {
        String csv = HEADER + "2026-02-20T12:00,A,B,1,1\n2026-02-20T12:00,\"A,B,1,2\n2026-02-20T12:00,A,B,1,3\n";

        mockMvc.perform(multipart("/api/payments/upload").file(csvFile(csv)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("Malformed CSV at line 3")))
                .andExpect(jsonPath("$.message").value(containsString("The 1 payment before that point was stored")));
        assertThat(cache().containsKey("1")).isTrue();
        assertThat(cache().containsKey("3")).isFalse();
    }

    @Test
    @DisplayName("400 for an empty file")
    void emptyFile() throws Exception {
        mockMvc.perform(multipart("/api/payments/upload").file(csvFile("")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("The uploaded file is empty"));
    }

    @Test
    @DisplayName("400 when the multipart request has no 'file' part")
    void missingFilePart() throws Exception {
        mockMvc.perform(multipart("/api/payments/upload"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("file")));
    }

    @Test
    @DisplayName("415 when the request isn't multipart")
    void notMultipart() throws Exception {
        mockMvc.perform(post("/api/payments/upload").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isUnsupportedMediaType())
                .andExpect(jsonPath("$.status").value(415));
    }

    @Test
    @DisplayName("507 and nothing stored when the cluster memory is nearly full")
    void memoryNearlyFull() throws Exception {
        double watermark = (double) ReflectionTestUtils.getField(memoryGuard, "highWatermark");
        ReflectionTestUtils.setField(memoryGuard, "highWatermark", 0.0);
        ReflectionTestUtils.setField(memoryGuard, "lastReading", null);
        try {
            mockMvc.perform(multipart("/api/payments/upload").file(csvFile(TEST_CSV)))
                    .andExpect(status().isInsufficientStorage())
                    .andExpect(jsonPath("$.message").value(containsString("The cluster is running out of memory")));
            assertThat(cache().size()).isZero();
        } finally {
            ReflectionTestUtils.setField(memoryGuard, "highWatermark", watermark);
            ReflectionTestUtils.setField(memoryGuard, "lastReading", null);
        }
    }

    @Test
    @DisplayName("stored payments keep exact amounts and real date-times")
    void exactValues() throws Exception {
        upload(HEADER + "2026-02-20 12:00:30.5,Bank A,Bank B,0.10,x\n");

        Payment stored = cache().get("x");
        assertThat(stored.getValue()).isEqualTo(new BigDecimal("0.10"));
        assertThat(stored.getDateTime()).isEqualTo(TestData.at("2026-02-20T12:00:30.500"));
    }
}
