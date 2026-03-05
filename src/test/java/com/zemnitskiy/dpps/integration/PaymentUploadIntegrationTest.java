package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.IgniteCache;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MvcResult;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("POST /api/payments/upload")
@Tag("integration")
class PaymentUploadIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("Valid CSV — all 15 records loaded as new")
    void validCsv_shouldUploadAllRecords() throws Exception {
        MvcResult result = performUpload(TEST_CSV);

        UploadResult uploadResult = parseResponse(result, UploadResult.class);

        assertThat(uploadResult.getSuccessfullyLoaded()).isEqualTo(15);
        assertThat(uploadResult.getNewRecords()).isEqualTo(15);
        assertThat(uploadResult.getUpdatedRecords()).isEqualTo(0);
        assertThat(uploadResult.getErrors()).isEmpty();
    }

    @Test
    @DisplayName("CSV with missing fields — reports specific missing field errors")
    void csvWithMissingFields_shouldReportErrors() throws Exception {
        // Row 2: missing sender, Row 3: missing receiver, Row 4: missing datetime, Row 5: missing id
        String csv = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-20T12:00,Bank A,Bank B,100,1
                2026-02-20T13:00,,Bank C,50,2
                2026-02-20T14:00,Bank A,,100,3
                ,Bank A,Bank B,100,4
                2026-02-20T15:00,Bank A,Bank B,100,
                """;

        MvcResult result = performUpload(csv);
        UploadResult uploadResult = parseResponse(result, UploadResult.class);

        assertThat(uploadResult.getSuccessfullyLoaded()).isEqualTo(1);
        assertThat(uploadResult.getErrors())
                .containsEntry("missing_sender", 1)
                .containsEntry("missing_receiver", 1)
                .containsEntry("missing_datetime", 1)
                .containsEntry("missing_id", 1);
    }

    @Test
    @DisplayName("CSV with negative and zero amounts — reports invalid_value errors")
    void csvWithInvalidAmounts_shouldReportInvalidValue() throws Exception {
        // Row 1: negative, Row 2: zero, Row 3: NaN, Row 4: valid
        String csv = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-20T12:00,Bank A,Bank B,-50,1
                2026-02-20T13:00,Bank A,Bank B,0,2
                2026-02-20T14:00,Bank A,Bank B,abc,3
                2026-02-20T15:00,Bank A,Bank B,100,4
                """;

        MvcResult result = performUpload(csv);
        UploadResult uploadResult = parseResponse(result, UploadResult.class);

        assertThat(uploadResult.getSuccessfullyLoaded()).isEqualTo(1);
        assertThat(uploadResult.getErrors()).containsEntry("invalid_value", 3);
    }

    @Test
    @DisplayName("Header-only CSV — returns zero loaded, no errors")
    void headerOnlyCsv_shouldReturnZeroLoaded() throws Exception {
        String csv = "DateTime,Sender,Receiver,Amount,ID\n";

        MvcResult result = performUpload(csv);
        UploadResult uploadResult = parseResponse(result, UploadResult.class);

        assertThat(uploadResult.getSuccessfullyLoaded()).isEqualTo(0);
        assertThat(uploadResult.getErrors()).isEmpty();
    }

    @Test
    @DisplayName("CSV with 'Value' column name works same as 'Amount'")
    void csvWithValueColumnName_shouldWork() throws Exception {
        String csv = """
                DateTime,Sender,Receiver,Value,ID
                2026-02-20T12:00,Bank A,Bank B,100,1
                2026-02-20T13:00,Bank A,Bank B,200,2
                """;

        MvcResult result = performUpload(csv);
        UploadResult uploadResult = parseResponse(result, UploadResult.class);

        assertThat(uploadResult.getSuccessfullyLoaded()).isEqualTo(2);
        assertThat(uploadResult.getErrors()).isEmpty();
    }

    @Test
    @DisplayName("Duplicate IDs within same batch — last value wins")
    void duplicateIds_shouldOverwriteInBatch() throws Exception {
        String csv = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-20T12:00,Bank A,Bank B,100,SAME_ID
                2026-02-20T13:00,Bank C,Bank D,999,SAME_ID
                """;

        performUpload(csv);

        IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
        assertThat(cache.size()).isEqualTo(1);

        Payment stored = cache.get("SAME_ID");
        assertThat(stored.getValue()).isEqualTo(999);
        assertThat(stored.getSender()).isEqualTo("Bank C");
    }

    @Test
    @DisplayName("Re-upload same file — all records reported as updated, not new")
    void reUploadSameFile_shouldReportUpdatedRecords() throws Exception {
        performUpload(TEST_CSV);

        MvcResult result = performUpload(TEST_CSV);
        UploadResult uploadResult = parseResponse(result, UploadResult.class);

        assertThat(uploadResult.getSuccessfullyLoaded()).isEqualTo(15);
        assertThat(uploadResult.getNewRecords()).isEqualTo(0);
        assertThat(uploadResult.getUpdatedRecords()).isEqualTo(15);
    }

    @Test
    @DisplayName("Multiple uploads accumulate data in cache")
    void multipleUploads_shouldAccumulateData() throws Exception {
        String csv1 = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-20T12:00,Bank A,Bank B,100,1
                """;
        String csv2 = """
                DateTime,Sender,Receiver,Amount,ID
                2026-02-20T13:00,Bank C,Bank D,200,2
                """;

        performUpload(csv1);
        performUpload(csv2);

        IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
        assertThat(cache.size()).isEqualTo(2);
    }
}
