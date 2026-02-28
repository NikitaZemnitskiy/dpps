package com.zemnitskiy.dpps;

import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.dto.StatisticsResponse.*;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@WithMockUser
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DppsApplicationTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Ignite ignite;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    private static final String TEST_CSV =
            "DateTime,Sender,Receiver,Amount,ID\n" +
            "2026-02-20T12:00,Bank A,Bank B,100,1\n" +
            "2026-02-20T16:00,Bank B,Bank C,50,2\n" +
            "2026-02-20T20:00,Bank A,Bank C,200,3\n" +
            "2026-02-20T21:00,Bank C,Bank A,50,4\n" +
            "2026-02-20T22:00,Bank B,Bank A,100,5\n" +
            "2026-02-21T02:00,Bank A,Bank B,300,6\n" +
            "2026-02-21T03:00,Bank B,Bank C,150,7\n" +
            "2026-02-21T06:00,Bank C,Bank A,200,8\n" +
            "2026-02-21T07:00,Bank A,Bank C,100,9\n" +
            "2026-02-21T10:00,Bank C,Bank B,50,10\n" +
            "2026-02-21T11:00,Bank B,Bank A,50,11\n" +
            "2026-02-21T15:00,Bank A,Bank B,100,12\n" +
            "2026-02-21T16:00,Bank B,Bank C,200,13\n" +
            "2026-02-21T22:00,Bank C,Bank A,150,14\n" +
            "2026-02-21T23:00,Bank A,Bank B,50,15\n";

    @BeforeEach
    void clearCache() {
        IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
        if (cache != null) {
            cache.clear();
        }
    }

    @Test
    void uploadCsv_shouldReturnSuccessCount() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "payments.csv", "text/csv", TEST_CSV.getBytes());

        MvcResult result = mockMvc.perform(multipart("/api/payments/upload").file(file))
                .andExpect(status().isOk())
                .andReturn();

        UploadResult uploadResult = objectMapper.readValue(
                result.getResponse().getContentAsString(), UploadResult.class);

        assertThat(uploadResult.getSuccessfullyLoaded()).isEqualTo(15);
        assertThat(uploadResult.getErrors()).isEmpty();
    }

    @Test
    void uploadCsv_withInvalidRows_shouldSkipAndReport() throws Exception {
        String csvWithErrors =
                "DateTime,Sender,Receiver,Amount,ID\n" +
                "2026-02-20T12:00,Bank A,Bank B,100,1\n" +
                "2026-02-20T13:00,,Bank C,50,2\n" +     // missing sender
                "2026-02-20T14:00,Bank A,Bank B,,3\n";   // missing value

        MockMultipartFile file = new MockMultipartFile(
                "file", "payments.csv", "text/csv", csvWithErrors.getBytes());

        MvcResult result = mockMvc.perform(multipart("/api/payments/upload").file(file))
                .andExpect(status().isOk())
                .andReturn();

        UploadResult uploadResult = objectMapper.readValue(
                result.getResponse().getContentAsString(), UploadResult.class);

        assertThat(uploadResult.getSuccessfullyLoaded()).isEqualTo(1);
        assertThat(uploadResult.getErrors()).containsEntry("missing_sender", 1);
        assertThat(uploadResult.getErrors()).containsEntry("missing_value", 1);
    }

    @Test
    void getPayments_shouldReturnPaymentsInRange() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/payments")
                        .param("from", "2026-02-20T00:00")
                        .param("to", "2026-02-20T23:59"))
                .andExpect(status().isOk())
                .andReturn();

        List<Payment> payments = objectMapper.readValue(
                result.getResponse().getContentAsString(), new TypeReference<>() {});

        assertThat(payments).hasSize(5);
    }

    @Test
    void getPayments_shouldRejectRangeExceedingOneWeek() throws Exception {
        mockMvc.perform(get("/api/payments")
                        .param("from", "2026-02-01T00:00")
                        .param("to", "2026-02-20T00:00"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void deletePayments_withRange_shouldReturnDeletedCount() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(delete("/api/payments")
                        .param("from", "2026-02-20T00:00")
                        .param("to", "2026-02-20T23:59"))
                .andExpect(status().isOk())
                .andReturn();

        DeleteResult deleteResult = objectMapper.readValue(
                result.getResponse().getContentAsString(), DeleteResult.class);

        assertThat(deleteResult.deletedCount()).isEqualTo(5);
    }

    @Test
    void deletePayments_withoutRange_shouldDeleteAll() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(delete("/api/payments"))
                .andExpect(status().isOk())
                .andReturn();

        DeleteResult deleteResult = objectMapper.readValue(
                result.getResponse().getContentAsString(), DeleteResult.class);

        assertThat(deleteResult.deletedCount()).isEqualTo(15);
    }

    @Test
    void statistics_byDate_generalAndValue() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_DATE")
                        .param("metrics", "GENERAL", "VALUE"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = objectMapper.readValue(
                result.getResponse().getContentAsString(), StatisticsResponse.class);

        Map<String, GroupStats> data = response.data();
        assertThat(data).containsKeys("2026-02-20", "2026-02-21");

        // Test case 1: 2026-02-20
        GroupStats day20 = data.get("2026-02-20");
        assertThat(day20.general().count()).isEqualTo(5);
        assertThat(day20.value().min()).isEqualTo(50);
        assertThat(day20.value().max()).isEqualTo(200);
        assertThat(day20.value().sum()).isEqualTo(500);
        assertThat(day20.value().average()).isEqualTo(100);

        // Test case 1: 2026-02-21
        GroupStats day21 = data.get("2026-02-21");
        assertThat(day21.general().count()).isEqualTo(10);
        assertThat(day21.value().min()).isEqualTo(50);
        assertThat(day21.value().max()).isEqualTo(300);
        assertThat(day21.value().sum()).isEqualTo(1350);
        assertThat(day21.value().average()).isEqualTo(135);
    }

    @Test
    void statistics_byBank_generalValueAndDateTime() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_BANK")
                        .param("metrics", "GENERAL", "VALUE", "DATETIME"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = objectMapper.readValue(
                result.getResponse().getContentAsString(), StatisticsResponse.class);

        Map<String, GroupStats> data = response.data();

        // Bank A: Count 11, Sum -300
        GroupStats bankA = data.get("Bank A");
        assertThat(bankA.general().count()).isEqualTo(11);
        assertThat(bankA.value().min()).isEqualTo(-300);
        assertThat(bankA.value().max()).isEqualTo(200);
        assertThat(bankA.value().sum()).isEqualTo(-300);
        assertThat(bankA.dateTime().min()).isEqualTo("2026-02-20T12:00");
        assertThat(bankA.dateTime().max()).isEqualTo("2026-02-21T23:00");

        // Bank B: Count 10, Sum 50
        GroupStats bankB = data.get("Bank B");
        assertThat(bankB.general().count()).isEqualTo(10);
        assertThat(bankB.value().sum()).isEqualTo(50);

        // Bank C: Count 9, Sum 250
        GroupStats bankC = data.get("Bank C");
        assertThat(bankC.general().count()).isEqualTo(9);
        assertThat(bankC.value().sum()).isEqualTo(250);
    }

    @Test
    void statistics_byConnection_general() throws Exception {
        loadTestData();

        MvcResult result = mockMvc.perform(get("/api/statistics")
                        .param("aggregation", "BY_CONNECTION")
                        .param("metrics", "GENERAL"))
                .andExpect(status().isOk())
                .andReturn();

        StatisticsResponse response = objectMapper.readValue(
                result.getResponse().getContentAsString(), StatisticsResponse.class);

        Map<String, GroupStats> data = response.data();

        assertThat(data.get("Bank A-Bank B").general().count()).isEqualTo(4);
        assertThat(data.get("Bank A-Bank C").general().count()).isEqualTo(2);
        assertThat(data.get("Bank B-Bank A").general().count()).isEqualTo(2);
        assertThat(data.get("Bank B-Bank C").general().count()).isEqualTo(3);
        assertThat(data.get("Bank C-Bank A").general().count()).isEqualTo(3);
        assertThat(data.get("Bank C-Bank B").general().count()).isEqualTo(1);
    }

    private void loadTestData() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "payments.csv", "text/csv", TEST_CSV.getBytes());
        mockMvc.perform(multipart("/api/payments/upload").file(file))
                .andExpect(status().isOk());
    }
}
