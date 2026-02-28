package com.zemnitskiy.dpps;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.model.Payment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.junit.jupiter.api.BeforeEach;
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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Base class for all integration tests.
 * Spins up an embedded Ignite node, mocks Keycloak JWT, provides shared helpers.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@WithMockUser
public abstract class BaseIntegrationTest {

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected Ignite ignite;

    @MockitoBean
    protected JwtDecoder jwtDecoder;

    /**
     * 15 payment records across 2 days (2026-02-20 and 2026-02-21),
     * 3 banks (A, B, C), values from 50 to 300.
     */
    protected static final String TEST_CSV =
            """
                    DateTime,Sender,Receiver,Amount,ID
                    2026-02-20T12:00,Bank A,Bank B,100,1
                    2026-02-20T16:00,Bank B,Bank C,50,2
                    2026-02-20T20:00,Bank A,Bank C,200,3
                    2026-02-20T21:00,Bank C,Bank A,50,4
                    2026-02-20T22:00,Bank B,Bank A,100,5
                    2026-02-21T02:00,Bank A,Bank B,300,6
                    2026-02-21T03:00,Bank B,Bank C,150,7
                    2026-02-21T06:00,Bank C,Bank A,200,8
                    2026-02-21T07:00,Bank A,Bank C,100,9
                    2026-02-21T10:00,Bank C,Bank B,50,10
                    2026-02-21T11:00,Bank B,Bank A,50,11
                    2026-02-21T15:00,Bank A,Bank B,100,12
                    2026-02-21T16:00,Bank B,Bank C,200,13
                    2026-02-21T22:00,Bank C,Bank A,150,14
                    2026-02-21T23:00,Bank A,Bank B,50,15
                    """;
    @BeforeEach
    void clearCache() {
        IgniteCache<String, Payment> cache = ignite.cache(IgniteConfig.PAYMENTS_CACHE);
        if (cache != null) {
            cache.clear();
        }
    }

    protected void loadTestData() throws Exception {
        performUpload(TEST_CSV);
    }

    protected MvcResult performUpload(String csvContent) throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "payments.csv", "text/csv", csvContent.getBytes());
        return mockMvc.perform(multipart("/api/payments/upload").file(file))
                .andExpect(status().isOk())
                .andReturn();
    }

    protected <T> T parseResponse(MvcResult result, Class<T> type) throws Exception {
        return objectMapper.readValue(result.getResponse().getContentAsString(), type);
    }

    protected <T> T parseResponse(MvcResult result, TypeReference<T> typeRef) throws Exception {
        return objectMapper.readValue(result.getResponse().getContentAsString(), typeRef);
    }
}
