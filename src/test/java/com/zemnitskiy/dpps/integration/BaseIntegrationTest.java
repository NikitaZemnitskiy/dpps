package com.zemnitskiy.dpps.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zemnitskiy.dpps.config.IgniteConfig;
import com.zemnitskiy.dpps.dto.StatisticsResponse;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.security.Roles;
import com.zemnitskiy.dpps.support.TestData;
import com.zemnitskiy.dpps.support.TestJwtIssuer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Base class for integration tests: one Spring context with an embedded Ignite node, shared by all test
 * classes. Requests run as a user holding both API roles unless a test says otherwise; security tests send
 * real tokens signed by {@link TestJwtIssuer}.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@WithMockUser(roles = {Roles.READ, Roles.WRITE})
public abstract class BaseIntegrationTest {

    protected static final String TEST_CSV = TestData.TEST_CSV;

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected Ignite ignite;

    @DynamicPropertySource
    static void jwtValidation(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", TestJwtIssuer.get()::jwkSetUri);
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> TestJwtIssuer.ISSUER);
        registry.add("spring.security.oauth2.resourceserver.jwt.audiences", () -> TestJwtIssuer.AUDIENCE);
    }

    @BeforeEach
    void clearCache() {
        cache().clear();
    }

    protected IgniteCache<String, Payment> cache() {
        return ignite.cache(IgniteConfig.PAYMENTS_CACHE);
    }

    protected void loadTestData() throws Exception {
        upload(TEST_CSV);
    }

    /** Uploads {@code csv} and expects HTTP 200. */
    protected UploadResult upload(String csv) throws Exception {
        MvcResult result = mockMvc.perform(multipart("/api/payments/upload").file(csvFile(csv)))
                .andExpect(status().isOk())
                .andReturn();
        return parse(result, UploadResult.class);
    }

    protected static MockMultipartFile csvFile(String csv) {
        return new MockMultipartFile("file", "payments.csv", "text/csv", csv.getBytes());
    }

    /** GET /api/payments (a streamed response, so it completes in an async dispatch). */
    protected List<Payment> getPayments(String from, String to) throws Exception {
        MvcResult started = mockMvc.perform(get("/api/payments").param("from", from).param("to", to))
                .andExpect(request().asyncStarted())
                .andReturn();
        MvcResult result = mockMvc.perform(asyncDispatch(started))
                .andExpect(status().isOk())
                .andReturn();
        return parse(result, new TypeReference<>() {
        });
    }

    protected StatisticsResponse statistics(MockHttpServletRequestBuilder request) throws Exception {
        MvcResult result = mockMvc.perform(request).andExpect(status().isOk()).andReturn();
        return parse(result, StatisticsResponse.class);
    }

    protected <T> T parse(MvcResult result, Class<T> type) throws Exception {
        return objectMapper.readValue(result.getResponse().getContentAsString(), type);
    }

    protected <T> T parse(MvcResult result, TypeReference<T> type) throws Exception {
        return objectMapper.readValue(result.getResponse().getContentAsString(), type);
    }
}
