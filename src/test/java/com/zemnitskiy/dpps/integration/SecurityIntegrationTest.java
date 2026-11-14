package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.security.Roles;
import com.zemnitskiy.dpps.support.TestJwtIssuer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.security.test.context.support.WithAnonymousUser;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Security with real, signed access tokens validated by the application's resource-server configuration
 * against a local JWK set (see {@link TestJwtIssuer}).
 */
@DisplayName("Security")
@Tag("integration")
@WithAnonymousUser
class SecurityIntegrationTest extends BaseIntegrationTest {

    private static final TestJwtIssuer ISSUER = TestJwtIssuer.get();

    private static String readerToken() {
        return ISSUER.token("viewer").roles(Roles.READ).bearer();
    }

    private static String writerToken() {
        return ISSUER.token("user").roles(Roles.READ, Roles.WRITE).bearer();
    }

    private static MockHttpServletRequestBuilder getPayments() {
        return get("/api/payments").param("from", "2026-02-20T00:00").param("to", "2026-02-20T23:59:59");
    }

    private static MockHttpServletRequestBuilder getStatistics() {
        return get("/api/statistics").param("aggregation", "BY_DATE").param("metrics", "GENERAL");
    }

    private static MockHttpServletRequestBuilder upload() {
        return multipart("/api/payments/upload").file(csvFile(TEST_CSV));
    }

    @Nested
    @DisplayName("Authentication")
    class Authentication {

        @Test
        @DisplayName("no token: 401 with a Bearer challenge")
        void noToken() throws Exception {
            mockMvc.perform(getPayments())
                    .andExpect(status().isUnauthorized())
                    .andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, containsString("Bearer")));
        }

        @Test
        @DisplayName("a valid token is accepted, also for the streamed response")
        void validToken() throws Exception {
            MvcResult started = mockMvc.perform(getPayments().header(HttpHeaders.AUTHORIZATION, readerToken()))
                    .andExpect(request().asyncStarted())
                    .andReturn();
            mockMvc.perform(asyncDispatch(started)).andExpect(status().isOk());
        }

        @Test
        @DisplayName("forged signature: 401")
        void forgedSignature() throws Exception {
            String forged = ISSUER.token("mallory").roles(Roles.READ, Roles.WRITE)
                    .signedWith(TestJwtIssuer.newKey("test-signing-key")).bearer();

            mockMvc.perform(getStatistics().header(HttpHeaders.AUTHORIZATION, forged))
                    .andExpect(status().isUnauthorized());
        }

        @Test
        @DisplayName("expired token: 401")
        void expired() throws Exception {
            String expired = ISSUER.token("user").roles(Roles.READ).expiresAt(Instant.now().minusSeconds(300)).bearer();

            mockMvc.perform(getStatistics().header(HttpHeaders.AUTHORIZATION, expired))
                    .andExpect(status().isUnauthorized())
                    .andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, containsString("expired")));
        }

        @Test
        @DisplayName("token from another issuer (e.g. another realm): 401")
        void wrongIssuer() throws Exception {
            String foreign = ISSUER.token("user").roles(Roles.READ).issuer("http://evil.example/realms/x").bearer();

            mockMvc.perform(getStatistics().header(HttpHeaders.AUTHORIZATION, foreign))
                    .andExpect(status().isUnauthorized());
        }

        @Test
        @DisplayName("token meant for another service (wrong audience): 401")
        void wrongAudience() throws Exception {
            String otherAudience = ISSUER.token("user").roles(Roles.READ).audience("some-other-api").bearer();

            mockMvc.perform(getStatistics().header(HttpHeaders.AUTHORIZATION, otherAudience))
                    .andExpect(status().isUnauthorized());
        }
    }

    @Nested
    @DisplayName("Authorization by realm role")
    class Authorization {

        @Test
        @DisplayName("payments-read may read payments and statistics")
        void readerCanRead() throws Exception {
            mockMvc.perform(getStatistics().header(HttpHeaders.AUTHORIZATION, readerToken()))
                    .andExpect(status().isOk());
            mockMvc.perform(getPayments().header(HttpHeaders.AUTHORIZATION, readerToken()))
                    .andExpect(request().asyncStarted());
        }

        @Test
        @DisplayName("payments-read may neither upload nor delete")
        void readerCannotWrite() throws Exception {
            loadTestDataAsWriter();

            mockMvc.perform(upload().header(HttpHeaders.AUTHORIZATION, readerToken()))
                    .andExpect(status().isForbidden());
            mockMvc.perform(delete("/api/payments").header(HttpHeaders.AUTHORIZATION, readerToken()))
                    .andExpect(status().isForbidden());

            assertThat(cache().size()).isEqualTo(15);
        }

        @Test
        @DisplayName("payments-write may upload and delete")
        void writerCanWrite() throws Exception {
            mockMvc.perform(upload().header(HttpHeaders.AUTHORIZATION, writerToken()))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.successfullyLoaded").value(15));
            mockMvc.perform(delete("/api/payments").header(HttpHeaders.AUTHORIZATION, writerToken()))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.deletedCount").value(15));
        }

        @Test
        @DisplayName("an authenticated user without API roles gets 403 everywhere")
        void noRoles() throws Exception {
            String noRoles = ISSUER.token("guest").roles("offline_access").bearer();

            mockMvc.perform(getStatistics().header(HttpHeaders.AUTHORIZATION, noRoles)).andExpect(status().isForbidden());
            mockMvc.perform(getPayments().header(HttpHeaders.AUTHORIZATION, noRoles)).andExpect(status().isForbidden());
            mockMvc.perform(upload().header(HttpHeaders.AUTHORIZATION, noRoles)).andExpect(status().isForbidden());
        }

        @Test
        @DisplayName("anything not explicitly allowed is denied, even with all roles")
        void denyByDefault() throws Exception {
            mockMvc.perform(put("/api/payments").header(HttpHeaders.AUTHORIZATION, writerToken()))
                    .andExpect(status().isForbidden());
            mockMvc.perform(get("/api/internal").header(HttpHeaders.AUTHORIZATION, writerToken()))
                    .andExpect(status().isForbidden());
            mockMvc.perform(get("/actuator/env").header(HttpHeaders.AUTHORIZATION, writerToken()))
                    .andExpect(status().isForbidden());
        }

        private void loadTestDataAsWriter() throws Exception {
            mockMvc.perform(upload().header(HttpHeaders.AUTHORIZATION, writerToken())).andExpect(status().isOk());
        }
    }

    @Nested
    @DisplayName("Health endpoint")
    class Health {

        @Test
        @DisplayName("is public but shows details only to authenticated readers")
        void publicStatusOnly() throws Exception {
            mockMvc.perform(get("/actuator/health"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("UP"))
                    .andExpect(jsonPath("$.components").doesNotExist());
            mockMvc.perform(get("/actuator/health").header(HttpHeaders.AUTHORIZATION, readerToken()))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.components.ignite.details.serverNodes").value(1));
        }
    }
}
