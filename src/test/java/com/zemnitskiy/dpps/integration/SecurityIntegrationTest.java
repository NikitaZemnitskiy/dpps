package com.zemnitskiy.dpps.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.security.test.context.support.WithAnonymousUser;
import org.springframework.security.test.context.support.WithMockUser;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("Security")
@Tag("integration")
class SecurityIntegrationTest extends BaseIntegrationTest {

    @Test
    @WithAnonymousUser
    @DisplayName("Unauthenticated request to protected endpoint returns 401")
    void unauthenticatedRequest_shouldReturn401() throws Exception {
        mockMvc.perform(get("/api/payments")
                        .param("from", "2026-02-20T00:00")
                        .param("to", "2026-02-20T23:59"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @WithMockUser
    @DisplayName("Authenticated request to protected endpoint returns 200")
    void authenticatedRequest_shouldReturn200() throws Exception {
        mockMvc.perform(get("/api/payments")
                        .param("from", "2026-02-20T00:00")
                        .param("to", "2026-02-20T23:59"))
                .andExpect(status().isOk());
    }

    @Test
    @WithAnonymousUser
    @DisplayName("Health endpoint is accessible without authentication")
    void healthEndpoint_shouldBeAccessibleWithoutAuth() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk());
    }
}
