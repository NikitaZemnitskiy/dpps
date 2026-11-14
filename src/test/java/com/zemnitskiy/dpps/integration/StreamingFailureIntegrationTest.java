package com.zemnitskiy.dpps.integration;

import com.zemnitskiy.dpps.exception.ClusterUnavailableException;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.security.Roles;
import com.zemnitskiy.dpps.service.PaymentService;
import com.zemnitskiy.dpps.support.TestJwtIssuer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Consumer;

import static com.zemnitskiy.dpps.support.TestData.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

/**
 * What a client really receives over HTTP (real Tomcat, real token) when reading payments fails while
 * GET /api/payments is being streamed. Runs in its own context with its own, separate Ignite node.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "ignite.instance-name=streaming-failure-test",
        "ignite.discovery.addresses=127.0.0.1:48600..48609",
        "ignite.discovery.local-port=48600",
        "ignite.communication.local-port=48200"})
@DirtiesContext
@DisplayName("Streaming failures over real HTTP")
@Tag("integration")
class StreamingFailureIntegrationTest {

    private static final String QUERY = "/api/payments?from=2026-02-20T00:00&to=2026-02-26T23:59:59";

    @MockitoBean
    private PaymentService paymentService;

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void jwtValidation(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", TestJwtIssuer.get()::jwkSetUri);
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> TestJwtIssuer.ISSUER);
        registry.add("spring.security.oauth2.resourceserver.jwt.audiences", () -> TestJwtIssuer.AUDIENCE);
    }

    private HttpResponse<String> get() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + QUERY))
                .header("Authorization", TestJwtIssuer.get().token("viewer").roles(Roles.READ).bearer())
                .build();
        return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
                .send(request, HttpResponse.BodyHandlers.ofString());
    }

    private void streamThenFail(int payments, RuntimeException failure) {
        doAnswer(invocation -> {
            Consumer<Payment> consumer = invocation.getArgument(1);
            for (int i = 0; i < payments; i++) {
                consumer.accept(payment("p" + i, "2026-02-21T10:00", "Bank A", "Bank B", "100.00"));
            }
            throw failure;
        }).when(paymentService).streamPayments(any(TimeRange.class), any());
    }

    @Test
    @DisplayName("a complete stream is a complete JSON array")
    void complete() throws Exception {
        doAnswer(invocation -> {
            Consumer<Payment> consumer = invocation.getArgument(1);
            for (int i = 0; i < 20_000; i++) {
                consumer.accept(payment("p" + i, "2026-02-21T10:00", "Bank A", "Bank B", "100.00"));
            }
            return null;
        }).when(paymentService).streamPayments(any(TimeRange.class), any());

        HttpResponse<String> response = get();

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).startsWith("[{\"id\":\"p0\"").endsWith("\"value\":100.00}]");
    }

    @Test
    @DisplayName("failure before anything was sent: a proper 503 with an error body, not '200 ['")
    void failureBeforeData() throws Exception {
        doThrow(new ClusterUnavailableException("Reading payments failed because the cluster is unavailable", null))
                .when(paymentService).streamPayments(any(TimeRange.class), any());

        HttpResponse<String> response = get();

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).startsWith("{\"status\":503").contains("Reading payments failed");
    }

    @Test
    @DisplayName("failure after part of the data was sent: the transfer breaks instead of ending like a complete one")
    void failureMidStream() {
        streamThenFail(20_000, new ClusterUnavailableException("node left", null));

        assertThatThrownBy(this::get).isInstanceOf(IOException.class);
    }
}
