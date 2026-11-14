package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.dto.ApiError;
import com.zemnitskiy.dpps.exception.ClusterUnavailableException;
import com.zemnitskiy.dpps.exception.GlobalExceptionHandler;
import com.zemnitskiy.dpps.exception.PaymentDataLostException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.HttpMediaTypeNotSupportedException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("GlobalExceptionHandler")
@Tag("unit")
class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    @DisplayName("an unexpected error becomes a 500 that points to the log instead of leaking internals")
    void unexpectedError() throws Exception {
        ResponseEntity<ApiError> response = handler.handleOther(
                new IllegalStateException("password=secret at db-host-17"), new MockHttpServletResponse());

        assertThat(response.getStatusCode().value()).isEqualTo(500);
        assertThat(response.getBody().message())
                .matches("Unexpected server error \\(reference [0-9a-f]{8}\\); the details are in the server log")
                .doesNotContain("secret", "db-host");
    }

    @Test
    @DisplayName("Spring MVC exceptions keep their own status code")
    void springErrorsKeepStatus() throws Exception {
        ResponseEntity<ApiError> response = handler.handleOther(
                new HttpMediaTypeNotSupportedException(MediaType.APPLICATION_JSON, List.of(MediaType.MULTIPART_FORM_DATA)),
                new MockHttpServletResponse());

        assertThat(response.getStatusCode().value()).isEqualTo(415);
        assertThat(response.getBody().error()).isEqualTo("Unsupported Media Type");
    }

    @Test
    @DisplayName("an error after part of a stream was sent is rethrown, so the container aborts the connection")
    void committedResponse() {
        MockHttpServletResponse committed = new MockHttpServletResponse();
        committed.setCommitted(true);
        IllegalStateException broken = new IllegalStateException("broken stream");
        ClusterUnavailableException unavailable = new ClusterUnavailableException("node left", null);

        assertThatThrownBy(() -> handler.handleOther(broken, committed)).isSameAs(broken);
        assertThatThrownBy(() -> handler.handleClusterUnavailable(unavailable, committed)).isSameAs(unavailable);
    }

    @Test
    @DisplayName("output that was only buffered is discarded before the error body is written")
    void bufferedOutputDiscarded() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        response.getWriter().write("[{\"id\":\"1\"");

        handler.handleClusterUnavailable(new ClusterUnavailableException("node left", null), response);

        assertThat(response.getContentAsString()).isEmpty();
    }

    @Test
    @DisplayName("lost payments answer 503 without Retry-After, since retrying doesn't help")
    void dataLost() {
        ResponseEntity<ApiError> response = handler.handleDataLost(
                new PaymentDataLostException(3, 1024), new MockHttpServletResponse());

        assertThat(response.getStatusCode().value()).isEqualTo(503);
        assertThat(response.getHeaders().containsKey(HttpHeaders.RETRY_AFTER)).isFalse();
        assertThat(response.getBody().message()).contains("3 of 1024 partitions", "DELETE /api/payments");
    }

    @Test
    @DisplayName("a temporarily unavailable cluster answers 503 with Retry-After")
    void clusterUnavailable() {
        ResponseEntity<ApiError> response = handler.handleClusterUnavailable(
                new ClusterUnavailableException("The cluster topology kept changing", null), new MockHttpServletResponse());

        assertThat(response.getStatusCode().value()).isEqualTo(503);
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("5");
        assertThat(response.getBody().message()).isEqualTo("The cluster topology kept changing");
    }
}
