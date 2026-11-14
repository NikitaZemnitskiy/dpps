package com.zemnitskiy.dpps.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zemnitskiy.dpps.controller.PaymentController;
import com.zemnitskiy.dpps.exception.ClusterUnavailableException;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.service.PartitionLossGuard;
import com.zemnitskiy.dpps.service.PaymentService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Consumer;

import static com.zemnitskiy.dpps.support.TestData.at;
import static com.zemnitskiy.dpps.support.TestData.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/** The streamed GET response: what reaches the client when everything works, and when it doesn't. */
@DisplayName("PaymentController streaming")
@Tag("unit")
class PaymentControllerTest {

    private final PaymentService service = mock(PaymentService.class);
    private final PaymentController controller = new PaymentController(service, mock(PartitionLossGuard.class),
            new ObjectMapper().registerModule(new JavaTimeModule()));

    private StreamingResponseBody body() {
        return controller.getPayments(at("2026-02-20T00:00"), at("2026-02-20T23:59:59")).getBody();
    }

    private void serviceStreams(int payments, RuntimeException failure) {
        doAnswer(invocation -> {
            Consumer<Payment> consumer = invocation.getArgument(1);
            for (int i = 0; i < payments; i++) {
                consumer.accept(payment("p" + i, "2026-02-20T12:00", "A", "B", "1.50"));
            }
            if (failure != null) {
                throw failure;
            }
            return null;
        }).when(service).streamPayments(any(TimeRange.class), any());
    }

    @Test
    @DisplayName("writes a complete JSON array and flushes once, not once per payment")
    void completeArray() throws IOException {
        serviceStreams(500, null);
        FlushCountingStream output = new FlushCountingStream();

        body().writeTo(output);

        assertThat(output.toString()).startsWith("[{\"id\":\"p0\"").endsWith("\"value\":1.50}]");
        assertThat(output.flushes).isLessThanOrEqualTo(1);
    }

    @Test
    @DisplayName("a failure before anything was sent leaves the response untouched, so a proper error can follow")
    void failureBeforeData() {
        ClusterUnavailableException failure = new ClusterUnavailableException("node left", null);
        serviceStreams(3, failure);
        FlushCountingStream output = new FlushCountingStream();

        assertThatThrownBy(() -> body().writeTo(output)).isSameAs(failure);
        assertThat(output.size()).isZero();
    }

    private static final class FlushCountingStream extends ByteArrayOutputStream {
        private int flushes;

        @Override
        public void flush() {
            flushes++;
        }
    }
}
