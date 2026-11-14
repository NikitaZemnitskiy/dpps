package com.zemnitskiy.dpps.controller;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.exception.InvalidRequestException;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.service.PartitionLossGuard;
import com.zemnitskiy.dpps.service.PaymentService;
import com.zemnitskiy.dpps.util.DateTimes;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.security.Principal;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Payment management: CSV upload, retrieval by time range and deletion.
 * Date-time parameters accept the format described in {@link com.zemnitskiy.dpps.util.DateTimes}; ranges include
 * both bounds.
 */
@RestController
@RequestMapping("/api/payments")
@Slf4j
public class PaymentController {

    static final Duration MAX_QUERY_RANGE = Duration.ofDays(7);

    private final PaymentService paymentService;
    private final PartitionLossGuard partitionLossGuard;
    private final ObjectMapper objectMapper;
    private final ObjectWriter paymentWriter;

    public PaymentController(PaymentService paymentService, PartitionLossGuard partitionLossGuard,
                             ObjectMapper objectMapper) {
        this.paymentService = paymentService;
        this.partitionLossGuard = partitionLossGuard;
        this.objectMapper = objectMapper;
        // Jackson flushes after every value by default - one HTTP chunk and system call per payment.
        this.paymentWriter = objectMapper.writerFor(Payment.class).without(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
    }

    /**
     * Uploads a CSV file ({@code DateTime,Sender,Receiver,Amount,ID}) and stores its valid rows.
     * Invalid rows are skipped and reported in the response.
     */
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public UploadResult uploadPayments(@RequestParam("file") MultipartFile file, Principal principal) {
        if (file.isEmpty()) {
            throw new InvalidRequestException("The uploaded file is empty");
        }
        log.info("Upload of '{}' ({} bytes) by {}", file.getOriginalFilename(), file.getSize(), principal.getName());
        return paymentService.uploadPayments(file);
    }

    /**
     * Streams the payments within {@code [from, to]} (at most 7 days) as a JSON array, written entry by entry so
     * the result is never held in memory. If reading fails before any data has been sent, the client gets a normal
     * error response; if it fails later, the connection is aborted (see GlobalExceptionHandler), so a broken
     * transfer can never pass for a complete list.
     */
    @GetMapping
    public ResponseEntity<StreamingResponseBody> getPayments(@RequestParam LocalDateTime from,
                                                             @RequestParam LocalDateTime to) {
        TimeRange range = TimeRange.of(from, to);
        if (range.length().compareTo(MAX_QUERY_RANGE) > 0) {
            throw new InvalidRequestException("The time range must not exceed 7 days");
        }
        partitionLossGuard.ensureNoDataLoss(); // fail before the response turns into a stream
        StreamingResponseBody body = output -> writePayments(output, range);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(body);
    }

    /**
     * Deletes the payments within {@code [from, to]}, or all payments when neither bound is given.
     * Giving only one bound is rejected - it is far more likely a mistake than a request to wipe everything.
     */
    @DeleteMapping
    public DeleteResult deletePayments(@RequestParam(required = false) LocalDateTime from,
                                       @RequestParam(required = false) LocalDateTime to,
                                       HttpServletRequest request, Principal principal) {
        // A parameter that is present but empty (?from=&to= or ?from&to) must never mean "no range".
        rejectPresentButEmpty(request, "from", from);
        rejectPresentButEmpty(request, "to", to);
        if ((from == null) != (to == null)) {
            throw new InvalidRequestException(
                    "Provide both 'from' and 'to' to delete a time range, or neither to delete all payments");
        }
        TimeRange range = TimeRange.of(from, to);
        log.info("Delete of payments in {} by {}", range, principal.getName());
        return new DeleteResult(paymentService.deletePayments(range));
    }

    private static void rejectPresentButEmpty(HttpServletRequest request, String name, LocalDateTime value) {
        if (value == null && request.getParameterMap().containsKey(name)) {
            throw new InvalidRequestException("Parameter '" + name + "' is empty; expected " + DateTimes.EXPECTED_FORMAT);
        }
    }

    private void writePayments(OutputStream output, TimeRange range) throws IOException {
        // Deliberately not try-with-resources: closing the generator after a failure would flush the buffered
        // start of the array and commit a 200 response. Left unclosed, unsent output is simply dropped.
        JsonGenerator generator = objectMapper.getFactory().createGenerator(output);
        generator.writeStartArray();
        paymentService.streamPayments(range, payment -> {
            try {
                paymentWriter.writeValue(generator, payment);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        generator.writeEndArray();
        generator.close();
    }
}
