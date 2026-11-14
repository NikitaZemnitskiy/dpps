package com.zemnitskiy.dpps.controller;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.exception.InvalidRequestException;
import com.zemnitskiy.dpps.model.TimeRange;
import com.zemnitskiy.dpps.service.PaymentService;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
@Slf4j
public class PaymentController {

    static final Duration MAX_QUERY_RANGE = Duration.ofDays(7);

    private final PaymentService paymentService;
    private final ObjectMapper objectMapper;

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
     * the result is never held in memory. If the scan fails half-way, the array is left unterminated: clients get
     * invalid JSON rather than a silently truncated list.
     */
    @GetMapping
    public ResponseEntity<StreamingResponseBody> getPayments(@RequestParam LocalDateTime from,
                                                             @RequestParam LocalDateTime to) {
        TimeRange range = TimeRange.of(from, to);
        if (range.length().compareTo(MAX_QUERY_RANGE) > 0) {
            throw new InvalidRequestException("The time range must not exceed 7 days");
        }
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
                                       Principal principal) {
        if ((from == null) != (to == null)) {
            throw new InvalidRequestException(
                    "Provide both 'from' and 'to' to delete a time range, or neither to delete all payments");
        }
        TimeRange range = TimeRange.of(from, to);
        log.info("Delete of payments in {} by {}", range, principal.getName());
        return new DeleteResult(paymentService.deletePayments(range));
    }

    private void writePayments(OutputStream output, TimeRange range) throws IOException {
        try (JsonGenerator generator = objectMapper.getFactory().createGenerator(output)) {
            generator.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);
            generator.writeStartArray();
            paymentService.streamPayments(range, payment -> {
                try {
                    generator.writeObject(payment);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            generator.writeEndArray();
        }
    }
}
