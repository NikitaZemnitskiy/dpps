package com.zemnitskiy.dpps.controller;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.service.PaymentService;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * REST controller for payment CRUD operations: CSV upload, retrieval, and deletion.
 */
@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Validated
@Slf4j
public class PaymentController {

    private final PaymentService paymentService;
    private final ObjectMapper objectMapper;

    /**
     * Uploads a CSV file and stores parsed payments in the distributed cache.
     * Invalid rows are skipped and reported in the response.
     *
     * @param file CSV file with header: DateTime, Sender, Receiver, Amount, ID
     * @return upload summary with counts of new/updated records and errors
     */
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<UploadResult> uploadPayments(@RequestParam("file") MultipartFile file) {
        log.info("POST /api/payments/upload file={} size={}", file.getOriginalFilename(), file.getSize());
        if (file.isEmpty()) {
            throw new IllegalArgumentException("Uploaded file is empty");
        }
        UploadResult result = paymentService.uploadPayments(file);
        return ResponseEntity.ok(result);
    }

    /**
     * Streams payments within the given time range as a JSON array (max 1 week).
     * Uses {@link StreamingResponseBody} with Jackson {@link JsonGenerator}
     * to write directly to the HTTP output stream — never accumulates all payments in memory.
     *
     * @param from start of the range (ISO 8601)
     * @param to   end of the range (ISO 8601)
     */
    @GetMapping
    public ResponseEntity<StreamingResponseBody> getPayments(
            @RequestParam @NotBlank String from,
            @RequestParam @NotBlank String to) {
        log.info("GET /api/payments from={} to={}", from, to);

        // Validate BEFORE starting the stream — exceptions here produce 400
        paymentService.validateTimeRange(from, to);

        StreamingResponseBody body = outputStream -> {
            try (JsonGenerator gen = objectMapper.getFactory().createGenerator(outputStream)) {
                gen.writeStartArray();
                paymentService.streamPayments(from, to, payment -> {
                    try {
                        gen.writeObject(payment);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                gen.writeEndArray();
            }
        };

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(body);
    }

    /**
     * Deletes payments. If both {@code from} and {@code to} are provided,
     * deletes only payments within that range; otherwise clears the entire cache.
     */
    @DeleteMapping
    public ResponseEntity<DeleteResult> deletePayments(
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to) {
        log.info("DELETE /api/payments from={} to={}", from, to);
        DeleteResult result = paymentService.deletePayments(from, to);
        return ResponseEntity.ok(result);
    }
}
