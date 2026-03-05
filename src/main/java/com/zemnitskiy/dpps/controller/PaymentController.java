package com.zemnitskiy.dpps.controller;

import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.service.PaymentService;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

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
     * Retrieves payments within the given time range (max 1 week).
     *
     * @param from start of the range (ISO 8601)
     * @param to   end of the range (ISO 8601)
     */
    @GetMapping
    public ResponseEntity<List<Payment>> getPayments(
            @RequestParam @NotBlank String from,
            @RequestParam @NotBlank String to) {
        log.info("GET /api/payments from={} to={}", from, to);
        List<Payment> payments = paymentService.getPayments(from, to);
        log.debug("GET /api/payments returned {} records", payments.size());
        return ResponseEntity.ok(payments);
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
