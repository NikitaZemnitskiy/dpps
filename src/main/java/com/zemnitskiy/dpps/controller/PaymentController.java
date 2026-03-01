package com.zemnitskiy.dpps.controller;

import com.zemnitskiy.dpps.dto.DeleteResult;
import com.zemnitskiy.dpps.dto.UploadResult;
import com.zemnitskiy.dpps.model.Payment;
import com.zemnitskiy.dpps.service.PaymentService;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Validated
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<UploadResult> uploadPayments(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            throw new IllegalArgumentException("Uploaded file is empty");
        }
        UploadResult result = paymentService.uploadPayments(file);
        return ResponseEntity.ok(result);
    }

    @GetMapping
    public ResponseEntity<List<Payment>> getPayments(
            @RequestParam @NotBlank String from,
            @RequestParam @NotBlank String to) {
        List<Payment> payments = paymentService.getPayments(from, to);
        return ResponseEntity.ok(payments);
    }

    @DeleteMapping
    public ResponseEntity<DeleteResult> deletePayments(
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to) {
        DeleteResult result = paymentService.deletePayments(from, to);
        return ResponseEntity.ok(result);
    }
}
