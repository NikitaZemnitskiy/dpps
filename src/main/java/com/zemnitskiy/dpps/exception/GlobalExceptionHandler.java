package com.zemnitskiy.dpps.exception;

import com.zemnitskiy.dpps.dto.ErrorResponse;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import java.time.format.DateTimeParseException;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    private static final String BAD_REQUEST = "Bad Request";

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(), BAD_REQUEST, ex.getMessage()));
    }

    @ExceptionHandler(DateTimeParseException.class)
    public ResponseEntity<ErrorResponse> handleDateTimeParse(DateTimeParseException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(), BAD_REQUEST,
                        "Invalid date-time format. Expected ISO 8601 (e.g. 2026-02-20T12:00)"));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(ConstraintViolationException ex) {
        String message = ex.getConstraintViolations().stream()
                .map(v -> {
                    String field = v.getPropertyPath().toString();
                    int dot = field.lastIndexOf('.');
                    if (dot >= 0) field = field.substring(dot + 1);
                    return "'" + field + "' " + v.getMessage();
                })
                .collect(Collectors.joining("; "));
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(), "Validation Error", message));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingParam(MissingServletRequestParameterException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(), BAD_REQUEST,
                        "Required parameter '" + ex.getParameterName() + "' is missing"));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(), BAD_REQUEST,
                        "Invalid value for parameter '" + ex.getName() + "': " + ex.getValue()));
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<ErrorResponse> handleMaxUploadSize(MaxUploadSizeExceededException ex) {
        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
                .body(new ErrorResponse(HttpStatus.PAYLOAD_TOO_LARGE.value(), "Payload Too Large",
                        "File size exceeds the maximum allowed upload size"));
    }

    @ExceptionHandler(CsvParsingException.class)
    public ResponseEntity<ErrorResponse> handleCsvParsing(CsvParsingException ex) {
        log.error("CSV parsing failed", ex);
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(), "CSV Parsing Error", ex.getMessage()));
    }

    @ExceptionHandler(PaymentProcessingException.class)
    public ResponseEntity<ErrorResponse> handlePaymentProcessing(PaymentProcessingException ex) {
        log.error("Payment processing failed", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Payment Processing Error", ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex) {
        log.error("Unexpected error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Internal Server Error", ex.getMessage()));
    }
}
