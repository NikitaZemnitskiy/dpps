package com.zemnitskiy.dpps.exception;

import com.zemnitskiy.dpps.dto.ApiError;
import com.zemnitskiy.dpps.util.DateTimes;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Maps exceptions to {@link ApiError} responses. Client errors get a precise message; unexpected server errors
 * get a reference to the log entry instead of internal details.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    private static final int MAX_ECHOED_VALUE_LENGTH = 100;
    private static final String RETRY_AFTER_SECONDS = "5";

    @ExceptionHandler(InvalidRequestException.class)
    public ResponseEntity<ApiError> handleInvalidRequest(InvalidRequestException ex) {
        return error(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(CsvParsingException.class)
    public ResponseEntity<ApiError> handleCsvParsing(CsvParsingException ex) {
        log.warn("Rejected CSV upload: {}", ex.getMessage());
        return error(HttpStatus.BAD_REQUEST, "CSV Parsing Error", ex.getMessage());
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ApiError> handleMissingParameter(MissingServletRequestParameterException ex) {
        return error(HttpStatus.BAD_REQUEST, "Required parameter '" + ex.getParameterName() + "' is missing");
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiError> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        return error(HttpStatus.BAD_REQUEST, "Invalid value '" + echo(ex.getValue()) + "' for parameter '"
                + ex.getName() + "': expected " + expectedValue(ex.getParameter()));
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<ApiError> handleMaxUploadSize(MaxUploadSizeExceededException ex) {
        return error(HttpStatus.PAYLOAD_TOO_LARGE, "The file exceeds the maximum upload size");
    }

    @ExceptionHandler(InsufficientStorageException.class)
    public ResponseEntity<ApiError> handleInsufficientStorage(InsufficientStorageException ex) {
        log.warn("Upload refused: {}", ex.getMessage());
        return error(HttpStatus.INSUFFICIENT_STORAGE, ex.getMessage());
    }

    @ExceptionHandler(ClusterUnavailableException.class)
    public ResponseEntity<ApiError> handleClusterUnavailable(ClusterUnavailableException ex) {
        log.warn("Cluster operation failed: {}", ex.getMessage(), ex);
        HttpStatus status = HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(new ApiError(status.value(), status.getReasonPhrase(), ex.getMessage()));
    }

    /**
     * Everything else. Spring MVC's own exceptions (unsupported media type, missing multipart part, ...) keep
     * their status code; anything unexpected becomes a 500 whose message points to the log entry.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleOther(Exception ex, HttpServletResponse response) {
        if (ex instanceof ErrorResponse springError) {
            HttpStatusCode status = springError.getStatusCode();
            String detail = springError.getBody().getDetail();
            return error(status, detail != null ? detail : reasonPhrase(status));
        }
        if (response.isCommitted()) {
            // E.g. a streamed response broke off half-way (often just a client that disconnected).
            log.warn("Request failed after the response was committed: {}", ex.toString());
            return null;
        }
        String reference = UUID.randomUUID().toString().substring(0, 8);
        log.error("Unexpected error [reference {}]", reference, ex);
        return error(HttpStatus.INTERNAL_SERVER_ERROR,
                "Unexpected server error (reference " + reference + "); the details are in the server log");
    }

    private static ResponseEntity<ApiError> error(HttpStatusCode status, String message) {
        return error(status, reasonPhrase(status), message);
    }

    private static ResponseEntity<ApiError> error(HttpStatusCode status, String error, String message) {
        return ResponseEntity.status(status).body(new ApiError(status.value(), error, message));
    }

    private static String reasonPhrase(HttpStatusCode status) {
        HttpStatus known = HttpStatus.resolve(status.value());
        return known != null ? known.getReasonPhrase() : "Error";
    }

    private static String expectedValue(MethodParameter parameter) {
        ResolvableType type = ResolvableType.forMethodParameter(parameter);
        Class<?> target = Collection.class.isAssignableFrom(type.toClass())
                ? type.asCollection().getGeneric(0).toClass()
                : type.toClass();
        if (target == LocalDateTime.class) {
            return DateTimes.EXPECTED_FORMAT;
        }
        if (target.isEnum()) {
            return "one of " + Arrays.stream(target.getEnumConstants()).map(String::valueOf)
                    .collect(Collectors.joining(", "));
        }
        return "a value of type " + target.getSimpleName();
    }

    private static String echo(Object value) {
        String text = value instanceof Object[] array
                ? Arrays.stream(array).map(String::valueOf).collect(Collectors.joining(","))
                : String.valueOf(value);
        return text.length() > MAX_ECHOED_VALUE_LENGTH ? text.substring(0, MAX_ECHOED_VALUE_LENGTH) + "..." : text;
    }
}
