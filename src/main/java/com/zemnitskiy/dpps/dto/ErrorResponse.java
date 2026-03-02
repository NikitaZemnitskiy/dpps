package com.zemnitskiy.dpps.dto;

/** Unified error response body returned by {@link com.zemnitskiy.dpps.exception.GlobalExceptionHandler}. */
public record ErrorResponse(int status, String error, String message) {
}
