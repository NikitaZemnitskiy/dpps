package com.zemnitskiy.dpps.exception;

/**
 * The cluster's off-heap memory is nearly full, so an upload is refused or stopped (HTTP 507).
 * <p>
 * Ignite halts a node whose data region overflows, so uploads stop at a high-watermark instead of
 * running into that limit.
 */
public class InsufficientStorageException extends RuntimeException {

    public InsufficientStorageException(String message) {
        super(message);
    }
}
