package com.zemnitskiy.dpps.exception;

/** Thrown when an I/O or unexpected error occurs during payment upload/storage. */
public class PaymentProcessingException extends RuntimeException {

    public PaymentProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
