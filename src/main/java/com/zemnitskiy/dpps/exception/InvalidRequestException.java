package com.zemnitskiy.dpps.exception;

/** The request is well-formed but its parameters are not acceptable (HTTP 400). */
public class InvalidRequestException extends RuntimeException {

    public InvalidRequestException(String message) {
        super(message);
    }
}
