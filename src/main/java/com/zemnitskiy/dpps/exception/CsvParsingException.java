package com.zemnitskiy.dpps.exception;

public class CsvParsingException extends RuntimeException {

    public CsvParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
