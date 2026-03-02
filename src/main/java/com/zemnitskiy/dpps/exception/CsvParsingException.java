package com.zemnitskiy.dpps.exception;

/** Thrown when CSV file parsing fails due to structural or encoding issues. */
public class CsvParsingException extends RuntimeException {

    public CsvParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
