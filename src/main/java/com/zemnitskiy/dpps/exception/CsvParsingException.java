package com.zemnitskiy.dpps.exception;

/**
 * The uploaded file can't be processed as a payments CSV (HTTP 400): its header lacks required columns or the
 * CSV syntax is broken (e.g. an unterminated quote). Individual invalid rows don't cause this exception; they
 * are skipped and reported in the upload result.
 */
public class CsvParsingException extends RuntimeException {

    public CsvParsingException(String message) {
        super(message);
    }

    public CsvParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
