package com.zemnitskiy.dpps.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

/** Response DTO for the CSV upload endpoint. Tracks loaded, new, updated counts and per-field errors. */
@Data
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class UploadResult {

    private int successfullyLoaded;
    private int newRecords;
    private int updatedRecords;
    private Map<String, Integer> errors = new LinkedHashMap<>();

    public void incrementError(String fieldName) {
        errors.merge("missing_" + fieldName.toLowerCase(), 1, Integer::sum);
    }

    public void incrementInvalidValue() {
        errors.merge("invalid_value", 1, Integer::sum);
    }
}
