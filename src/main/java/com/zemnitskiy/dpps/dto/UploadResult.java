package com.zemnitskiy.dpps.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

/** Response DTO for the CSV upload endpoint. Tracks loaded count and per-field errors. */
@Data
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class UploadResult {

    private int successfullyLoaded;
    private Map<String, Integer> errors = new LinkedHashMap<>();

    public void incrementMissing(String fieldName) {
        errors.merge("missing_" + fieldName.toLowerCase(), 1, Integer::sum);
    }

    public void incrementInvalid(String fieldName) {
        errors.merge("invalid_" + fieldName.toLowerCase(), 1, Integer::sum);
    }
}
