package com.zemnitskiy.dpps.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Response of the CSV upload.
 *
 * @param successfullyLoaded rows that were stored; a row whose ID already exists replaces the earlier payment
 * @param rejected           rows that were skipped because they failed validation
 * @param errors             validation problems by code (e.g. {@code missing_sender}, {@code invalid_datetime});
 *                           one rejected row can contribute several codes. Omitted when there are none.
 */
public record UploadResult(int successfullyLoaded,
                           int rejected,
                           @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Integer> errors) {

    public UploadResult {
        errors = errors == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(errors));
    }
}
