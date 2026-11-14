package com.zemnitskiy.dpps.service;

import com.zemnitskiy.dpps.dto.UploadResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/** Mutable counters collected while a CSV upload is parsed and stored (one instance per upload, one thread). */
public class UploadReport {

    private int loaded;
    private int rejected;
    private final Map<String, Integer> errors = new HashMap<>();

    /** Records rows that were written to the cluster. */
    public void addLoaded(int rows) {
        loaded += rows;
    }

    /** Records one rejected row together with all problems found in it. */
    public void reject(Collection<String> errorCodes) {
        rejected++;
        errorCodes.forEach(code -> errors.merge(code, 1, Integer::sum));
    }

    public int loaded() {
        return loaded;
    }

    public int rejected() {
        return rejected;
    }

    public UploadResult toResult() {
        return new UploadResult(loaded, rejected, errors);
    }
}
