package com.zemnitskiy.dpps.exception;

/**
 * A cluster operation failed for a reason that is expected to be temporary - a node left, the topology kept
 * changing, or the operation timed out (HTTP 503). The message is safe to show to API clients.
 */
public class ClusterUnavailableException extends RuntimeException {

    public ClusterUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
