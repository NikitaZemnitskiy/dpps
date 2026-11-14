package com.zemnitskiy.dpps.compute;

import org.apache.ignite.IgniteException;

import java.io.Serial;

/**
 * The cluster topology changed while a computation was running (a node left or a partition moved), so its
 * result would be incomplete. The computation can simply be retried.
 */
public class TopologyChangedException extends IgniteException {

    @Serial
    private static final long serialVersionUID = 1L;

    public TopologyChangedException(String message, Throwable cause) {
        super(message, cause);
    }
}
