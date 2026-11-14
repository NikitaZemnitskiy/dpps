package com.zemnitskiy.dpps.exception;

/**
 * Some payments are gone: every copy of one or more partitions left the cluster at once (e.g. two nodes failed
 * together). Answers would be silently incomplete, so the cluster refuses them until the loss is acknowledged by
 * deleting all payments (HTTP 503, no automatic retry).
 */
public class PaymentDataLostException extends RuntimeException {

    public PaymentDataLostException(int lostPartitions, int totalPartitions) {
        super("Payments were lost: all copies of " + lostPartitions + " of " + totalPartitions + " partitions left the"
                + " cluster at the same time (for example, two nodes failed together). Results would be incomplete,"
                + " so they are refused. To recover, delete all payments (DELETE /api/payments) and upload the source"
                + " files again.");
    }
}
