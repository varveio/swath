/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

/** Cooperative cancellation point shared by the merge and final-drain hot loops. */
final class MergeCancellation {

    private MergeCancellation() {
    }

    static void check() {
        if (Thread.currentThread().isInterrupted()) {
            throw new Cancelled();
        }
    }

    /** Internal control-flow exception; the finalization coordinator translates the initiating failure. */
    static final class Cancelled extends RuntimeException {
        Cancelled() {
            super("sort finalization cancelled");
        }
    }
}
