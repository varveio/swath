/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import java.util.concurrent.atomic.AtomicReference;

/** First-failure relay shared by finalization readers, router, and encoders. */
final class FinalizationFailure {
    private final AtomicReference<Throwable> first = new AtomicReference<>();

    void record(Throwable failure) {
        first.compareAndSet(null, failure);
    }

    /** Surface the first peer failure and carry cooperative interrupt cancellation for every lane. */
    void check() {
        Throwable failure = first.get();
        if (failure != null) {
            throw new Failed(failure);
        }
        MergeCancellation.check();
    }

    Throwable cause() {
        return first.get();
    }

    static final class Failed extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failed(Throwable cause) {
            super("sort finalization pipeline failed", cause);
        }
    }
}
