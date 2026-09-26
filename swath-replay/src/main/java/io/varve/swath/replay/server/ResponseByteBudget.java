/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.metrics.ChunkAllocationReason;
import java.util.concurrent.atomic.AtomicLong;

/** Nonblocking aggregate accounting for live response array capacities. */
final class ResponseByteBudget {
    private final long limit;
    private final ChunkObserver chunkObserver;
    private final AtomicLong charged = new AtomicLong();
    private final AtomicLong peak = new AtomicLong();

    ResponseByteBudget(long limit) {
        this(limit, (reason, bytes) -> { });
    }

    ResponseByteBudget(long limit, ChunkObserver chunkObserver) {
        if (limit <= 0) {
            throw new IllegalArgumentException("response buffer budget must be positive");
        }
        this.limit = limit;
        this.chunkObserver = chunkObserver;
    }

    boolean tryCharge(long bytes) {
        for (;;) {
            long before = charged.get();
            if (bytes < 0 || bytes > limit - before) {
                return false;
            }
            long after = before + bytes;
            if (charged.compareAndSet(before, after)) {
                peak.accumulateAndGet(after, Math::max);
                return true;
            }
        }
    }

    void release(long bytes) {
        long after = charged.addAndGet(-bytes);
        if (after < 0) {
            throw new IllegalStateException("response byte budget released twice");
        }
    }

    long charged() { return charged.get(); }
    long peak() { return peak.get(); }
    long limit() { return limit; }

    void chunkAllocated(ChunkAllocationReason reason, int bytes) {
        chunkObserver.allocated(reason, bytes);
    }

    @FunctionalInterface
    interface ChunkObserver {
        void allocated(ChunkAllocationReason reason, int bytes);
    }
}
