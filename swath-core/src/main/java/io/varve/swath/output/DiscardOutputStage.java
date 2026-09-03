/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.PageBatch;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;
import java.time.Duration;

/**
 * Profiling sink that drains raw listing pages without formatting, writer queues, compression, or
 * filesystem output.
 *
 * <p>The stage deliberately retains the normal emission accounting: it tallies every row, records
 * estimated object bytes, advances {@code swath.entries.emitted}/universal progress once per page,
 * and records the page's emit span. Therefore it measures the listing engine plus the standard
 * checkpoint, channel, tally, and metrics costs, while removing only the material output path.
 */
public final class DiscardOutputStage implements Pipeline.Consumer<PageBatch> {

    private final RowTally tally = new RowTally();

    @Override
    public void consume(RunContext ctx, Channel<PageBatch> in) throws SwathException, InterruptedException {
        while (true) {
            long receiveStartedNs = System.nanoTime();
            Msg<PageBatch> msg = in.receive();
            ctx.metrics().recordChannelReceive(System.nanoTime() - receiveStartedNs);
            switch (msg) {
                case Item<PageBatch> item -> discardBatch(ctx, item.value());
                case End<PageBatch> ignored -> {
                    return;
                }
                case Failure<PageBatch> failure -> {
                    switch (failure.cause()) {
                        case SwathException s -> throw s;
                        case InterruptedException ie -> throw ie;
                        case null -> throw new ListingException("listing failed upstream of discard output");
                        default -> throw new ListingException(
                                "listing failed upstream of discard output", failure.cause());
                    }
                }
            }
        }
    }

    private void discardBatch(RunContext ctx, PageBatch batch) {
        long startedNs = System.nanoTime();
        ctx.metrics().recordEntriesEmitted(batch.entryCount());
        tally.merge(batch.tally(), ctx.metrics());   // O(1): tallied on the fetch worker
        ctx.metrics().recordEmit(System.nanoTime() - startedNs);
    }

    public long objects() {
        return tally.objects();
    }

    public long totalRows() {
        return tally.totalRows();
    }

    public long estimatedBytes() {
        return tally.estimatedBytes();
    }

    public ListingStatistics statistics(long apiCalls, Duration elapsed) {
        return tally.statistics(apiCalls, elapsed);
    }
}
