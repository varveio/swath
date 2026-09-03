/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.ListingStatistics;
import io.varve.swath.output.RowTally;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;
import java.time.Duration;

/**
 * Dispatches each {@link PageBatch} to a decoupled dataset writer pool. The caller closes the pool
 * after listing quiesces, which finalizes open parts and publishes the completed dataset.
 */
public final class DatasetOutputStage implements Pipeline.Consumer<PageBatch> {

    private final DatasetWriterPool pool;
    private final RowTally tally = new RowTally();

    public DatasetOutputStage(DatasetWriterPool pool) {
        this.pool = pool;
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

    @Override
    public void consume(RunContext ctx, Channel<PageBatch> in) throws SwathException, InterruptedException {
        while (true) {
            long receiveStartedNs = System.nanoTime();
            Msg<PageBatch> msg = in.receive();
            ctx.metrics().recordChannelReceive(System.nanoTime() - receiveStartedNs);
            switch (msg) {
                case Item<PageBatch> item -> {
                    // The per-page emit span (client service cost) -- one nanoTime pair per page
                    // around this stage's whole dispatch, the same seam OutputStage times.
                    long startedNs = System.nanoTime();
                    pool.submit(item.value());
                    count(ctx, item.value());
                    ctx.metrics().recordEmit(System.nanoTime() - startedNs);
                }
                case End<PageBatch> ignored -> {
                    return;
                }
                case Failure<PageBatch> failure -> {
                    switch (failure.cause()) {
                        case SwathException s -> throw s;
                        case InterruptedException ie -> throw ie;
                        case null -> throw new ListingException("listing failed upstream of dataset output");
                        default -> throw new ListingException("listing failed upstream of dataset output", failure.cause());
                    }
                }
            }
        }
    }

    /** O(1) per page: the fetch worker tallied the page when it built the batch. */
    private void count(RunContext ctx, PageBatch batch) {
        ctx.metrics().recordEntriesEmitted(batch.entryCount());
        tally.merge(batch.tally(), ctx.metrics());
    }
}
