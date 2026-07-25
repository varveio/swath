/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.ListEntry;
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
 * Output stage for Parquet: dispatches each {@link PageBatch} to the decoupled
 * {@link ParquetWriterPool} (sticky by node id). The pool is closed by the
 * caller after the pipeline completes (so the footer + manifest commit happen
 * once listing has quiesced).
 */
public final class ParquetOutputStage implements Pipeline.Consumer<PageBatch> {

    private final ParquetWriterPool pool;
    private final RowTally tally = new RowTally();

    public ParquetOutputStage(ParquetWriterPool pool) {
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
            Msg<PageBatch> msg = in.receive();
            switch (msg) {
                case Item<PageBatch> item -> {
                    pool.submit(item.value());
                    count(ctx, item.value());
                }
                case End<PageBatch> ignored -> {
                    return;
                }
                case Failure<PageBatch> failure -> {
                    switch (failure.cause()) {
                        case SwathException s -> throw s;
                        case InterruptedException ie -> throw ie;
                        case null -> throw new ListingException("listing failed upstream of parquet output");
                        default -> throw new ListingException("listing failed upstream of parquet output", failure.cause());
                    }
                }
            }
        }
    }

    private void count(RunContext ctx, PageBatch batch) {
        ctx.metrics().recordEntriesEmitted(batch.entryCount());
        for (ListEntry e : batch.entries()) {
            tally.add(e, ctx.metrics());
        }
    }
}
