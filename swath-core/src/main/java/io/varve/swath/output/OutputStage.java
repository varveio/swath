/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import io.varve.swath.error.ListingException;
import io.varve.swath.error.OutputException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;
import java.io.IOException;
import java.time.Duration;

/**
 * The single output stage: drains {@link PageBatch}es from the channel and writes each entry
 * through an {@link EntryFormatter}, accumulating run statistics.
 *
 * <p><b>Broken pipe → clean exit (INT-12).</b> If the downstream reader closes
 * the pipe ({@code | head}), the write throws an {@link IOException}; the stage
 * recognises it ({@link BrokenPipe}) and returns cleanly so the run exits 0 with
 * no stack trace. Any other write failure is an {@link OutputException} (exit 1).
 */
public final class OutputStage implements Pipeline.Consumer<PageBatch> {

    private final EntryFormatter formatter;
    private final RowTally tally = new RowTally();

    private boolean brokenPipe;

    public OutputStage(EntryFormatter formatter) {
        this.formatter = formatter;
    }

    public boolean wasBrokenPipe() {
        return brokenPipe;
    }

    /**
     * Marks the stage as having hit a broken pipe outside {@link #consume}, e.g. when the
     * downstream reader closes the pipe during the final {@code formatter.close()} flush
     * rather than during a {@code write()} inside the pipeline. Callers that catch a
     * broken-pipe {@link IOException} at close time must call this so the run is still
     * treated as truncated (checkpoint {@code FAILED}, sidecar {@code completed:false}).
     */
    public void markBrokenPipe() {
        this.brokenPipe = true;
    }

    public long objects() {
        return tally.objects();
    }

    public long totalRows() {
        return tally.totalRows();
    }

    @Override
    public void consume(RunContext ctx, Channel<PageBatch> in) throws SwathException, InterruptedException {
        try {
            formatter.writeHeader();
            while (true) {
                Msg<PageBatch> msg = in.receive();
                switch (msg) {
                    case Item<PageBatch> item -> writeBatch(ctx, item.value());
                    case End<PageBatch> ignored -> {
                        return;
                    }
                    case Failure<PageBatch> failure -> {
                        switch (failure.cause()) {
                            case SwathException s -> throw s;
                            case InterruptedException ie -> throw ie;
                            case null -> throw new ListingException("listing failed upstream of output");
                            default -> throw new ListingException(
                                    "listing failed upstream of output", failure.cause());
                        }
                    }
                }
            }
        } catch (IOException e) {
            if (BrokenPipe.is(e)) {
                brokenPipe = true;   // downstream closed — clean stop, exit 0
                return;
            }
            throw new OutputException("output write failed", e);
        }
    }

    private void writeBatch(RunContext ctx, PageBatch batch) throws IOException {
        long entriesWritten = 0L;
        try {
            for (ListEntry e : batch.entries()) {
                formatter.write(e);
                entriesWritten++;
                tally.add(e, ctx.metrics());
            }
        } finally {
            ctx.metrics().recordEntriesEmitted(entriesWritten);
        }
    }

    public ListingStatistics statistics(long apiCalls, Duration elapsed) {
        return tally.statistics(apiCalls, elapsed);
    }
}
