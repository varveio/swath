/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.error.SwathException;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * The one drain loop the engine tests share: run {@code producer} (normally a {@link
 * io.varve.swath.engine.WorkStealingScan}) through a {@link Pipeline} of {@code capacity}
 * slots and hand every emitted {@link PageBatch} to a sink until {@link End}.
 *
 * <p>The termination contract every consumer of this class relies on: the drain returns
 * only on {@link End}, and a {@link Failure} is rethrown as an {@code IllegalStateException}
 * carrying the producer's cause — so a producer-side fault fails the test loudly instead of
 * passing as a short read. Tests whose drain needs DIFFERENT termination semantics (swallow
 * the {@code Failure} so a typed cause surfaces from the join instead, or rethrow it as a
 * test-local signal) keep their own loop; that divergence is what those tests are about.
 */
public final class PipelineDrain {

    private PipelineDrain() {
    }

    /** Drain every emitted batch into {@code sink}, on a caller-supplied run context. */
    public static void drain(int capacity, RunContext ctx, Pipeline.Producer<PageBatch> producer,
            Consumer<PageBatch> sink) throws SwathException, InterruptedException {
        new Pipeline<PageBatch>(capacity).run(ctx, producer, (c, in) -> {
            while (true) {
                Msg<PageBatch> msg = in.receive();
                if (msg instanceof End<PageBatch>) {
                    return;
                }
                if (msg instanceof Failure<PageBatch> f) {
                    throw new IllegalStateException("producer failed", f.cause());
                }
                if (msg instanceof Item<PageBatch> item) {
                    sink.accept(item.value());
                }
            }
        });
    }

    /** As {@link #drain(int, RunContext, Pipeline.Producer, Consumer)}, on a fresh {@link RunContext}. */
    public static void drain(int capacity, Pipeline.Producer<PageBatch> producer, Consumer<PageBatch> sink)
            throws SwathException, InterruptedException {
        drain(capacity, RunContext.create(), producer, sink);
    }

    /** Append the raw bytes of every emitted key to {@code into}, on a caller-supplied run context. */
    public static void collectKeys(int capacity, RunContext ctx, Pipeline.Producer<PageBatch> producer,
            List<byte[]> into) throws SwathException, InterruptedException {
        drain(capacity, ctx, producer, batch -> {
            for (ListEntry e : batch.entries()) {
                into.add(e.key().raw());
            }
        });
    }

    /** As {@link #collectKeys(int, RunContext, Pipeline.Producer, List)}, on a fresh {@link RunContext}. */
    public static void collectKeys(int capacity, Pipeline.Producer<PageBatch> producer, List<byte[]> into)
            throws SwathException, InterruptedException {
        collectKeys(capacity, RunContext.create(), producer, into);
    }

    /** As {@link #collectKeys(int, Pipeline.Producer, List)}, into a fresh list. */
    public static List<byte[]> collectKeys(int capacity, Pipeline.Producer<PageBatch> producer)
            throws SwathException, InterruptedException {
        List<byte[]> emitted = new ArrayList<>();
        collectKeys(capacity, producer, emitted);
        return emitted;
    }

    /** Drain and discard: the run's metrics/trace are what the test reads, not the keys. */
    public static void discard(int capacity, Pipeline.Producer<PageBatch> producer)
            throws SwathException, InterruptedException {
        drain(capacity, RunContext.create(), producer, batch -> { });
    }
}
