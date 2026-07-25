/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.engine.RangeScanner;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.store.PageFetcher;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The sequential producer with the resume spine: scans one {@link Node}'s
 * range and, for each page, <b>commits the cursor before emitting</b> (I1,
 * algorithms.md §4.2). The pagination cursor advances to the last <i>raw</i>
 * in-range key (pre-filter), so a {@code --resume} continues correctly past
 * filtered-out keys; an empty terminal page commits {@code COMPLETED} without
 * advancing the cursor.
 *
 * <p>{@code WorkStealingScan} replaces this single-node scan with the worklist +
 * thief, reusing the same per-page commit-before-emit.
 */
public final class CheckpointedScanProducer implements Pipeline.Producer<PageBatch> {

    private final PageFetcher fetcher;
    private final CheckpointStore store;
    private final Node node;
    private final byte[] prefix;
    private final int maxKeys;
    private final FilterChain filters;

    public CheckpointedScanProducer(PageFetcher fetcher, CheckpointStore store, Node node,
                                    byte[] prefix, int maxKeys, FilterChain filters) {
        this.fetcher = fetcher;
        this.store = store;
        this.node = node;
        this.prefix = prefix;
        this.maxKeys = maxKeys;
        this.filters = filters;
    }

    @Override
    public void produce(RunContext ctx, Channel<PageBatch> channel) throws Exception {
        RangeScanner scanner = new RangeScanner(fetcher, maxKeys, ctx.metrics(), null, null, null);
        AtomicLong pageSeq = new AtomicLong();
        try {
            scanner.runRange(prefix, ListingMode.OBJECTS, node.cursor(), node.rangeEnd(), ctx.cancellation(),
                    (batch, lastKey, completed) -> {
                        // I1 commit-before-emit: durably advance the cursor (the raw last
                        // in-range key, pre-filter) BEFORE the page is pushed downstream.
                        store.commitPage(new PageCommit(node.id(),
                                batch.isEmpty() ? null : lastKey, completed));
                        if (lastKey != null) {
                            ctx.metrics().setCursor(lastKey);
                        }
                        if (batch.isEmpty()) {
                            return;
                        }
                        List<ListEntry> kept = filters.apply(batch);
                        if (kept.isEmpty()) {
                            return;
                        }
                        Timer.Sample queueWait = ctx.metrics().startQueueWaitTimer();
                        boolean sent = channel.send(new Item<>(new PageBatch(node.id(), pageSeq.getAndIncrement(), kept)));
                        ctx.metrics().recordQueueWait(queueWait);
                        if (!sent) {
                            throw new ScanProducer.ReceiverGone();
                        }
                        ctx.metrics().recordPage();
                    });
        } catch (ScanProducer.ReceiverGone ignored) {
            // Downstream closed (broken pipe / cancellation) — stop cleanly. The last
            // committed page may not have been emitted (at-most-once for the default sink).
        }
    }
}
