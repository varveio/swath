/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.micrometer.core.instrument.Timer;
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
 * The listing producer: a sequential {@link RangeScanner} over the
 * whole prefix that filters each page and emits {@link PageBatch}es. The
 * {@code WorkStealingScan} engine replaces this with varied node ids;
 * the consumers (text / Parquet) are unchanged.
 */
public final class ScanProducer implements Pipeline.Producer<PageBatch> {

    /** Stops the producer cleanly when the downstream receiver is gone. */
    static final class ReceiverGone extends RuntimeException {
        ReceiverGone() {
            super(null, null, false, false);
        }
    }

    private final PageFetcher fetcher;
    private final byte[] prefix;
    private final int maxKeys;
    private final FilterChain filters;

    public ScanProducer(PageFetcher fetcher, byte[] prefix, int maxKeys, FilterChain filters) {
        this.fetcher = fetcher;
        this.prefix = prefix;
        this.maxKeys = maxKeys;
        this.filters = filters;
    }

    @Override
    public void produce(RunContext ctx, Channel<PageBatch> channel) throws Exception {
        RangeScanner scanner = new RangeScanner(fetcher, maxKeys, ctx.metrics(), null, null, null);
        AtomicLong pageSeq = new AtomicLong();
        try {
            scanner.runRange(prefix, ListingMode.OBJECTS, null, (byte[]) null, ctx.cancellation(), (batch, lastKey, completed) -> {
                if (lastKey != null) {
                    ctx.metrics().setCursor(lastKey);
                }
                List<ListEntry> kept = filters.apply(batch);
                if (kept.isEmpty()) {
                    return;
                }
                Timer.Sample queueWait = ctx.metrics().startQueueWaitTimer();
                boolean sent = channel.send(new Item<>(new PageBatch(0L, pageSeq.getAndIncrement(), kept)));
                ctx.metrics().recordQueueWait(queueWait);
                if (!sent) {
                    throw new ReceiverGone();
                }
                ctx.metrics().recordPage();
            });
        } catch (ReceiverGone ignored) {
            // Downstream closed (broken pipe / cancellation) — stop cleanly.
        }
    }
}
