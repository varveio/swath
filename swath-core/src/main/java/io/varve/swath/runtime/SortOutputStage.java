/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.varve.swath.error.ListingException;
import io.varve.swath.error.OutputException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PackedPage;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.ListingStatistics;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.sort.stage.SpillLane;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Output stage for {@code --sort}: admits each {@link PageBatch} into the single ordered
 * {@link SpillLane} (keyed by node id, so the lane accumulates per-node max keys for the checkpoint).
 * Mirrors {@link io.varve.swath.output.dataset.DatasetOutputStage}, but the lane's async encoder is
 * drained by the caller ({@code close()}/{@code abort()}) after the listing quiesces, then the merge
 * step publishes the final sorted file.
 */
public final class SortOutputStage implements Pipeline.Consumer<PageBatch> {

    private final SpillLane lane;

    private long objects;
    private long commonPrefixes;
    private long deleteMarkers;
    private long estimatedBytes;
    /** Allocated only when JVM assertions engage the live-page ordering tripwire. */
    private Map<Long, AcceptedPage> acceptedPages;

    public SortOutputStage(SpillLane lane) {
        this.lane = lane;
    }

    public long totalRows() {
        return objects + commonPrefixes + deleteMarkers;
    }

    public ListingStatistics statistics(long apiCalls, Duration elapsed) {
        return new ListingStatistics(objects, commonPrefixes, deleteMarkers, estimatedBytes, apiCalls, elapsed);
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
                    // around this stage's whole admission, the same seam OutputStage times.
                    long startedNs = System.nanoTime();
                    admit(item.value());
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
                        case null -> throw new ListingException("listing failed upstream of sort output");
                        default -> throw new ListingException("listing failed upstream of sort output", failure.cause());
                    }
                }
            }
        }
    }

    private void admit(PageBatch batch) throws SwathException, InterruptedException {
        try {
            assert verifyPageOrder(batch);
            if (batch.completionOnly()) {
                return;
            }
            if (batch.isPacked()) {
                lane.admit(batch.nodeId(), batch.packed());   // Already packed on the fetch worker
            } else {
                lane.admit(batch.nodeId(), batch.entries());
            }
        } catch (SwathException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new OutputException("sort lane failed", e);
        }
    }

    /** Debug-only tripwire: observation and assertion, never a sorting or recovery mechanism. */
    boolean verifyPageOrder(PageBatch batch) {
        if (acceptedPages == null) {
            acceptedPages = new HashMap<>();
        }
        byte[] firstKey;
        byte[] lastKey;
        if (batch.isPacked()) {
            firstKey = batch.packed().firstKey();
            lastKey = batch.packed().lastKey();
        } else if (batch.entries().isEmpty()) {
            firstKey = null;
            lastKey = null;
        } else {
            firstKey = batch.entries().getFirst().key().rawUnsafe();
            lastKey = batch.entries().getLast().key().rawUnsafe();
        }
        AcceptedPage previous = acceptedPages.get(batch.nodeId());
        if (previous != null && batch.pageSeq() <= previous.pageSeq()) {
            throw new AssertionError("sort page sequence did not increase for node "
                    + batch.nodeId() + ": " + batch.pageSeq() + " <= " + previous.pageSeq());
        }
        if (previous != null && firstKey != null && previous.lastKey() != null
                && KeyBytes.compareUnsigned(firstKey, previous.lastKey()) <= 0) {
            throw new AssertionError("sort page key did not advance for node " + batch.nodeId());
        }
        if (batch.nodeCompleted()) {
            acceptedPages.remove(batch.nodeId());
        } else {
            acceptedPages.put(batch.nodeId(), new AcceptedPage(batch.pageSeq(), lastKey));
        }
        return true;
    }

    int trackedPageCountForTesting() {
        return acceptedPages == null ? 0 : acceptedPages.size();
    }

    private record AcceptedPage(long pageSeq, byte[] lastKey) {
    }

    private void count(RunContext ctx, PageBatch batch) {
        ctx.metrics().recordEntriesEmitted(batch.entryCount());
        if (batch.isPacked()) {
            // The fetch worker tallied the per-row-type counts + object-size sum at pack time, so
            // the drain thread reads them off the packed page instead of iterating (and decoding) entries —
            // the same totals a per-entry loop would produce (recordEstimatedBytes is an additive counter).
            PackedPage p = batch.packed();
            objects += p.objectCount();
            commonPrefixes += p.commonPrefixCount();
            deleteMarkers += p.deleteMarkerCount();
            long size = p.totalObjectSize();
            estimatedBytes += size;
            ctx.metrics().recordEstimatedBytes(size);
            return;
        }
        for (ListEntry e : batch.entries()) {
            switch (e) {
                case ObjectEntry o -> {
                    objects++;
                    estimatedBytes += o.size();
                    ctx.metrics().recordEstimatedBytes(o.size());
                }
                case CommonPrefixEntry ignored -> commonPrefixes++;
                case DeleteMarkerEntry ignored -> deleteMarkers++;
            }
        }
    }
}
