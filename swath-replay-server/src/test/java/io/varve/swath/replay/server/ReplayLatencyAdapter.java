/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.LatencyModel;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Test-side bridge: drives {@link ReplayHandler}'s per-request latency hook (a plain {@code
 * BiFunction} over the protocol types, so this module's MAIN code never depends on swath-core's
 * testFixtures) with one of swath-core's canned {@code io.varve.swath.testkit.LatencyModels}
 * profiles — "reusing the same LatencyModel/profiles" across both the in-process
 * {@code MockPageFetcher} harness and this real-HTTP replay server.
 *
 * <p>The {@code warm}/{@code cold} classification here is a cheaper APPROXIMATION of {@code
 * MockPageFetcher}'s boundary-tracking one: {@code LatencyModel.Context} carries no last-served-row
 * boundary, so even though the hook now sees the result, this adapter keeps the request-side
 * heuristic — {@code startAfter() != null && maxKeys() > 1} is warm — true for every ordinary
 * continuation page of a real S3 pager, false for the first page of a scan and for
 * {@code maxKeys<=1} structural probes. That is good enough to drive {@code denseColdProbes}
 * against a real HTTP round trip, but it is NOT a drop-in replacement for {@code MockPageFetcher}'s
 * exact-boundary classification. (Result-proportional injection is {@link ShapeLatency}'s job, not
 * this adapter's.)
 */
final class ReplayLatencyAdapter implements BiFunction<S3ListRequest, S3ListResult, Duration> {

    private final LatencyModel model;
    private final AtomicInteger callIndex = new AtomicInteger();

    ReplayLatencyAdapter(LatencyModel model) {
        this.model = model;
    }

    @Override
    public Duration apply(S3ListRequest req, S3ListResult result) {
        int idx = callIndex.getAndIncrement();
        boolean probe = req.maxKeys() <= 1;
        boolean warm = !probe && req.startAfter() != null;
        boolean delimiterVisible = req.delimiter() != null && req.delimiter().length > 0;
        PageRequest pageRequest = PageRequest.objectsDelimited(
                req.prefix(), req.delimiter(), req.startAfter(), req.maxKeys());
        return model.delayFor(new LatencyModel.Context(pageRequest, idx, warm, probe, delimiterVisible));
    }
}
