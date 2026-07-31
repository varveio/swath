/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.sim.kernel.SimContext;
import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.kernel.SimKernel;
import io.varve.swath.sim.kernel.SimRngStream;
import io.varve.swath.sim.kernel.SimRunResult;
import io.varve.swath.sim.model.CallClass;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HexFormat;
import java.util.List;

/**
 * A policy-free workload: workers claim ranges from a shared list and list each one to exhaustion.
 * Its closed forms make kernel arithmetic independently checkable:
 *
 * <ul>
 *   <li>A range holding {@code n} keys costs exactly {@code floor(n / pageSize) + 1} calls: full
 *       pages until the remainder, and then one short page, which is the only way <em>this</em>
 *       lister learns it has reached the end. A range whose size is an exact multiple of the page
 *       size therefore costs one call more than dividing would suggest, and an empty range still
 *       costs one. This is not {@code ListObjectsV2}: the store seam has no {@code IsTruncated}; the
 *       policy executor models that protocol and costs {@code ceil(n / pageSize)} instead.</li>
 *   <li>The total call count {@code P = sum over ranges of (floor(n_r / pageSize) + 1)} does not
 *       depend on the worker count at all — no work is duplicated or skipped by adding workers, so
 *       concurrency changes only <em>when</em> calls happen. This makes call count a structural
 *       invariant a scaling test can assert exactly.</li>
 *   <li>With constant latency {@code L} and client costs zeroed: one worker gives
 *       {@code wall = P x L}; enough workers to hold every range at once gives
 *       {@code wall = L x max_r (floor(n_r / pageSize) + 1)}; and {@code L = 0} gives {@code wall = 0}
 *       with all {@code P} calls still made.</li>
 * </ul>
 *
 * <p>{@link #run} borrows its already-open store; the caller owns its lifecycle.
 */
public final class SequentialListingDriver {

    /** Calls issued. A truncated run can have more issued than answered calls. */
    public static final String STORE_CALLS_COUNTER = "store.calls";
    /** Keys returned across every call — counted on arrival, so only answered pages contribute. */
    public static final String KEYS_LISTED_COUNTER = "store.keys";
    /** Ranges taken off the shared work list. */
    public static final String RANGES_CLAIMED_COUNTER = "ranges.claimed";

    private static final HexFormat HEX = HexFormat.of();
    private static final String OPEN_BOUND = "*";

    private final SimScenario scenario;
    private final ListingStore store;
    private final Deque<KeyRange> worklist;

    private SequentialListingDriver(SimScenario scenario, ListingStore store) {
        this.scenario = scenario;
        this.store = store;
        this.worklist = new ArrayDeque<>(scenario.ranges());
    }

    /**
     * Runs {@code scenario} against an already-open {@code store}.
     *
     * @param store the caller's handle; used, never opened, never closed
     */
    public static SimRunResult run(SimScenario scenario, ListingStore store) {
        if (store == null) {
            throw new IllegalArgumentException("the run API takes an already-open store handle");
        }
        scenario.clientCost().requireReadyForNewRun();
        SequentialListingDriver driver = new SequentialListingDriver(scenario, store);
        SimEventLog log = scenario.recordEventLog() ? SimEventLog.recording() : SimEventLog.disabled();
        SimKernel kernel = new SimKernel(scenario.seed(), scenario.budgets(), log, scenario.maxEvents());
        for (int worker = 0; worker < scenario.workerCount(); worker++) {
            kernel.scheduleBootstrap(0, worker, "worker.start", driver::claimNext);
        }
        return kernel.run();
    }

    private void claimNext(SimContext ctx) {
        KeyRange range = worklist.poll();
        if (range == null) {
            ctx.record("worker.retire", "");
            return;
        }
        ctx.count(RANGES_CLAIMED_COUNTER, 1);
        ctx.record("range.claim", bound(range.fromInclusive()) + "|" + bound(range.toExclusive()));
        issueCall(ctx, range, range.fromInclusive(), true);
    }

    /** Schedules a response that materializes rows at arrival, for future mutable stores. */
    private void issueCall(SimContext ctx, KeyRange range, ByteKey from, boolean fromInclusive) {
        long serviceNanos = scenario.latency().drawNanos(CallClass.WORKER_PAGE, ctx.rng(SimRngStream.LATENCY));
        ctx.count(STORE_CALLS_COUNTER, 1);
        ctx.record("list.request", bound(from) + "|inclusive=" + fromInclusive
                + "|limit=" + scenario.pageSize());
        ctx.schedule(serviceNanos, "list.response", next -> onResponse(next, range, from, fromInclusive));
    }

    private void onResponse(SimContext ctx, KeyRange range, ByteKey from, boolean fromInclusive) {
        List<ListedObject> rows = store.rows(from, fromInclusive, range.toExclusive(), scenario.pageSize(),
                Projection.KEYS_ONLY);
        ctx.count(KEYS_LISTED_COUNTER, rows.size());
        ctx.record("list.rows", "rows=" + rows.size());
        boolean lastPage = rows.size() < scenario.pageSize();
        ByteKey resumeFrom = lastPage ? null : ByteKey.copyOf(rows.getLast().key());
        scenario.clientCost().chargePage(ctx, rows.size(), charged -> {
            if (lastPage) {
                claimNext(charged);
            } else {
                issueCall(charged, range, resumeFrom, false);
            }
        });
    }

    /** Renders an open trace bound as {@code *}; other bounds are lowercase hex. */
    private static String bound(ByteKey key) {
        return key == null ? OPEN_BOUND : HEX.formatHex(key.toByteArray());
    }
}
