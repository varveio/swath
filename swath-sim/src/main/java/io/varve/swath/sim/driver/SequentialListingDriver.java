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
 * The simplest workload that exercises the whole stack: {@code T} workers pulling ranges from a
 * shared list and listing each one page by page to its end. No stealing, no splitting, no pacing, no
 * adaptive concurrency — deliberately.
 *
 * <p><b>What it is for.</b> Two things, both of which need the real policies to be absent. First, it
 * closes the loop end to end — kernel, latency model, client-cost model and a real ground-truth store
 * — so that an integration defect surfaces here rather than inside a policy under test. Second, and
 * more importantly, its behaviour is simple enough to have closed forms, which is what makes the
 * kernel's arithmetic checkable independently of any policy's judgement:
 *
 * <ul>
 *   <li>A range holding {@code n} keys costs exactly {@code floor(n / pageSize) + 1} calls: full
 *       pages until the remainder, and then one short page, which is the only way a lister learns it
 *       has reached the end. A range whose size is an exact multiple of the page size therefore
 *       costs one call more than dividing would suggest, and an empty range still costs one.</li>
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
 * <p><b>The store handle is the caller's.</b> {@link #run} takes an open {@link ListingStore} and
 * neither opens nor closes it. Opening one is expensive enough on a large fixture to dominate a whole
 * sweep if it were paid per run, so the handle is opened once and reused across every run of the
 * sweep; this API makes that the only possibility rather than a convention.
 */
public final class SequentialListingDriver {

    /**
     * Store calls <b>issued</b>, counted when the request goes out rather than when its response
     * arrives. On a run that quiesced the two are the same number; on a run cut short by a duration or
     * event ceiling this <b>overstates</b> the calls actually answered, by however many were in flight
     * at the ceiling. Deliberate, and the same bias (and the same reasoning) as the FIFO resource's
     * service accounting: an issued-but-unanswered call is work the modelled system committed to, and
     * counting at arrival instead would silently drop it. Read it as "calls issued", and read a
     * truncated run's numbers as upper bounds.
     */
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

    /** Takes the next range, or retires this worker when the shared list is empty. */
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

    /**
     * Issues one page request at the current instant; the response is scheduled for this worker one
     * service time later. The store is not consulted here — the response's own event materialises it,
     * so that everything the response depends on is read at the instant it arrives, not at the
     * instant it was asked for. For an immutable fixture the two are the same answer; the point is
     * that a later model whose store state can change gets the faithful one without a redesign.
     */
    private void issueCall(SimContext ctx, KeyRange range, ByteKey from, boolean fromInclusive) {
        long serviceNanos = scenario.latency().drawNanos(CallClass.WORKER_PAGE, ctx.rng(SimRngStream.LATENCY));
        // Counted at REQUEST time, not on arrival -- see the counter constant's own javadoc for why,
        // and for how to read it on a run that a ceiling cut short.
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

    /** A bound rendered for the trace: lowercase hex, or {@code *} for an open bound. */
    private static String bound(ByteKey key) {
        return key == null ? OPEN_BOUND : HEX.formatHex(key.toByteArray());
    }
}
