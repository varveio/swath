/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.kernel.SimRunResult;
import io.varve.swath.sim.kernel.SimStopReason;
import io.varve.swath.sim.model.ClientCostTerm;
import java.util.Collections;
import java.util.Locale;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Result of one policy run together with the inputs needed to interpret and reproduce it.
 *
 * @param run          the kernel's own result: virtual duration, events dispatched, stop reason, trace
 * @param scenario     the scenario as run, including the declared budgets
 * @param storeLabel   what served the fixture (a resolved backend, or a test store's own name)
 * @param nodesCreated ranges that existed at any point — seeds plus every split child
 * @param splitsRejected split proposals refused by the ledger's final bound, cursor, or completion guard
 * @param storeReads   range reads issued against the fixture. The simulator's own cost, never the
 *                     modelled system's — a delimiter probe is one modelled call whatever it takes to
 *                     answer
 * @param finalConcurrencyTarget the adaptive controller's target when the run ended
 * @param stuck        whether the BOUNDED disposition stopped at the worker retry threshold
 * @param timeline     when the run did what: its phase boundaries, and the tail's own rates and occupancy
 * @param sensing      position sensor used by victim selection and owner-split gates
 */
public record PolicyRunResult(
        SimRunResult run,
        PolicyScenario scenario,
        String storeLabel,
        SortedMap<String, Long> counters,
        long nodesCreated,
        long splitsRejected,
        long storeReads,
        int finalConcurrencyTarget,
        boolean stuck,
        PolicyRunTimeline timeline,
        SensingVariant sensing) {

    /** Merges kernel and controller counters into one result. */
    static PolicyRunResult of(SimRunResult run, PolicyScenario scenario, String storeLabel,
                              SortedMap<String, Long> gaugeCounters, int finalConcurrencyTarget,
                              long nodesCreated, long splitsRejected, long storeReads, boolean stuck,
                              PolicyRunTimeline timeline, SensingVariant sensing) {
        return new PolicyRunResult(run, scenario, storeLabel, merge(run.counters(), gaugeCounters),
                nodesCreated, splitsRejected, storeReads, finalConcurrencyTarget, stuck, timeline,
                sensing);
    }

    public PolicyRunResult {
        if (run == null || scenario == null || timeline == null || sensing == null) {
            throw new IllegalArgumentException("a run record needs its kernel result, its scenario, its "
                    + "timeline and the sensor it steered on");
        }
        counters = Collections.unmodifiableSortedMap(new TreeMap<>(counters));
    }

    /** The named counter, or zero if nothing incremented it. */
    public long counter(String name) {
        return counters.getOrDefault(name, 0L);
    }

    /** Time to quiescence, excluding later dispatch of uncancellable park timers. */
    public long virtualNanos() {
        return timeline.endNanos();
    }

    /** Kernel end time, including post-quiescence dispatch of stale park timers. */
    public long kernelNanos() {
        return run.wallNanos();
    }

    /** Why the run ended; only {@link SimStopReason#QUIESCED} with {@code !stuck} is a finished run. */
    public SimStopReason stopReason() {
        return run.stopReason();
    }

    /** Whether this run finished the fixture rather than hitting a ceiling or a modelled failure. */
    public boolean completed() {
        return !stuck && run.stopReason() == SimStopReason.QUIESCED;
    }

    /** The trace, empty unless the scenario asked for one. */
    public SimEventLog log() {
        return run.log();
    }

    /** Keys the run emitted — the number a completeness check compares against the fixture's size. */
    public long keysEmitted() {
        return counter(SimExecutor.KEYS_EMITTED_COUNTER);
    }

    /** Committed pages, including empty pages. */
    public long pages() {
        return counter(SimExecutor.PAGES_COUNTER);
    }

    /** Modelled store calls issued, of every class. */
    public long storeCalls() {
        return counter(SimExecutor.STORE_CALLS_COUNTER);
    }

    /** Children published by owner-side splits and by thief steals, respectively. */
    public long ownerSplitChildren() {
        return counter(SimExecutor.OWNER_SPLIT_COUNTER);
    }

    /** @see #ownerSplitChildren() */
    public long thiefChildren() {
        return counter(SimExecutor.THIEF_SPLIT_COUNTER);
    }

    /**
     * Thief proposals rejected before the ledger because the cursor passed the pivot or the bound
     * moved. {@link #splitsRejected()} separately counts final ledger-guard refusals, including a
     * completed parent.
     */
    public long splitsLostAtRevalidation() {
        return counter("RETRY.cursor_passed_pivot") + counter("RETRY.bound_moved");
    }

    /** Dispatched events that had no effect because something faster had already retired their subject. */
    public long staleEvents() {
        return counter(SimExecutor.STALE_EVENTS_COUNTER);
    }

    /** Renders the run and its interpretation/reproduction inputs one fact per line. */
    public String describe() {
        ClientCostTerm term = scenario.clientCost().term();
        StringBuilder out = new StringBuilder();
        out.append(String.format(Locale.ROOT, "virtual_duration=%.3fs%n", virtualNanos() / 1e9));
        out.append(String.format(Locale.ROOT, "stop_reason=%s completed=%s stuck=%s fault_disposition=%s%n",
                stopReason(), completed(), stuck, scenario.faultDisposition()));
        out.append(String.format(Locale.ROOT, "events=%d stale_events=%d%n", run.eventsProcessed(),
                staleEvents()));
        out.append(String.format(Locale.ROOT,
                "workers=%d page_size=%d final_concurrency_target=%d sensing=%s%n",
                scenario.workerCount(), scenario.pageSize(), finalConcurrencyTarget, sensing));
        out.append(String.format(Locale.ROOT, "seed=%d store_server_capacity=%d max_events=%d%n",
                scenario.seed(), scenario.storeServerCapacity(), scenario.maxEvents()));
        out.append(String.format(Locale.ROOT, "keys_emitted=%d pages=%d store_calls=%d%n", keysEmitted(),
                pages(), storeCalls()));
        out.append(String.format(Locale.ROOT, "ranges=%d owner_split_children=%d thief_children=%d "
                        + "splits_lost_revalidation=%d splits_rejected=%d%n",
                nodesCreated, ownerSplitChildren(), thiefChildren(), splitsLostAtRevalidation(),
                splitsRejected));
        out.append(String.format(Locale.ROOT, "seed_mode=%s seed_probes=%d seed_ranges=%d%n",
                scenario.seedMode(), counter(SimExecutor.SEED_PROBES_COUNTER), counter("seed.ranges")));
        out.append(timeline.describe());
        out.append(sensorLine());
        out.append(String.format(Locale.ROOT, "store=%s store_reads=%d%n", storeLabel, storeReads));
        out.append(String.format(Locale.ROOT, "client_cost=%s (%s)%n", term.provenance(), term.sourceLabel()));
        // The uncancellable attempt-slot park explains the quiescence-to-kernel-end gap.
        out.append(String.format(Locale.ROOT, "budgets: worker_attempt_timeout=%dms probe_attempt_timeout=%dms "
                + "clean_window=%dms idle_park=%dms..%dms attempt_slot_park=%dms%n",
                scenario.budgets().workerAttemptTimeoutNanos() / 1_000_000L,
                scenario.budgets().probeAttemptTimeoutNanos() / 1_000_000L,
                scenario.budgets().concurrencyCleanWindowNanos() / 1_000_000L,
                scenario.budgets().idleStealBaseParkNanos() / 1_000_000L,
                scenario.budgets().idleStealBackoffCapNanos() / 1_000_000L,
                scenario.budgets().idleStealAttemptParkNanos() / 1_000_000L));
        return out.toString();
    }

    /** Formats sensor ratios with their raw numerators and denominators. */
    private String sensorLine() {
        long commits = counter(SimExecutor.SENSOR_BOUNDED_COMMITS_COUNTER);
        long bounded = counter(SimExecutor.SENSOR_VICTIMS_BOUNDED_COUNTER);
        return String.format(Locale.ROOT,
                "sensor: cursor_advance_invisible=%d/%d (%.3f) victims_scanned=%d "
                        + "est_ignores_keys=%d/%d (%.3f) est_zero=%d/%d (%.3f)%n",
                counter(SimExecutor.SENSOR_INVISIBLE_ADVANCE_COUNTER), commits,
                share(counter(SimExecutor.SENSOR_INVISIBLE_ADVANCE_COUNTER), commits),
                counter(SimExecutor.SENSOR_VICTIMS_SCANNED_COUNTER),
                counter(SimExecutor.SENSOR_EST_IGNORES_KEYS_COUNTER), bounded,
                share(counter(SimExecutor.SENSOR_EST_IGNORES_KEYS_COUNTER), bounded),
                counter(SimExecutor.SENSOR_EST_ZERO_COUNTER), bounded,
                share(counter(SimExecutor.SENSOR_EST_ZERO_COUNTER), bounded));
    }

    private static double share(long numerator, long denominator) {
        return denominator == 0L ? 0.0 : (double) numerator / denominator;
    }

    private static SortedMap<String, Long> merge(SortedMap<String, Long> kernel,
                                                 SortedMap<String, Long> gauge) {
        TreeMap<String, Long> merged = new TreeMap<>(kernel);
        for (var entry : gauge.entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
        return merged;
    }
}
