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
 * What one policy run produced, and — just as important — what it was produced <em>with</em>.
 *
 * <p>A simulated wall time means nothing on its own. The same fixture yields a different number under
 * a different store backend, a different client-cost term, or different declared budgets, and all
 * three are choices somebody made rather than facts about swath. So the record carries them: which
 * store served the run, which client-cost term it was charged against and how far that term can be
 * trusted, and what the run declared its timeouts and windows to be. A result quoted without them is
 * not a result.
 *
 * @param run          the kernel's own result: virtual duration, events dispatched, stop reason, trace
 * @param scenario     the scenario as run, including the declared budgets
 * @param storeLabel   what served the fixture (a resolved backend, or a test store's own name)
 * @param nodesCreated ranges that existed at any point — seeds plus every split child
 * @param splitsRejected split proposals the durable guard turned down: the races actually lost
 * @param storeReads   range reads issued against the fixture. The simulator's own cost, never the
 *                     modelled system's — a delimiter probe is one modelled call whatever it takes to
 *                     answer
 * @param finalConcurrencyTarget the adaptive controller's target when the run ended
 * @param stuck        whether the run ended because a page fetch exhausted its transient retries, which
 *                     is a modelled failure of the run rather than a completed one
 * @param timeline     when the run did what: its phase boundaries, and the tail's own rates and occupancy
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
        PolicyRunTimeline timeline) {

    /**
     * Assembles a result, merging the kernel's counters with the controller's own so a reader has one map
     * rather than two places to look for a run's engagement counts.
     *
     * <p>A named factory rather than a second constructor: the two would have had the same arity and
     * differed only by which of several {@code long}/{@code int} positions carried what, which is a
     * call-site mistake the compiler would have been happy to accept.
     */
    static PolicyRunResult of(SimRunResult run, PolicyScenario scenario, String storeLabel,
                              SortedMap<String, Long> gaugeCounters, int finalConcurrencyTarget,
                              long nodesCreated, long splitsRejected, long storeReads, boolean stuck,
                              PolicyRunTimeline timeline) {
        return new PolicyRunResult(run, scenario, storeLabel, merge(run.counters(), gaugeCounters),
                nodesCreated, splitsRejected, storeReads, finalConcurrencyTarget, stuck, timeline);
    }

    public PolicyRunResult {
        if (run == null || scenario == null || timeline == null) {
            // Every accessor and describe() dereferences all three, so a missing one has to fail here,
            // where the caller that dropped it is still on the stack, rather than at the first read.
            throw new IllegalArgumentException("a run record needs its kernel result, its scenario and "
                    + "its timeline");
        }
        counters = Collections.unmodifiableSortedMap(new TreeMap<>(counters));
    }

    /** The named counter, or zero if nothing incremented it. */
    public long counter(String name) {
        return counters.getOrDefault(name, 0L);
    }

    /** The run's virtual duration — the headline number. */
    public long virtualNanos() {
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

    /** Pages committed. */
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

    /** Dispatched events that had no effect because something faster had already retired their subject. */
    public long staleEvents() {
        return counter(SimExecutor.STALE_EVENTS_COUNTER);
    }

    /**
     * A one-line-per-fact rendering of the run and the inputs it must be read against — the shape a run
     * record takes when it is written down.
     */
    public String describe() {
        ClientCostTerm term = scenario.clientCost().term();
        StringBuilder out = new StringBuilder();
        out.append(String.format(Locale.ROOT, "virtual_duration=%.3fs%n", virtualNanos() / 1e9));
        out.append(String.format(Locale.ROOT, "stop_reason=%s completed=%s stuck=%s fault_disposition=%s%n",
                stopReason(), completed(), stuck, scenario.faultDisposition()));
        out.append(String.format(Locale.ROOT, "events=%d stale_events=%d%n", run.eventsProcessed(),
                staleEvents()));
        out.append(String.format(Locale.ROOT, "workers=%d page_size=%d final_concurrency_target=%d%n",
                scenario.workerCount(), scenario.pageSize(), finalConcurrencyTarget));
        out.append(String.format(Locale.ROOT, "keys_emitted=%d pages=%d store_calls=%d%n", keysEmitted(),
                pages(), storeCalls()));
        out.append(String.format(Locale.ROOT, "ranges=%d owner_split_children=%d thief_children=%d "
                + "splits_rejected=%d%n", nodesCreated, ownerSplitChildren(), thiefChildren(), splitsRejected));
        out.append(String.format(Locale.ROOT, "seed_mode=%s seed_probes=%d seed_ranges=%d%n",
                scenario.seedMode(), counter(SimExecutor.SEED_PROBES_COUNTER), counter("seed.ranges")));
        out.append(timeline.describe());
        out.append(sensorLine());
        out.append(String.format(Locale.ROOT, "store=%s store_reads=%d%n", storeLabel, storeReads));
        out.append(String.format(Locale.ROOT, "client_cost=%s (%s)%n", term.provenance(), term.sourceLabel()));
        out.append(String.format(Locale.ROOT, "budgets: worker_attempt_timeout=%dms probe_attempt_timeout=%dms "
                + "clean_window=%dms idle_park=%dms..%dms%n",
                scenario.budgets().workerAttemptTimeoutNanos() / 1_000_000L,
                scenario.budgets().probeAttemptTimeoutNanos() / 1_000_000L,
                scenario.budgets().concurrencyCleanWindowNanos() / 1_000_000L,
                scenario.budgets().idleStealBaseParkNanos() / 1_000_000L,
                scenario.budgets().idleStealBackoffCapNanos() / 1_000_000L));
        return out.toString();
    }

    /**
     * The position-sensor readings as shares of what they were read over, because the raw totals are
     * only meaningful against their own denominators: how often a page commit moved keys without moving
     * the fraction, and how often a scored victim's estimate degenerated. Shares are printed as
     * {@code n/d} alongside the ratio so a run with a tiny denominator cannot masquerade as a result.
     */
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
