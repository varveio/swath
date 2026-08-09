/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.varve.swath.engine.EngineToggles;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic + terminal writer for the JSON run-summary artifact. Mirrors
 * {@link RunProgressReporter}'s single-thread scheduler pattern: every {@code
 * flushInterval} a mid-run snapshot is written with {@code completed:false}; {@link
 * #complete(RunSummary)} writes the terminal {@code completed:true} record; {@link
 * #close()} makes a best-effort attempt to leave a partial file behind even when {@code complete}
 * is never reached — a sidecar failure must never crash the run, so an exception/SIGINT still
 * gets a best-effort write on the way out (a run whose every terminal write fails, with no prior
 * periodic flush, ends with no file — logged {@code summary_json_final_write_failed}; see the ladder below).
 *
 * <p>Every write is atomic: serialize to {@code <path>.tmp} in the same directory,
 * then rename over {@code <path>} ({@code ATOMIC_MOVE}, falling back to a plain
 * replace where the filesystem does not support atomic rename) so a reader never
 * observes a torn file. All failures are best-effort — logged and swallowed, never
 * propagated into the run.
 *
 * <p><b>The terminal write matters more than any periodic one, so it gets more
 * resilience.</b> {@link #close()}'s terminal write ({@code writeFinalPartialWithResilience})
 * retries the full write, then degrades to a meters-dropped minimal write, then — if even that
 * fails — logs a grep-able {@code ERROR} marker instead of a silent {@code WARN}, so "we have no
 * reliable terminal summary for this run" is visible in the same log stream an operator already
 * scrapes, never merely inferable after the fact from a suspiciously-round {@code duration_ms};
 * see that method's javadoc for the full ladder. Every write (periodic and terminal) also stamps
 * {@code as_of} — see {@link #write} — so a reader can always tell how fresh the file is,
 * independent of which path produced it.
 *
 * <p><b>Invariant:</b> once {@code completedCalled} is {@code true}, no
 * {@code completed:false} write ever lands on disk again. {@link #flushSafely()}
 * (the scheduler thread) and {@link #close()}'s partial fallback are the two paths
 * that can write {@code completed:false}; both re-check {@code completedCalled}
 * <i>inside</i> {@link #writeLock}, and {@link #complete(RunSummary)} sets the flag
 * and writes {@code completed:true} inside that same lock. That closes the TOCTOU
 * race a check-before-lock would leave open: whichever of a scheduled flush or
 * {@code complete()} acquires the lock second always observes the other's effect
 * before deciding whether to write.
 */
public final class JsonRunSummaryWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JsonRunSummaryWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TMP_SUFFIX = ".tmp";
    /** Bounded retries for the terminal (close()-time) write only — see {@link #close()}. */
    private static final int FINAL_WRITE_ATTEMPTS = 3;
    private static final long FINAL_WRITE_BACKOFF_MILLIS = 20L;

    private final Config config;
    private final MeterRegistry registry;
    private final Instant startedAt;
    private final Supplier<RunSummary> snapshotSupplier;
    private final Supplier<TerminalStatus> terminalStatusSupplier;
    private final ScheduledExecutorService executor;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean completedCalled = new AtomicBoolean(false);
    /** The pair {@link #pinTerminal} decided, or {@code null} while the suppliers still own it. */
    private final AtomicReference<Terminal> pinnedTerminal = new AtomicReference<>();
    private final Path parent;
    private final WriteSink sink;

    private JsonRunSummaryWriter(Config config, MeterRegistry registry, Instant startedAt,
                                  Supplier<RunSummary> snapshotSupplier,
                                  Supplier<TerminalStatus> terminalStatusSupplier,
                                  ScheduledExecutorService executor, WriteSink sink) {
        this.config = config;
        this.registry = registry;
        this.startedAt = startedAt;
        this.snapshotSupplier = snapshotSupplier;
        this.terminalStatusSupplier = terminalStatusSupplier;
        this.executor = executor;
        this.parent = config.path().toAbsolutePath().getParent();
        // Production default is the real atomic-file writer; tests inject a failing/flaky
        // sink to exercise the close()-time retry+fallback ladder deterministically, without
        // relying on OS permission bits.
        this.sink = sink != null ? sink : this::atomicWrite;
        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                log.warn("summary_json_write_failed path={} message={}", config.path(), e.getMessage());
            }
        }
    }

    /** The low-level "flush these bytes to disk" step {@link #write} delegates to — a seam so a
     * test can inject a failing/flaky sink instead of manipulating OS permission bits. */
    @FunctionalInterface
    interface WriteSink {
        void accept(ObjectNode root) throws IOException;
    }

    /**
     * The terminal disposition consulted only when {@link #close()} writes a partial (i.e. {@link
     * #complete} was never reached): the attributed {@link StopReason}, plus — for a {@code STUCK}
     * reason only — the {@code stop_source}/{@code error_class} pair a fleet parser
     * needs to tell a liveness-watchdog wedge from a transient-retry-cap wedge without re-deriving
     * it from logs. {@code stopSource}/{@code errorClass} are {@code null} for every non-{@code
     * STUCK} reason and for an unattributed/broken-pipe {@code null} reason.
     *
     * <p>Plain {@code String}s, not {@code CancelSource}/an enum, deliberately: this is the
     * {@code observability} package, which must not depend on {@code runtime}'s cancellation types
     * (see {@code RunMetrics}'s layering notes) — the caller (in {@code runtime}, which already
     * depends on both) does that translation, e.g. {@code source == null ? null : source.tag()}.
     */
    public record TerminalStatus(StopReason reason, String stopSource, String errorClass, Integer exitCode) {

        /** Convenience for terminals with no source/error_class/exit_code to attribute. */
        public TerminalStatus(StopReason reason) {
            this(reason, null, null, null);
        }

        /**
         * The pre-{@code exitCode} shape: reason + optional {@code stop_source}/{@code error_class},
         * with the process {@code exit_code} left unknown ({@code null}) — most partial-write paths
         * leave it that way. A classified seed failure may thread
         * both a resolved exit code AND an {@code error_class} via the 4-arg canonical constructor,
         * so a permissions/config seed failure reports its true class and exit 1.
         */
        public TerminalStatus(StopReason reason, String stopSource, String errorClass) {
            this(reason, stopSource, errorClass, null);
        }
    }

    /**
     * Starts the periodic flush. {@code config} must be non-null; callers no-op when the sidecar
     * is disabled. A terminal partial (the run unwound without {@link #complete}) is attributed
     * {@link StopReason#CRASH}; use {@link #start(Config, MeterRegistry, Instant, Supplier, Supplier)}
     * to attribute it differently (timebox/signal/broken-pipe/stuck).
     */
    public static JsonRunSummaryWriter start(Config config, MeterRegistry registry, Instant startedAt,
                                              Supplier<RunSummary> snapshotSupplier) {
        return start(config, registry, startedAt, snapshotSupplier, () -> new TerminalStatus(StopReason.CRASH));
    }

    /**
     * Starts the periodic flush with an explicit {@code terminalStatusSupplier} — consulted only
     * when {@link #close()} writes a partial (i.e. {@link #complete} was never reached), so it can
     * distinguish a timebox / signal / broken-pipe / stuck stop from a crash, and (for stuck) carry
     * the {@code stop_source}/{@code error_class} pair. A {@code null} {@link TerminalStatus#reason()}
     * leaves {@code stop_reason} unset (e.g. broken pipe, which is neither a crash nor a signal).
     */
    public static JsonRunSummaryWriter start(Config config, MeterRegistry registry, Instant startedAt,
                                              Supplier<RunSummary> snapshotSupplier,
                                              Supplier<TerminalStatus> terminalStatusSupplier) {
        return start(config, registry, startedAt, snapshotSupplier, terminalStatusSupplier, null);
    }

    /**
     * Test seam: as the 4-arg {@link #start}, but with an explicit {@link WriteSink}
     * overriding the default {@code this::atomicWrite}. {@code null} (every production caller)
     * keeps the real atomic-file writer. Package-private — production code never overrides this.
     */
    static JsonRunSummaryWriter start(Config config, MeterRegistry registry, Instant startedAt,
                                       Supplier<RunSummary> snapshotSupplier,
                                       Supplier<TerminalStatus> terminalStatusSupplier, WriteSink sink) {
        ScheduledExecutorService executor = DaemonSchedulers.create("swath-summary-json");
        JsonRunSummaryWriter writer = new JsonRunSummaryWriter(
                config, registry, startedAt, snapshotSupplier, terminalStatusSupplier, executor, sink);
        DaemonSchedulers.schedule(executor, config.flushInterval(), writer::flushSafely);
        return writer;
    }

    /**
     * Terminal write: {@code completed:true}, {@code exit_code:0}. Setting the flag and
     * writing the terminal record happen inside the same {@link #writeLock} critical
     * section as every {@code completed:false} write's re-check, so however the
     * scheduler thread's periodic flush and this call interleave, the on-disk end state
     * is always {@code completed:true} — see the class javadoc's TOCTOU note.
     */
    public void complete(RunSummary finalSummary) {
        writeLock.lock();
        try {
            completedCalled.set(true);
            write(finalSummary, true, 0, StopReason.COMPLETED);
        } catch (IOException | RuntimeException e) {
            log.warn("summary_json_write_failed path={} message={}", config.path(), e.getMessage());
        } finally {
            writeLock.unlock();
        }
    }

    /** One terminal decision, shared by the run's summary sink and this writer's final record. */
    private record Terminal(RunSummary summary, TerminalStatus status) {
    }

    /**
     * Pin the exact {@code (summary, status)} pair {@link #close()} will write, instead of letting
     * it re-read {@link #snapshotSupplier}/{@link #terminalStatusSupplier} a moment later. The
     * caller renders that same pair to the run's {@link RunSummarySink}, so the human block and
     * this partial record cannot report a different {@code duration_ms} — nor, on a run that gets
     * cancelled between the two reads, a different disposition. Irrelevant once {@link
     * #complete(RunSummary)} has run: its terminal record is already the one decision.
     */
    public void pinTerminal(RunSummary summary, TerminalStatus status) {
        pinnedTerminal.set(new Terminal(summary, status));
    }

    @Override
    public void close() {
        DaemonSchedulers.stop(executor, log, "summary_json_writer_stop_timeout");
        // A partial run must still leave a file behind. If complete() was
        // never reached (an exception/SIGINT unwound the try-with-resources instead),
        // the last flush becomes final as a completed:false snapshot. The recheck of
        // completedCalled happens INSIDE writeLock (not before acquiring it) so a
        // complete() that races this close() on another thread can never be clobbered
        // by a stale completed:false write landing after it.
        writeLock.lock();
        try {
            if (!completedCalled.get()) {
                writeFinalPartialWithResilience();
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * The ONE write that matters most on an aborted run gets bounded retry + a degraded
     * fallback instead of the single best-effort attempt every other write site here uses.
     * Always called with {@link #writeLock} already
     * held (from {@link #close()}), so no further locking here; never throws — a
     * sidecar failure must never crash the run, so that guarantee stays inviolate all the way through the ladder,
     * INCLUDING evaluating {@link #snapshotSupplier}/{@link #terminalStatusSupplier} themselves
     * (deliberately read HERE, inside this method's own try, rather than by {@link #close()}
     * passing already-evaluated arguments — a supplier built from partially-initialized state
     * mid-abort is exactly the kind of edge case that bites, and evaluating it in the caller's
     * frame would let it escape uncaught before this method's ladder even starts). A run whose
     * terminal path already decided the pair ({@link #pinTerminal}) skips the suppliers entirely
     * and writes that decision, which is also what the operator-facing block reported.
     *
     * <ol>
     *   <li>build the snapshot: if either supplier itself throws, there is no summary to write at
     *       all — log the grep-able {@code summary_json_final_snapshot_failed} marker and return;</li>
     *   <li>retry the FULL write (including {@code meters[]}) up to {@link #FINAL_WRITE_ATTEMPTS}
     *       times, with a tiny backoff between attempts;</li>
     *   <li>if every full attempt fails, degrade: one attempt at a MINIMAL write that drops the
     *       {@code meters[]} block (the bulk of the payload, and the likeliest large-serialization
     *       cost on a giant run) but keeps every terminal fact a reader needs — {@code completed},
     *       {@code stop_reason}, {@code as_of}, {@code objects}/{@code duration_ms}, the recovered-
     *       error rollup, etc.;</li>
     *   <li>if even that fails, the run still exits cleanly — a sidecar failure never crashes the
     *       run — but NOT silently: an
     *       {@code ERROR}-level {@code summary_json_final_write_failed} marker lands on the same
     *       stderr stream (logback routes {@code io.varve.swath} WARN/ERROR to {@code STDERR}) a
     *       fleet operator already scrapes — grep-able evidence that this run's on-disk summary is
     *       untrustworthy, instead of silent, indistinguishable staleness.</li>
     * </ol>
     */
    private void writeFinalPartialWithResilience() {
        RunSummary summary;
        TerminalStatus status;
        Terminal pinned = pinnedTerminal.get();
        try {
            summary = pinned != null ? pinned.summary() : snapshotSupplier.get();
            status = pinned != null ? pinned.status() : terminalStatusSupplier.get();
        } catch (RuntimeException e) {
            // No summary was ever built, so there is nothing for the retry/degrade ladder below to
            // even attempt — a DISTINCT marker from summary_json_final_write_failed (that one means
            // "built a snapshot, couldn't persist it"; this one means "couldn't even build one") so
            // an operator can tell the two failure modes apart; both answer the same question ("do I
            // have a reliable terminal summary for this run?") and both should be grepped together
            // for that question (`summary_json_final_.*_failed`).
            log.error("summary_json_final_snapshot_failed path={} message={} (snapshotSupplier/"
                    + "terminalStatusSupplier threw before any write attempt — no reliable terminal "
                    + "summary for this run)", config.path(), e.getMessage());
            return;
        }
        if (status == null) {
            // A supplier returning a null TerminalStatus (as opposed to a TerminalStatus with a null
            // reason — e.g. broken pipe, a legitimate value) is defensively treated the same as an
            // unattributed reason, so the never-crash guarantee stays inviolate even for a caller that returns the
            // record itself as null instead of TerminalStatus(null).
            status = new TerminalStatus(null);
        }
        Exception lastFailure = null;
        for (int attempt = 1; attempt <= FINAL_WRITE_ATTEMPTS; attempt++) {
            try {
                write(summary, false, status.exitCode(), status.reason(), status.stopSource(), status.errorClass());
                if (attempt > 1) {
                    log.debug("summary_json_final_write_recovered path={} attempt={} of={}",
                            config.path(), attempt, FINAL_WRITE_ATTEMPTS);
                }
                return;
            } catch (IOException | RuntimeException e) {
                lastFailure = e;
                log.warn("summary_json_final_write_attempt_failed path={} attempt={} of={} message={}",
                        config.path(), attempt, FINAL_WRITE_ATTEMPTS, e.getMessage());
                if (attempt < FINAL_WRITE_ATTEMPTS && !sleepBackoff()) {
                    break;   // interrupted mid-retry: stop retrying, still try the degraded write below
                }
            }
        }
        try {
            // The degraded fallback still carries stop_source/error_class — they are terminal
            // FACTS about why the run stopped, not meters, so dropping meters[] must not drop them.
            // exit_code is likewise a terminal fact (the seed-failure path threads a real code).
            write(summary, false, status.exitCode(), status.reason(), status.stopSource(), status.errorClass(), false);
            log.error("summary_json_final_write_degraded path={} attempts={} message={} "
                    + "(full write failed every attempt; wrote a meters-dropped fallback instead — "
                    + "completed/stop_reason/stop_source/error_class/as_of/counts are still trustworthy, "
                    + "meters[] is absent)",
                    config.path(), FINAL_WRITE_ATTEMPTS, lastFailure == null ? null : lastFailure.getMessage());
        } catch (IOException | RuntimeException e) {
            // The grep-able "we have no reliable terminal summary for this run" marker:
            // an operator/fleet classifier counts THIS line, not a suspiciously-round duration_ms,
            // to find every run whose on-disk summary is stale/untrustworthy.
            log.error("summary_json_final_write_failed path={} attempts={} message={}",
                    config.path(), FINAL_WRITE_ATTEMPTS, e.getMessage());
        }
    }

    /** Tiny fixed backoff between final-write retries; {@code false} (interrupt flag restored)
     * tells the caller to stop retrying and go straight to the degraded attempt. */
    private boolean sleepBackoff() {
        try {
            Thread.sleep(FINAL_WRITE_BACKOFF_MILLIS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Scheduled periodic flush. The {@code completedCalled} check is re-taken here
     * <i>after</i> {@link #writeLock} is acquired (not before, as a cheap early-exit
     * would do) — that closes the TOCTOU race: without the lock, this scheduler-thread
     * check could pass just before {@link #complete(RunSummary)} runs on the caller
     * thread, and this method would then write a stale {@code completed:false} record
     * over the terminal {@code completed:true} one. Once {@link #complete} has set the
     * flag under the lock, every subsequent {@code flushSafely} that acquires the lock
     * afterward sees it and no-ops instead of writing.
     */
    private void flushSafely() {
        writeLock.lock();
        try {
            if (completedCalled.get()) {
                return;
            }
            // A mid-run heartbeat has not stopped yet, so it carries no stop_reason.
            write(snapshotSupplier.get(), false, null, null);
        } catch (IOException | RuntimeException e) {
            log.warn("summary_json_write_failed path={} message={}", config.path(), e.getMessage());
        } finally {
            writeLock.unlock();
        }
    }

    private void write(RunSummary summary, boolean completedFlag, Integer exitCode, StopReason stopReason)
            throws IOException {
        write(summary, completedFlag, exitCode, stopReason, null, null, true);
    }

    /** As the 4-arg {@link #write}, with an explicit {@code stop_source}/{@code error_class} pair. */
    private void write(RunSummary summary, boolean completedFlag, Integer exitCode, StopReason stopReason,
            String stopSource, String errorClass) throws IOException {
        write(summary, completedFlag, exitCode, stopReason, stopSource, errorClass, true);
    }

    /**
     * {@code includeMeters=false} is the degraded-fallback shape {@link
     * #writeFinalPartialWithResilience} falls back to when every full-write attempt fails: every
     * field below EXCEPT {@code meters[]} still lands, so a reader keeps {@code completed}/{@code
     * stop_reason}/{@code stop_source}/{@code error_class}/{@code as_of}/{@code objects}/{@code
     * duration_ms}/the recovered-error rollup even in the degraded case — only the (large,
     * self-syncing-with-the-registry) meter dump is dropped.
     */
    private void write(RunSummary summary, boolean completedFlag, Integer exitCode, StopReason stopReason,
            String stopSource, String errorClass, boolean includeMeters) throws IOException {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("schema_version", 2);
        root.put("run_id", summary.runId());
        root.put("args_hash", config.argsHash());
        // Raw argv alongside the narrow resume-safety args_hash — ground truth for
        // corpus reproducibility (args_hash deliberately excludes progress/diagnostic-only flags).
        ArrayNode argvNode = root.putArray("argv");
        config.argv().forEach(argvNode::add);
        root.put("strategy", summary.strategy().toLowerCase(Locale.ROOT));
        root.put("completed", completedFlag);
        if (exitCode == null) {
            root.putNull("exit_code");
        } else {
            root.put("exit_code", exitCode);
        }
        // Schema v2: stop_reason refines `completed`. A mid-run heartbeat
        // (still running) has no reason yet → null; a terminal record names one unless its stop has no
        // attributable reason (e.g. a broken pipe: TerminalStatus.reason()==null → stop_reason also null).
        if (stopReason == null) {
            root.putNull("stop_reason");
        } else {
            root.put("stop_reason", stopReason.wireName());
        }
        // The fleet-forensics fields — which mechanism
        // attributed a STUCK stop (stop_source, e.g. liveness_watchdog vs. transient_retry_cap) and
        // the source-routed error_class (stuck_api_timeouts/stuck_throttle/stuck_unknown), the SAME
        // pair ListCommand's list_stuck_stop marker prints (RunMetrics#stuckErrorClass is the one
        // shared derivation). Both null for every non-STUCK terminal, every mid-run heartbeat, and a
        // COMPLETED run — present (as JSON null) rather than omitted, matching stop_reason/exit_code's
        // always-present-maybe-null shape above, so a fleet parser never needs a key-presence check.
        if (stopSource == null) {
            root.putNull("stop_source");
        } else {
            root.put("stop_source", stopSource);
        }
        if (errorClass == null) {
            root.putNull("error_class");
        } else {
            root.put("error_class", errorClass);
        }
        root.put("started_at", DateTimeFormatter.ISO_INSTANT.format(startedAt));
        // Wall-clock stamp of THIS write, independent of started_at/duration_ms — the only
        // field a reader can use to tell a genuinely fresh write from a stale one on sight,
        // regardless of which write path (periodic, complete(), or a close()-time retry/degrade)
        // produced the file, and regardless of whether a later write attempt silently failed.
        root.put("as_of", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
        // duration_ms is the whole-run clock (post-seed): on a --sort run it includes the
        // merge/publish tail, not listing alone.
        root.put("duration_ms", summary.duration().toMillis());
        // The whole-invocation session clock, seeding included -- the SAME span the live progress
        // line reports, unlike duration_ms above. Additive under schema v2 -- NOT a v3 bump.
        root.put("session_duration_ms", summary.sessionDuration().toMillis());
        // The listing-only clock (equal to duration_ms on a run that never merges) -- the honest
        // denominator for a listing-phase rate, since keys_per_sec/cpu_efficiency stay whole-run for
        // compatibility. Additive under schema v2 -- NOT a v3 bump.
        root.put("listing_duration_ms", summary.listingDuration().toMillis());
        root.put("objects", summary.objects());
        // The rows a resume carried over from a previous attempt: objects describes the DATASET, so
        // objects - recovered_objects is the numerator for anything this process's own rate is
        // measured on (it neither listed nor paid a LIST call for them). Previously visible only on
        // the -v progress line. Additive under schema v2 -- NOT a v3 bump.
        root.put("recovered_objects", summary.recoveredObjects());

        RunConfig rc = config.runConfig();
        writeConfig(root.putObject("config"), rc);
        writeEngineFlags(root.putObject("engine_flags"), rc);
        writeOutput(root.putObject("output"), rc, summary);
        writeCost(root.putObject("cost"), summary);
        writeEfficiency(root.putObject("efficiency"), summary);
        writeEngine(root.putObject("engine"), summary);
        writeRecoveredErrors(root.putObject("recovered_errors"));

        // SeedStep's mode/probes/cut_points/synthesized_cuts/ranges, so post-hoc analysis reads
        // them without scraping logs. Null on a resumed run (seeding never re-runs) or a run that
        // never seeds.
        if (summary.seed() != null) {
            writeSeed(root.putObject("seed"), summary.seed());
        }

        // The bounded time-bin in-flight/progress-rate trajectory rollup + its derived
        // serial_frac/collapse_at_frac/peak_workers/final_workers scalars. Null on the same
        // construction paths `shape` is (no page ever fetched).
        if (summary.trajectory() != null) {
            writeTrajectory(root.putObject("trajectory"), summary.trajectory());
        }

        // The top-N (RunMetrics.SLOW_RANGES_LIMIT) slowest/remaining live ranges at the instant
        // this summary was built, each with its own per-range steal_reason tallies. Always present
        // (an empty array — never omitted) — empty is itself informative (nothing left in flight).
        writeSlowRanges(root.putArray("slow_ranges"), summary.slowRanges());

        // The --sort block, read straight off the registry meters so a
        // sorted run is self-describing (did sort engage, which regime, what did it cost?). Only
        // emitted for a --sort run; the classification signals (page_runs_per_buffer,
        // buffer_sort_fallbacks) are the cheap "was pre-sortedness exploited" tells.
        if (rc.sortEnabled()) {
            writeSort(root.putObject("sort"), rc);
        }

        // Per-call-class latency-phase percentile breakdown. Always present as an array
        // (possibly empty, never omitted) -- an empty array is itself informative (no fetch of any
        // class completed, e.g. a pre-run early exit). The generic meters[] readback below carries
        // each Timer's count/total_ms/max_ms only, not percentiles -- this is the dedicated readback
        // (same reason shape.regime.api_latency_p50_ms/_p99_ms needed one).
        writeProbeLatency(root.putArray("probe_latency"), summary.callClassLatency());

        // Per-page client-service-cost spans. Always present as an array (possibly empty, never
        // omitted), same idiom and same dedicated-percentile-readback reason as probe_latency above.
        writeClientCost(root.putArray("client_cost"), summary.clientCost());

        // OWNER_SPLIT.demand_gated T-vs-Tmax visibility. Omitted entirely (not a null-valued
        // block) when the demand gate never fired this run -- same idiom as seed/shape/trajectory.
        if (summary.demandGate() != null) {
            writeDemandGate(root.putObject("demand_gate"), summary.demandGate());
        }

        // The flat shape feature-vector (post-hoc classification signals). One block,
        // trainable without log joins. Additive under schema v2 — NOT a v3 bump. On a
        // --max-duration/interrupted run this describes only the
        // LISTED SPAN, not the whole bucket (partial mirrors !completed for trainability).
        if (summary.shape() != null) {
            writeShape(root.putObject("shape"), summary, rc, completedFlag);
        }

        if (includeMeters) {
            writeMeters(root.putArray("meters"));
        }

        sink.accept(root);
    }

    /** The {@code config} block: the resolved/effective listing config, echoed field-for-field. */
    private void writeConfig(ObjectNode configNode, RunConfig rc) {
        configNode.put("target", rc.target());
        configNode.put("region", rc.region());
        configNode.put("format", rc.format());
        configNode.put("max_parallel_listings", rc.maxParallelListings());
        configNode.put("no_sign_request", rc.noSignRequest());
        configNode.put("request_payer", rc.requestPayer());
        if (rc.rateLimitApi() == null) {
            configNode.putNull("rate_limit_api");
        } else {
            configNode.put("rate_limit_api", rc.rateLimitApi());
        }
        configNode.put("progress_interval_ms", rc.progressIntervalMs());
        ArrayNode filtersNode = configNode.putArray("filters");
        rc.filters().forEach(filtersNode::add);
    }

    /** The genuine engine toggles (on/off, plus the value-taking {@code tail_floor} mode), so a
     * run is self-identifying (e.g. --no-owner-split vs. the default) without re-deriving it
     * from args_hash, which deliberately excludes them. max_duration_ms is likewise excluded
     * from args_hash but must still be recognizable in the corpus. This block also carries
     * the other --engine-toggle names (owner_split is unchanged — the same
     * field --no-owner-split always echoed, sourced from the merged EngineToggles). */
    private void writeEngineFlags(ObjectNode engineFlagsNode, RunConfig rc) {
        EngineToggles toggles = rc.engineToggles();
        engineFlagsNode.put("owner_split", toggles.ownerSplit());
        engineFlagsNode.put("density_ewma", toggles.densityEwma());
        engineFlagsNode.put("radix_bands", toggles.radixBands());
        engineFlagsNode.put("structure_probes", toggles.structureProbes());
        engineFlagsNode.put("far_ahead", toggles.farAhead());
        engineFlagsNode.put("alphabet_pivots", toggles.alphabetPivots());
        engineFlagsNode.put("reflect", toggles.reflect());
        engineFlagsNode.put("confetti_feedback", toggles.confettiFeedback());
        engineFlagsNode.put("reflect_lift", toggles.reflectLift());
        engineFlagsNode.put("fanout_tiling", toggles.fanoutTiling());
        // readahead (opt-in, default OFF) and mass_aware_seed (default ON; --engine-toggle
        // mass_aware_seed=off is the opt-out) are both new-mechanism
        // toggles deliberately outside EngineToggles.NAMES (see its javadoc) but are still full
        // EngineToggles fields excluded from args_hash, so engine_flags is the ONLY post-hoc
        // reproducibility signal for them. Do not omit them here — that silently breaks the signal.
        engineFlagsNode.put("readahead", toggles.readahead());
        engineFlagsNode.put("mass_aware_seed", toggles.massAwareSeed());
        engineFlagsNode.put("rate_anchored_sensing", toggles.rateAnchoredSensing());
        // tail_floor is the one value-taking toggle, so it renders as its mode's own code string
        // rather than a boolean — same reproducibility reason as the two above.
        engineFlagsNode.put("tail_floor", toggles.tailFloor().code());
        if (rc.maxDurationMs() == null) {
            engineFlagsNode.putNull("max_duration_ms");
        } else {
            engineFlagsNode.put("max_duration_ms", rc.maxDurationMs());
        }
    }

    private void writeOutput(ObjectNode outputNode, RunConfig rc, RunSummary summary) {
        outputNode.put("format", rc.format());
        outputNode.put("files", summary.outputFiles());
        outputNode.put("compressed_size_bytes", summary.compressedBytes());
    }

    private void writeCost(ObjectNode costNode, RunSummary summary) {
        costNode.put("api_calls", summary.apiCalls());
        costNode.put("cost_usd", summary.costUsd());
        // The rate cost_usd was derived from, named rather than implied: it is a single-region
        // reference rate, so a reader in another region (or on another provider) can rescale
        // instead of trusting a number swath cannot know. Same constant the terminal summary
        // block labels its figure with. Additive under schema v2 — NOT a v3 bump.
        ObjectNode basisNode = costNode.putObject("basis");
        basisNode.put("rate_per_1k_usd", RunMetrics.LIST_COST_PER_1K_USD);
        basisNode.put("source", RunMetrics.LIST_COST_SOURCE);
    }

    private void writeEfficiency(ObjectNode efficiencyNode, RunSummary summary) {
        efficiencyNode.put("keys_per_sec", summary.keysPerSecond());
        efficiencyNode.put("api_calls_per_1k_objects", summary.apiCallsPer1kObjects());
        putLongOrNull(efficiencyNode, "peak_rss_bytes", summary.peakRssBytes());
        putLongOrNull(efficiencyNode, "peak_heap_bytes", summary.peakHeapBytes());
        putDoubleOrNull(efficiencyNode, "cpu_seconds", summary.cpuSeconds());
        putDoubleOrNull(efficiencyNode, "cpu_efficiency", summary.cpuEfficiency());
        // Derived waste/efficiency ratios, computed once from existing counters (RunSummary's
        // javadoc has the exact numerator/denominator + rationale for each).
        efficiencyNode.put("overfetch_ratio", summary.overfetchRatio());
        efficiencyNode.put("page_fill_ratio", summary.pageFillRatio());
        efficiencyNode.put("empty_split_ratio", summary.emptySplitRatio());
        efficiencyNode.put("wasted_probe_ratio", summary.wastedProbeRatio());
        efficiencyNode.put("steal_success_rate", summary.stealSuccessRate());
        efficiencyNode.put("compression_ratio", summary.compressionRatio());
    }

    private void writeEngine(ObjectNode engineNode, RunSummary summary) {
        engineNode.put("pages", summary.pages());
        engineNode.put("peak_in_flight", summary.peakInFlight());
        // Time-weighted average in-flight listing count — peak_in_flight saturates once
        // the concurrency ceiling is hit; avg_in_flight is the metric a sustained-parallelism fix moves.
        engineNode.put("avg_in_flight", summary.avgInFlight());
        // Ramp-up timings: how long the run took to reach its first steal and its peak
        // concurrency. Both were previously readable only off the -v list_run_diagnostics line,
        // unlike every other field on it; -1 when the event never happened. Additive under
        // schema v2 — NOT a v3 bump.
        putLongOrNull(engineNode, "time_to_first_steal_ms", summary.timeToFirstStealMs());
        putLongOrNull(engineNode, "time_to_peak_in_flight_ms", summary.timeToPeakInFlightMs());
        engineNode.put("steals", summary.steals());
        engineNode.put("splits", summary.splits());
        engineNode.put("errors", summary.errors());
    }

    /** Recovered-error rollup. Even a COMPLETED run must surface that recovery signals
     * fired — recovery must not HIDE that errors happened. These are already in `meters[]` but
     * scraping the meter array per-run is fragile, so the load-bearing counters get a stable,
     * named home here: attempt-timeout events, the timeout-shed / latency-freeze / connection-churn
     * engagement counters, and the min effective-T the AIMD/shed brake reached (the shed floor /
     * "ceiling hit"). A run with all-zeros here saw a clean network; non-zeros are the recovered
     * (or terminal) disposition. */
    private void writeRecoveredErrors(ObjectNode recoveredNode) {
        recoveredNode.put("attempt_timeout_events",
                (long) counterCount("swath.throttle.events", "type", "attempt_timeout"));
        recoveredNode.put("timeout_sheds", (long) counterCount("swath.aimd.timeout_shed"));
        recoveredNode.put("latency_freezes", (long) counterCount("swath.aimd.latency_freeze"));
        // The transient-timeout growth-freeze rung, parallel to latency_freezes above.
        recoveredNode.put("growth_freezes", (long) counterCount("swath.aimd.growth_freeze"));
        // The denominator for the two freeze counters above: successes that actually REACHED the
        // growth-freeze gates. A success at Tmax (or inside a throttle cool-down) returns before the
        // gates and can never freeze, so raw freeze counts are incomparable across runs at different
        // saturation; latency_freezes / freeze_gate_checks is the comparable rate.
        recoveredNode.put("freeze_gate_checks", (long) counterCount("swath.aimd.freeze_gate_checks"));
        recoveredNode.put("connection_aborted", (long) counterCount("swath.s3.pool.connection_aborted"));
        recoveredNode.put("handshakes", (long) counterCount("swath.s3.pool.handshakes"));
        // Socket-closure / IOException-wrapper faults (a non-SdkException RuntimeException whose
        // cause chain holds an IOException, e.g. UncheckedIOException(SocketException("Socket closed")))
        // reclassified transient-NETWORK and ridden out instead of crashing at error_class=unknown.
        recoveredNode.put("socket_closure", (long) counterCount("swath.s3.socket_closure_recovered"));
        // 0 = the target was never set (a run that never ran the AIMD engine, e.g. --checkpoint none);
        // render null so a consumer doesn't read it as "shed all the way to T=0".
        long minT = (long) gaugeValue("swath.aimd.target_low_water");
        if (minT <= 0) {
            recoveredNode.putNull("min_effective_t");
        } else {
            recoveredNode.put("min_effective_t", minT);
        }
    }

    private void writeSeed(ObjectNode seedNode, RunSummary.SeedSummary seed) {
        seedNode.put("mode", seed.mode());
        seedNode.put("probes", seed.probes());
        seedNode.put("cut_points", seed.cutPoints());
        seedNode.put("synthesized_cuts", seed.synthesizedCuts());
        seedNode.put("ranges", seed.ranges());
        // The per-probed-level seed decision trace (bounded — SeedStep caps probes ≤ ~256).
        ArrayNode decisionsNode = seedNode.putArray("decisions");
        for (RunSummary.SeedSummary.SeedDecision d : seed.decisions()) {
            ObjectNode dn = decisionsNode.addObject();
            dn.put("prefix", d.prefix());
            dn.put("fanout", d.fanout());
            dn.put("truncated", d.truncated());
            dn.put("classification", d.classification());
            dn.put("cuts_kept", d.cutsKept());
            dn.put("cuts_discarded", d.cutsDiscarded());
        }
    }

    private void writeTrajectory(ObjectNode trajectoryNode, RunSummary.TrajectorySummary trajectory) {
        ArrayNode inFlightNode = trajectoryNode.putArray("in_flight");
        for (double v : trajectory.inFlight()) {
            inFlightNode.add(v);
        }
        ArrayNode rateNode = trajectoryNode.putArray("progress_rate");
        for (double v : trajectory.progressRate()) {
            rateNode.add(v);
        }
        trajectoryNode.put("serial_frac", trajectory.serialFrac());
        double collapseAtFrac = trajectory.collapseAtFrac();
        if (collapseAtFrac < 0) {
            trajectoryNode.putNull("collapse_at_frac");
        } else {
            trajectoryNode.put("collapse_at_frac", collapseAtFrac);
        }
        trajectoryNode.put("peak_workers", trajectory.peakWorkers());
        trajectoryNode.put("final_workers", trajectory.finalWorkers());
    }

    private void writeSlowRanges(ArrayNode slowRangesNode, List<RunSummary.SlowRange> slowRanges) {
        for (RunSummary.SlowRange r : slowRanges) {
            ObjectNode rn = slowRangesNode.addObject();
            rn.put("lo", r.lo());
            rn.put("hi", r.hi());
            rn.put("cursor", r.cursor());
            // An open-frontier range's est_remaining is
            // Double.POSITIVE_INFINITY (StealMath#estRemaining) — JSON null, never the literal
            // string "Infinity" a bare ObjectNode.put(double) would emit. The sort order above
            // (Comparator.reversed() over a plain double) is unaffected: +Infinity still compares
            // largest, so an open-frontier range still surfaces first in slow_ranges[] — only its
            // JSON rendering changes.
            putFiniteDoubleOrNull(rn, "est_remaining", r.estRemaining());
            rn.put("drain_rate", r.drainRate());
            ObjectNode reasonsNode = rn.putObject("steal_reasons");
            reasonsNode.put("cursor_passed_pivot", r.cursorPassedPivot());
            reasonsNode.put("no_pivot", r.noPivot());
            reasonsNode.put("structure_suppressed", r.structureSuppressed());
            reasonsNode.put("demand_gated", r.demandGated());
        }
    }

    private void writeSort(ObjectNode sortNode, RunConfig rc) {
        sortNode.put("enabled", true);
        sortNode.put("segments", (long) counterCount("swath.sort.segments.written"));
        sortNode.put("passes", (long) counterCount("swath.sort.merge.passes"));
        sortNode.put("segment_bytes", (long) counterCount("swath.sort.segment.bytes"));
        sortNode.put("merge_ms", timerTotalMs("swath.sort.merge.latency"));
        // The parallel range merge's serial prologue. INCLUDED in merge_ms above and broken out here
        // because it is the one term that does not shrink as R rises: subtract it to see the ranges'
        // own scaling. Zero on the default serial merge, which never samples boundaries.
        sortNode.put("merge_boundaries_ms", timerTotalMs("swath.sort.merge.boundaries.latency"));
        sortNode.put("page_runs_per_buffer", distributionMean("swath.sort.page_runs_per_buffer"));
        sortNode.put("buffer_sort_fallbacks", (long) stealReasonCount("SORT", "buffer_sort_fallback"));
        // A merge-only `--sort --resume` (the listing was already complete in the
        // checkpoint; this process re-ran ONLY the k-way merge over already-durable staging
        // segments, ListRunner#runSortMergeOnly) recovers `segments`/the top-level `objects`
        // from the checkpoint/merge-output counts rather than the listing-phase counters (which
        // never ran in THIS process) -- see RunMetrics#recordRecoveredSortSegments and
        // #summary(...,objectsOverride). This marker distinguishes that recovered-from-checkpoint
        // reporting from a freshly-listed run's counter-derived counts, reusing the SAME
        // `SORT.merge_redone` engagement counter ListRunner#runSortMergeOnly already records
        // (did-it-fire, no new meter needed).
        sortNode.put("merge_only_resume", stealReasonCount("SORT", "merge_redone") > 0);
        // Segments/passes are populated exactly ONCE, after the whole merge
        // finishes, so a mid-merge summary.json snapshot is flat/indistinguishable from a
        // wedged run. merge_progress_units is fed by the SAME swath.progress.units counter
        // KWayMerge's per-pass batched callback already bumps live during merge (the internal
        // LivenessWatchdog's own forward-progress signal) -- surfacing it here gives an
        // external watcher (log tail / summary.json poller) a field that genuinely advances
        // mid-merge.
        sortNode.put("merge_progress_units", (long) counterCount("swath.progress.units"));
        // Sort observability polish: the binding merge-pass-width knob (SortConfig#effectiveFanIn),
        // echoed directly so a run is self-describing about which knob binds — instead of only
        // being inferable from segments/passes (docs/usage.md "Which knob actually binds"). Null
        // when the caller passes the pre-fan-in RunConfig constructor.
        if (rc.sortEffectiveFanIn() == null) {
            sortNode.putNull("effective_fan_in");
        } else {
            sortNode.put("effective_fan_in", rc.sortEffectiveFanIn());
        }
    }

    /** Per-call-class latency-phase percentile breakdown -- always present as an array
     * (possibly empty, never omitted); the generic meters[] readback carries each Timer's
     * count/total_ms/max_ms only, not percentiles -- this is the dedicated percentile readback. */
    private void writeProbeLatency(ArrayNode probeLatencyNode,
            List<RunSummary.CallClassLatencySummary> callClassLatency) {
        for (RunSummary.CallClassLatencySummary c : callClassLatency) {
            ObjectNode cn = probeLatencyNode.addObject();
            cn.put("call_class", c.callClass());
            cn.put("phase", c.phase());
            cn.put("count", c.count());
            putDoubleOrNullBoxed(cn, "p50_ms", c.p50Ms());
            putDoubleOrNullBoxed(cn, "p90_ms", c.p90Ms());
            putDoubleOrNullBoxed(cn, "p99_ms", c.p99Ms());
            cn.put("max_ms", c.maxMs());
        }
    }

    /** Per-page client-service-cost span percentiles -- always present as an array (possibly empty,
     * never omitted); the dedicated percentile readback, for the same reason writeProbeLatency is one. */
    private void writeClientCost(ArrayNode clientCostNode, List<RunSummary.ClientCostSpan> clientCost) {
        for (RunSummary.ClientCostSpan s : clientCost) {
            ObjectNode sn = clientCostNode.addObject();
            sn.put("span", s.span());
            sn.put("count", s.count());
            putDoubleOrNullBoxed(sn, "p50_ms", s.p50Ms());
            putDoubleOrNullBoxed(sn, "p90_ms", s.p90Ms());
            putDoubleOrNullBoxed(sn, "p99_ms", s.p99Ms());
            sn.put("max_ms", s.maxMs());
        }
    }

    private void writeDemandGate(ObjectNode dg, RunSummary.DemandGateSummary demandGate) {
        dg.put("events", demandGate.events());
        dg.put("last_t", demandGate.lastT());
        dg.put("min_t", demandGate.minT());
        dg.put("t_max", demandGate.tMax());
    }

    private void writeMeters(ArrayNode metersNode) {
        for (Meter meter : registry.getMeters()) {
            metersNode.add(meterNode(meter));
        }
    }

    /**
     * The flat {@code shape} feature-vector. The engine-observed signals come from {@code
     * summary.shape()}; the run-config confounds (region, worker count) and
     * the process fingerprint (binary/git sha, timestamps) are folded in here. {@code argv} is
     * the top-level array — referenced, not duplicated. {@code partial} mirrors {@code !completed}:
     * a {@code true} value means the block describes only the listed span, not the whole bucket (I5).
     */
    private void writeShape(ObjectNode shape, RunSummary summary, RunConfig rc, boolean completedFlag) {
        RunSummary.ShapeSummary s = summary.shape();
        shape.put("partial", !completedFlag);

        ArrayNode alphabet = shape.putArray("alphabet_cardinality");
        for (int c : s.alphabetCardinality()) {
            alphabet.add(c);
        }
        shape.put("alphabet_positions_observed", s.alphabetPositionsObserved());
        shape.put("alphabet_cardinality_note", "run-level aggregate over per-range positions past "
                + "divergence; entropy not computed");

        ArrayNode depth = shape.putArray("divergence_depth_histogram");
        for (long d : s.divergenceDepthHistogram()) {
            depth.add(d);
        }
        shape.put("mass_skew_gini", s.massSkewGini());

        ObjectNode fanout = shape.putObject("delimiter_fanout");
        fanout.put("max", s.delimiterFanoutMax());
        fanout.put("total", s.delimiterFanoutTotal());
        fanout.put("probes", s.delimiterProbes());

        ObjectNode regime = shape.putObject("regime");
        putDoubleOrNullBoxed(regime, "api_latency_p50_ms", s.apiLatencyP50Ms());
        putDoubleOrNullBoxed(regime, "api_latency_p99_ms", s.apiLatencyP99Ms());
        // api_latency_* spans EVERY call class, cheap structure/pivot probes included, so it reads
        // low of what a data page actually costs. The worker_page total is what a serial lister
        // would pay per page — the honest serial-baseline estimator.
        RunSummary.CallClassLatencySummary workerPage = workerPageTotalLatency(summary);
        putDoubleOrNullBoxed(regime, "worker_page_latency_p50_ms",
                workerPage == null ? null : workerPage.p50Ms());
        putDoubleOrNullBoxed(regime, "worker_page_latency_p99_ms",
                workerPage == null ? null : workerPage.p99Ms());
        regime.put("worker_count", rc.maxParallelListings());
        if (rc.region() == null) {
            regime.putNull("region");
        } else {
            regime.put("region", rc.region());
        }
        // The AIMD-driving THROTTLE.* steal_reasons count ONLY genuine store
        // backpressure (real 503 SlowDown / 5xx). Client attempt-timeouts and network faults are
        // recorded separately (TRANSIENT.* steal_reasons + swath.throttle.events{type}) and do not
        // vote concurrency down, so they are not folded into the throttle regime counts.
        regime.put("throttle_includes_attempt_timeout", false);

        ObjectNode fingerprint = shape.putObject("fingerprint");
        putTextOrNull(fingerprint, "binary_sha256", RunFingerprint.binarySha256());
        putTextOrNull(fingerprint, "git_sha", RunFingerprint.gitSha());
        fingerprint.put("started_at", DateTimeFormatter.ISO_INSTANT.format(startedAt));
        fingerprint.put("finished_at",
                DateTimeFormatter.ISO_INSTANT.format(startedAt.plus(summary.duration())));
        // argv is the top-level "argv" array — referenced here, not duplicated.
    }

    /** The whole-call {@code worker_page} latency row, or {@code null} when no page was ever timed. */
    private static RunSummary.CallClassLatencySummary workerPageTotalLatency(RunSummary summary) {
        for (RunSummary.CallClassLatencySummary c : summary.callClassLatency()) {
            if (RunMetrics.CALL_CLASS_WORKER_PAGE.equals(c.callClass())
                    && RunMetrics.LATENCY_PHASE_TOTAL.equals(c.phase())) {
                return c;
            }
        }
        return null;
    }

    private static void putTextOrNull(ObjectNode node, String field, String value) {
        if (value == null) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    private static void putDoubleOrNullBoxed(ObjectNode node, String field, Double value) {
        if (value == null) {
            node.putNull(field);
        } else {
            node.put(field, value.doubleValue());
        }
    }

    /** {@code RunSummary}'s {@code -1} "unavailable" sentinel (long fields) renders as JSON {@code null}. */
    private static void putLongOrNull(ObjectNode node, String field, long value) {
        if (value < 0) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    /** {@code RunSummary}'s {@code -1} "unavailable" sentinel (double fields) renders as JSON {@code null}. */
    private static void putDoubleOrNull(ObjectNode node, String field, double value) {
        if (value < 0) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    /**
     * A non-finite double ({@code StealMath#estRemaining}'s {@code
     * Double.POSITIVE_INFINITY} for an open-frontier range — {@code hi == null}, "always scores
     * highest") renders as JSON {@code null}, never {@code ObjectNode.put(double)}'s literal string
     * {@code "Infinity"}/{@code "NaN"} (Jackson's default double-to-JSON behavior for non-finite
     * values, which is NOT valid JSON per RFC 8259 and would silently turn a numeric field into a
     * string, breaking the additive-schema claim for every consumer that parses it as a number).
     * {@code null} here means "open frontier / unbounded remaining span", not "unavailable" — kept
     * distinct from {@link #putDoubleOrNull}'s {@code -1} sentinel semantics.
     */
    private static void putFiniteDoubleOrNull(ObjectNode node, String field, double value) {
        if (Double.isFinite(value)) {
            node.put(field, value);
        } else {
            node.putNull(field);
        }
    }

    private static ObjectNode meterNode(Meter meter) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("name", meter.getId().getName());
        node.put("type", meter.getId().getType().name().toLowerCase(Locale.ROOT));
        ObjectNode tags = node.putObject("tags");
        for (Tag tag : meter.getId().getTags()) {
            tags.put(tag.getKey(), tag.getValue());
        }
        if (meter instanceof Timer timer) {
            node.put("count", timer.count());
            node.put("total_ms", timer.totalTime(TimeUnit.MILLISECONDS));
            node.put("max_ms", timer.max(TimeUnit.MILLISECONDS));
        } else if (meter instanceof FunctionCounter fc) {
            node.put("value", fc.count());
        } else if (meter instanceof Counter c) {
            node.put("value", c.count());
        } else if (meter instanceof Gauge g) {
            double v = g.value();
            if (Double.isNaN(v)) {
                node.putNull("value");
            } else {
                node.put("value", v);
            }
        }
        return node;
    }

    // ---- --sort block: read the sort meters straight off the registry ----

    private double counterCount(String name) {
        Counter c = registry.find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    private double counterCount(String name, String tagKey, String tagValue) {
        Counter c = registry.find(name).tag(tagKey, tagValue).counter();
        return c == null ? 0.0 : c.count();
    }

    private double gaugeValue(String name) {
        Gauge g = registry.find(name).gauge();
        return g == null ? 0.0 : g.value();
    }

    private long timerTotalMs(String name) {
        Timer t = registry.find(name).timer();
        return t == null ? 0L : (long) t.totalTime(TimeUnit.MILLISECONDS);
    }

    private double distributionMean(String name) {
        DistributionSummary s = registry.find(name).summary();
        return s == null ? 0.0 : s.mean();
    }

    private double stealReasonCount(String outcome, String reason) {
        Counter c = registry.find("swath.steal_reason")
                .tag("outcome", outcome).tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    private void atomicWrite(ObjectNode root) throws IOException {
        Path path = config.path();
        // tmp must live in the same directory as the target: same-filesystem rename is what
        // makes the move atomic (and is what lets the AtomicMoveNotSupportedException fallback
        // below still be a same-directory rename rather than a cross-filesystem copy).
        Path tmp = parent != null
                ? parent.resolve(path.getFileName().toString() + TMP_SUFFIX)
                : path.resolveSibling(path.getFileName().toString() + TMP_SUFFIX);
        try {
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), root);
            try {
                Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // Still a same-directory rename (tmp is fully written before this point), so it
                // is atomic on POSIX even without the ATOMIC_MOVE flag — a reader never observes
                // a torn/half-written file.
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            // Best-effort cleanup: never let a leftover-tmp deletion failure mask the real
            // outcome (success, or the exception already propagating out of the try above).
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException e) {
                log.warn("summary_json_tmp_cleanup_failed path={} message={}", tmp, e.getMessage());
            }
        }
    }

    /**
     * Sidecar wiring: where/how often to write, plus the integrity fingerprint.
     * {@code argv} is the centrally sanitized invocation representation: endpoint values are
     * replaced by a fixed marker and control characters are escaped. It stays separate from
     * {@code runConfig} (the resolved/effective values) rather than being folded into it.
     */
    public record Config(Path path, Duration flushInterval, String argsHash, RunConfig runConfig,
                          List<String> argv) {
        public Config {
            // Enforce at the durable-artifact boundary as well as at the CLI producer. This keeps a
            // future non-CLI caller from accidentally bypassing the central endpoint/control policy.
            argv = SafeInput.argv(argv);
        }
    }

    /**
     * The effective/resolved listing config echoed into the {@code config} block.
     * {@code engineToggles}/{@code maxDurationMs} are the genuine
     * engine on/off toggles, rendered into the {@code engine_flags} block so a run is
     * self-identifying (e.g. a {@code --no-owner-split}/{@code --engine-toggle} ablation run is
     * distinguishable from the default in the summary alone, without re-deriving it from {@code
     * args_hash}).
     */
    public record RunConfig(
            String target,
            String region,
            String format,
            int maxParallelListings,
            boolean noSignRequest,
            Double rateLimitApi,
            long progressIntervalMs,
            List<String> filters,
            EngineToggles engineToggles,
            Long maxDurationMs,
            boolean sortEnabled,
            Integer sortEffectiveFanIn,
            boolean requestPayer) {

        public RunConfig withSortEnabled(boolean sortEnabled) {
            return new RunConfig(target, region, format, maxParallelListings, noSignRequest, rateLimitApi,
                    progressIntervalMs, filters, engineToggles, maxDurationMs, sortEnabled,
                    sortEffectiveFanIn, requestPayer);
        }

        public RunConfig withSortEffectiveFanIn(Integer sortEffectiveFanIn) {
            return new RunConfig(target, region, format, maxParallelListings, noSignRequest, rateLimitApi,
                    progressIntervalMs, filters, engineToggles, maxDurationMs, sortEnabled,
                    sortEffectiveFanIn, requestPayer);
        }

        public RunConfig withRequestPayer(boolean requestPayer) {
            return new RunConfig(target, region, format, maxParallelListings, noSignRequest, rateLimitApi,
                    progressIntervalMs, filters, engineToggles, maxDurationMs, sortEnabled,
                    sortEffectiveFanIn, requestPayer);
        }
    }
}
