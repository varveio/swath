/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-JVM liveness watchdog — the SECONDARY exit guarantee for a run that makes ZERO forward
 * progress yet never exits (e.g. a wedged TLS read that even the client attempt-timeout can't unblock).
 * The PRIMARY unblock is the S3 client's overall {@code apiCallTimeout} (each blocked logical call
 * returns bounded); this watchdog is the backstop for when even that fails.
 *
 * <p>ON by default (armed alongside {@link DeadlineCanceller}), tunable/disable-able via the CLI
 * {@code --stall-timeout} flag. A daemon poller reads a monotonic {@linkplain RunMetrics#progressSignal
 * progress signal}; when it observes no forward progress for the stall window it runs the
 * escalation ladder:
 * <ol>
 *   <li><b>cooperative cancel</b> with {@link StopReason#STUCK} — a cooperative flag the scan loop
 *       polls (like {@link DeadlineCanceller}); enough for a run whose workers are merely slow;</li>
 *   <li>after a short grace, a <b>forensic snapshot</b> to the log
 *       — thread stacks (best-effort: {@link Thread#getAllStackTraces()} surfaces only platform
 *       threads, so a mounted virtual thread's own stack is not separately visible — its carrier's top
 *       frame is the closest available signal), in-flight/worker counts, and last-progress timestamps;
 *       SDK connection-pool stats are NOT reflected out (the SDK exposes none without fragile
 *       reflection, which this deliberately avoids). Emitted BEFORE the interrupt below so the snapshot
 *       captures the wedged state before it is perturbed;</li>
 *   <li>then <b>interrupt</b> the worker/carrier threads this class can see by name — best-effort:
 *       {@link Thread#getAllStackTraces()} enumerates only PLATFORM threads (including a virtual
 *       thread's carrier), so interrupting a carrier does NOT itself interrupt the virtual thread
 *       MOUNTED on it. It can still wake a worker genuinely parked in platform-thread code (e.g. the
 *       Parquet writer lanes' blocking file I/O, {@link io.varve.swath.concurrent.Scope
 *       #ofPlatformThreads}); a virtual thread wedged in a native socket read is NOT reliably woken by
 *       this step — {@code halt} below is the real guarantee for that case, not this one;</li>
 *   <li>after a further grace (~one {@code apiCallTimeout}), {@link Runtime#halt(int)} — the only
 *       thing that reliably exits a JVM whose carrier thread is stuck inside a native socket read.</li>
 * </ol>
 *
 * <p><b>Phase-awareness.</b> The progress signal ({@link RunMetrics#progressSignal()}) advances in
 * every phase — page/object completion during LISTING, sort-segment flush while the encoder drains,
 * and {@link RunMetrics#markProgress()} ticks through the sort-merge tail (every merge-write stride)
 * and the finalize/publish tail (footer-boundary ticks; byte-keyed ticks additionally remain on the
 * metadata-less manifest-validation fallback) — so a nearly-done sorted run in its final k-way merge or its manifest
 * finalize (no page completes, but rows/bytes still move) does NOT false-trip. The one residual gap is
 * a single {@code fsync} of a multi-GB part: that is one blocking syscall with no intra-call ticks, so
 * the tick lands immediately before it and a finalize whose fsync alone exceeds the stall window can
 * still trip — billion-scale {@code --sort} operators should size {@code --stall-timeout} accordingly.
 *
 * <p><b>Testability.</b> The whole ladder is driven by the pure {@link #tick()} state machine reading
 * an injected clock and progress supplier and invoking injected {@code halt}/{@code interrupt}/{@code
 * dump} actions — so a test can advance a fake clock and assert the ladder reaches {@code halt}
 * WITHOUT scheduling a real thread or killing the JVM. {@link #arm} wires the production defaults.
 *
 * <p><b>Coexistence with {@code --max-duration}.</b> Both are cooperative and set the same
 * {@link CancellationToken}, whose reason attribution is first-writer-wins — so a {@code max_duration}
 * (or signal) cancel that fired first KEEPS its reason; the watchdog does not overwrite it. But the
 * watchdog still runs its interrupt→halt backstop if that cancel's unwind itself wedges past the stall
 * window: reaching {@code halt} means the process failed to exit cooperatively, which is a stuck-wedge
 * regardless of what first requested the stop, so {@code halt} always uses the stuck exit code.
 */
public final class LivenessWatchdog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LivenessWatchdog.class);

    /** Escalation state — monotonic; never de-escalates once tripping begins (cancel is committed). */
    enum State { HEALTHY, CANCELLED, INTERRUPTED, HALTED }

    private final ScheduledExecutorService scheduler;   // null when disarmed
    private final CancellationToken token;
    private final LongSupplier clock;
    private final LongSupplier progressSupplier;
    private final LongSupplier realProgressSupplier;
    // Classifies the proximate cause of a stuck disposition into a distinct
    // error_class token (stuck_api_timeouts / stuck_throttle / stuck_unknown) for the abort markers.
    private final Supplier<String> errorClassSupplier;
    private final IntConsumer haltAction;
    private final Runnable interruptAction;
    private final Runnable dumpAction;
    private final int stuckExitCode;

    private final long stallWindowNanos;      // <= 0 when the total-freeze tripwire is disabled
    private final long noProgressWindowNanos; // <= 0 when the zero-real-progress backstop is disabled
    private final long graceInterruptNanos;
    private final long graceHaltNanos;

    // Poller state — touched only from the single scheduler thread (or a test's single caller), so
    // plain fields are sufficient; the actions they invoke publish through their own memory barriers.
    private State state = State.HEALTHY;
    private long lastProgress;
    private long lastProgressNanos;
    private long lastRealProgress;
    private long lastRealProgressNanos;
    private long escalatedNanos;

    LivenessWatchdog(ScheduledExecutorService scheduler, CancellationToken token, LongSupplier clock,
                     LongSupplier progressSupplier, LongSupplier realProgressSupplier,
                     Supplier<String> errorClassSupplier, IntConsumer haltAction,
                     Runnable interruptAction, Runnable dumpAction, int stuckExitCode, Duration stallWindow,
                     Duration noProgressWindow, Duration graceInterrupt, Duration graceHalt) {
        this.scheduler = scheduler;
        this.token = token;
        this.clock = clock;
        this.progressSupplier = progressSupplier;
        this.realProgressSupplier = realProgressSupplier;
        this.errorClassSupplier = errorClassSupplier;
        this.haltAction = haltAction;
        this.interruptAction = interruptAction;
        this.dumpAction = dumpAction;
        this.stuckExitCode = stuckExitCode;
        this.stallWindowNanos = stallWindow.toNanos();
        this.noProgressWindowNanos = noProgressWindow.toNanos();
        this.graceInterruptNanos = graceInterrupt.toNanos();
        this.graceHaltNanos = graceHalt.toNanos();
        this.lastProgress = progressSupplier.getAsLong();
        this.lastProgressNanos = clock.getAsLong();
        this.lastRealProgress = realProgressSupplier.getAsLong();
        this.lastRealProgressNanos = clock.getAsLong();
    }

    /** After the cooperative cancel, wait this long for a cooperative unwind before interrupting. */
    static final Duration DEFAULT_GRACE_INTERRUPT = Duration.ofSeconds(10);
    /** After the interrupt + dump, wait ~one apiCallTimeout for the wake to take effect before halt. */
    static final Duration DEFAULT_GRACE_HALT = Duration.ofSeconds(60);
    /**
     * Default zero-real-progress backstop window. 10m is safely larger than the 120s stall window
     * AND larger than any realistic slow-503-grind inter-page gap (a real THR-1 grind commits a page
     * every few seconds — {@code realProgressSignal()} advances long before 10m), so it bounds a run
     * that is ACTIVE (throttle/retry events flowing) yet commits ZERO real pages, without regressing a
     * legitimately slow-but-progressing run.
     */
    public static final Duration DEFAULT_NO_PROGRESS_TIMEOUT = Duration.ofMinutes(10);

    /**
     * Arm the watchdog with production defaults, or a disarmed no-op when BOTH tripwires are disabled
     * ({@code stallWindow} AND {@code noProgressWindow} null/non-positive — the {@code --stall-timeout
     * 0/none/off} + {@code --no-progress-timeout 0/none/off} disable). Either window on its own arms the
     * poller. The {@code noProgressWindow} backstop bounds a run that is ACTIVE (throttle/retry
     * events keep {@link RunMetrics#progressSignal()} climbing) yet commits ZERO real pages
     * ({@link RunMetrics#realProgressSignal()} flat), which the stall tripwire alone would never trip.
     * The poller runs on a daemon so a clean run leaks no thread; {@link #close()} tears it down
     * (try-with-resources around the run).
     */
    public static LivenessWatchdog arm(CancellationToken token, RunMetrics metrics, Duration stallWindow,
                                       Duration noProgressWindow, int stuckExitCode) {
        boolean stallEnabled = isEnabled(stallWindow);
        boolean noProgressEnabled = isEnabled(noProgressWindow);
        // Windows passed to the constructor as ZERO when disabled, so the corresponding tick() guard
        // ({@code windowNanos > 0}) keeps that tripwire off — never trips a disabled window.
        Duration stall = stallEnabled ? stallWindow : Duration.ZERO;
        Duration noProgress = noProgressEnabled ? noProgressWindow : Duration.ZERO;
        if (!stallEnabled && !noProgressEnabled) {
            return new LivenessWatchdog(null, token, System::nanoTime, metrics::progressSignal,
                    metrics::realProgressSignal, metrics::classifyStuckErrorClass,
                    code -> Runtime.getRuntime().halt(code), () -> { },
                    () -> { }, stuckExitCode, stall, noProgress, DEFAULT_GRACE_INTERRUPT, DEFAULT_GRACE_HALT);
        }
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r ->
                Thread.ofPlatform().daemon(true).name("swath-liveness-watchdog").unstarted(r));
        LivenessWatchdog watchdog = new LivenessWatchdog(
                scheduler, token, System::nanoTime, metrics::progressSignal, metrics::realProgressSignal,
                metrics::classifyStuckErrorClass,
                code -> Runtime.getRuntime().halt(code),
                LivenessWatchdog::interruptWorkers,
                () -> dumpForensics(metrics), stuckExitCode,
                stall, noProgress, DEFAULT_GRACE_INTERRUPT, DEFAULT_GRACE_HALT);
        // Poll off whichever enabled window is SMALLER, so both tripwires stay responsive.
        long basisMs = Long.MAX_VALUE;
        if (stallEnabled) {
            basisMs = Math.min(basisMs, stallWindow.toMillis());
        }
        if (noProgressEnabled) {
            basisMs = Math.min(basisMs, noProgressWindow.toMillis());
        }
        long pollMs = Math.max(250L, Math.min(basisMs / 4, 5_000L));
        scheduler.scheduleAtFixedRate(watchdog::tickSafely, pollMs, pollMs, TimeUnit.MILLISECONDS);
        return watchdog;
    }

    private static boolean isEnabled(Duration window) {
        return window != null && !window.isZero() && !window.isNegative();
    }

    /**
     * Run one poll of the ladder, swallowing any error so the daemon never dies mid-escalation.
     * Catches {@link Throwable}, not just {@link RuntimeException} — an {@link Error} thrown from the
     * dump/interrupt path (e.g. {@code StackOverflowError} walking a pathological stack) must NOT
     * silently kill the escalation poller before it can {@code halt} a wedged JVM.
     */
    private void tickSafely() {
        try {
            tick();
        } catch (Throwable t) {
            log.warn("liveness_watchdog_tick_failed message={}", t.getMessage());
        }
    }

    /**
     * The pure escalation state machine (one poll). Forward progress resets the stall clock
     * <b>only while HEALTHY</b>; once tripping begins (the cooperative cancel is committed) the ladder
     * is <b>purely time-driven</b> and progress is ignored — so a cooperatively-unwinding run just
     * terminates (the daemon dies with the JVM) before {@code halt}, while a wedged unwind that only
     * dribbles a little progress can NOT postpone the halt backstop forever. The progress check
     * MUST be inside the {@code HEALTHY} arm, not before the switch: a post-trip progress tick would
     * otherwise short-circuit here and freeze the ladder, defeating the guaranteed halt.
     */
    void tick() {
        long now = clock.getAsLong();
        long progress = progressSupplier.getAsLong();
        long realProgress = realProgressSupplier.getAsLong();
        switch (state) {
            case HEALTHY -> {
                // Two INDEPENDENT clocks, each rearmed by its own signal advancing — but ONLY while
                // still HEALTHY. progressSignal (folds in throttle/retry activity) is the fast
                // total-freeze detector; realProgressSignal (committed work only) trips a run that
                // is active-but-failing (throttle events keep progressSignal climbing) yet commits
                // nothing. Both trackers are updated every HEALTHY tick, then we trip if EITHER window
                // has elapsed. (A rearm sets that clock to {@code now}, so its own trip check is a no-op
                // this tick — same skip-the-stall-check-on-fresh-progress behavior as before.)
                if (progress != lastProgress) {
                    lastProgress = progress;
                    lastProgressNanos = now;
                }
                if (realProgress != lastRealProgress) {
                    lastRealProgress = realProgress;
                    lastRealProgressNanos = now;
                }
                boolean totalFreeze = stallWindowNanos > 0 && now - lastProgressNanos >= stallWindowNanos;
                boolean noRealProgress = noProgressWindowNanos > 0
                        && now - lastRealProgressNanos >= noProgressWindowNanos;
                if (totalFreeze || noRealProgress) {
                    onStall(now, totalFreeze);
                }
            }
            case CANCELLED -> {
                if (now - escalatedNanos >= graceInterruptNanos) {
                    onInterruptAndDump();
                }
            }
            case INTERRUPTED -> {
                if (now - escalatedNanos >= graceInterruptNanos + graceHaltNanos) {
                    onHalt();
                }
            }
            case HALTED -> {
                // terminal — halt was requested; nothing more to do.
            }
        }
    }

    private void onStall(long now, boolean totalFreeze) {
        boolean weOwnReason = token.stopReason() == null;
        // Classify the proximate cause so the external classifier reads a distinct
        // error_class (stuck_api_timeouts / stuck_throttle / stuck_unknown) instead of a bare "stuck".
        String errorClass = errorClassSupplier.get();
        // Emit the stderr (logback -> System.err) marker an external runner's classifier maps to
        // error_class=stuck. Logged even when a prior reason (max_duration/signal) already won, so a
        // wedged UNWIND is still visible as a stuck escalation. Two SIBLING markers so a later unit can
        // map each trigger to a distinct error_class: list_stuck_abort = total signal freeze;
        // list_no_progress_abort = active-but-zero-real-progress (throttle/retry storm).
        if (totalFreeze) {
            long stalledMs = Duration.ofNanos(now - lastProgressNanos).toMillis();
            log.error("list_stuck_abort stop_reason=stuck error_class={} stall_ms={} exit_code={} "
                    + "owned_reason={} (no forward progress; escalating cooperative-cancel -> "
                    + "interrupt -> halt)",
                    errorClass, stalledMs, stuckExitCode, weOwnReason);
        } else {
            long noRealMs = Duration.ofNanos(now - lastRealProgressNanos).toMillis();
            log.error("list_no_progress_abort stop_reason=stuck error_class={} no_real_progress_ms={} "
                    + "exit_code={} owned_reason={} (active but ZERO real progress committed; escalating "
                    + "cooperative-cancel -> interrupt -> halt)",
                    errorClass, noRealMs, stuckExitCode, weOwnReason);
        }
        // first-writer-wins: a no-op if max_duration/signal already won. Attribute the source
        // so the terminal list_stuck_stop marker names the watchdog HONESTLY (vs. a per-fetch retry cap).
        token.cancel(StopReason.STUCK, CancelSource.LIVENESS_WATCHDOG);
        escalatedNanos = now;
        state = State.CANCELLED;
    }

    private void onInterruptAndDump() {
        log.error("list_stuck_escalate step=interrupt (cooperative cancel did not unwind within grace)");
        try {
            dumpAction.run();
        } catch (RuntimeException e) {
            log.warn("liveness_watchdog_dump_failed message={}", e.getMessage());
        }
        try {
            interruptAction.run();
        } catch (RuntimeException e) {
            log.warn("liveness_watchdog_interrupt_failed message={}", e.getMessage());
        }
        state = State.INTERRUPTED;
    }

    private void onHalt() {
        // Every watchdog TERMINAL marker carries the error_class fingerprint so an external
        // classifier can attribute the stuck disposition (stuck_api_timeouts / stuck_throttle /
        // stuck_unknown) — the abort markers already do; this halt rung must too. Guarded so a failing
        // classifier can never prevent the halt that guarantees process exit.
        String errorClass;
        try {
            errorClass = errorClassSupplier.get();
        } catch (Throwable t) {
            errorClass = "stuck_unknown";
        }
        // The halt path bypasses the cooperative finalizer's list_stuck_stop marker, so
        // its terminal markers carry stop_source themselves. On this path the source is DEFINITIONALLY the
        // watchdog (only the escalation ladder reaches halt), matching the cooperative path's format.
        log.error("list_stuck_halt stop_reason=stuck stop_source={} error_class={} exit_code={} (interrupt "
                + "did not unwind within grace; Runtime.halt)",
                CancelSource.LIVENESS_WATCHDOG.tag(), errorClass, stuckExitCode);
        // The hard Runtime.halt bypasses JsonRunSummaryWriter.close()/normal finalization, so in
        // the very case that most needs it (a fully-wedged worker) the terminal summary would NOT be
        // written. Durably record the stuck disposition on stderr (the marker an external runner's
        // classifier maps to error_class=stuck) IMMEDIATELY before halting — a minimal, machine-
        // parseable stuck record standing in for the summary the finalizer never gets to write.
        // Guarded (Throwable) so a failure here can never prevent the halt that guarantees process exit.
        try {
            log.error("list_stuck_summary stop_reason=stuck stop_source={} error_class={} completed=false "
                    + "exit_code={} (terminal stuck disposition recorded before Runtime.halt; the normal "
                    + "summary finalizer is bypassed by halt)",
                    CancelSource.LIVENESS_WATCHDOG.tag(), errorClass, stuckExitCode);
        } catch (Throwable t) {
            // deliberately ignored — the halt below is the guarantee, not this best-effort record.
        }
        state = State.HALTED;
        haltAction.accept(stuckExitCode);
    }

    /**
     * Whether a REAL poller is armed (at least one tripwire window enabled). When {@code
     * arm()} returned the disarmed no-op ({@code --stall-timeout 0} AND {@code --no-progress-timeout 0}),
     * this is {@code false} — nothing can ever end a ride-out, so the CLI must select
     * {@link io.varve.swath.engine.RetryPolicy#BOUNDED} for the retry loops.
     */
    public boolean isArmed() {
        return scheduler != null;
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    // ---- production default actions -------------------------------------------

    /**
     * Best-effort interrupt of the worker/carrier threads this class can see by name. This class is a
     * sibling of {@link DeadlineCanceller} and, like it, does not hold the worklist executor, so it
     * cannot target the pool directly (that seam is engine-owned).
     *
     * <p><b>Does NOT reliably wake a wedged virtual-thread socket read.</b>
     * {@link Thread#getAllStackTraces()} enumerates only platform threads, including a virtual thread's
     * carrier — interrupting the carrier interrupts the CARRIER, not the virtual thread mounted on it,
     * so a virtual thread parked in a native blocking read is unaffected. This step is genuinely useful
     * for a worker parked in platform-thread blocking I/O (the Parquet writer lanes, {@link
     * io.varve.swath.concurrent.Scope#ofPlatformThreads}); the {@link #onHalt() halt} step below is the
     * actual guarantee for a wedged virtual-thread carrier.
     *
     * <p>Matches the JDK's virtual-thread-scheduler {@code ForkJoinPool} carriers by their {@code
     * "ForkJoinPool-<n>-worker-"} name pattern (the hyphen right after {@code ForkJoinPool} — the
     * scheduler is a dedicated pool, distinct from {@code java.util.concurrent.ForkJoinPool
     * .commonPool()}, whose workers are named {@code "ForkJoinPool.commonPool-worker-"} with a period)
     * — so this deliberately does NOT sweep the JVM-wide common pool's unrelated tasks. Plus any named
     * {@code swath} worker pool; skips infrastructure daemons (this watchdog, the progress/summary/
     * max-duration schedulers) and its own thread.
     */
    private static void interruptWorkers() {
        Thread self = Thread.currentThread();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t == self || !t.isAlive()) {
                continue;
            }
            if (isInterruptibleWorker(t.getName())) {
                t.interrupt();
            }
        }
    }

    /**
     * Name predicate for the interrupt rung: TRUE for a worker/carrier lane this step can usefully
     * wake, FALSE for an infrastructure daemon (this watchdog, the progress/summary/max-duration
     * schedulers) or an unrelated thread. Extracted so the filter is unit-testable without
     * interrupting real threads.
     *
     * <p>The interrupt rung's one ACHIEVABLE purpose is waking a worker parked in
     * PLATFORM-thread blocking I/O — the {@code parquet-writer-*} ({@link
     * io.varve.swath.output.parquet.ParquetWriterPool}) and {@code *-encoder-*} ({@link
     * io.varve.swath.sort.SortLane}) lanes ({@link io.varve.swath.concurrent.Scope#ofPlatformThreads})
     * — since a virtual thread's carrier interrupt does not wake the mounted vthread's native read.
     * Matching {@code ForkJoinPool-} carriers alone would miss those platform
     * lanes and make the rung a no-op for its one job, so both are matched: the {@code ForkJoinPool-}
     * carrier match (harmless best-effort) plus the real lanes.
     */
    static boolean isInterruptibleWorker(String name) {
        if (name == null) {
            return false;
        }
        // "ForkJoinPool-" (hyphen) is the VT scheduler's dedicated pool; "ForkJoinPool.commonPool"
        // (period) is the JVM-wide common pool — deliberately excluded: it must not sweep
        // unrelated common-pool tasks elsewhere in the process. "parquet-writer"/"-encoder" are the
        // real platform-thread blocking-I/O lanes this rung exists to wake.
        boolean worker = name.startsWith("ForkJoinPool-")
                || name.contains("parquet-writer")
                || name.contains("-encoder");
        boolean infra = name.equals("swath-liveness-watchdog") || name.equals("swath-max-duration")
                || name.equals("swath-progress") || name.equals("swath-summary-json");
        return worker && !infra;
    }

    /** Forensic snapshot to the log (System.err via logback): the pre-halt evidence. */
    private static void dumpForensics(RunMetrics metrics) {
        Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
        log.error("list_stuck_forensics in_flight={} peak_in_flight={} objects_emitted={} "
                + "progress_signal={} live_threads={} (virtual-thread stacks are best-effort; the SDK "
                + "connection pool exposes no stats without reflection, deliberately omitted)",
                metrics.currentInFlight(), metrics.peakInFlight(), metrics.objectsEmitted(),
                metrics.progressSignal(), stacks.size());
        for (Map.Entry<Thread, StackTraceElement[]> e : stacks.entrySet()) {
            Thread t = e.getKey();
            StackTraceElement[] frames = e.getValue();
            String top = frames.length > 0 ? frames[0].toString() : "<no frames>";
            log.error("list_stuck_thread name={} state={} daemon={} top_frame={}",
                    t.getName(), t.getState(), t.isDaemon(), top);
        }
    }

    // ---- test observation seams -----------------------------------------------

    /** The current escalation state (test-only observation). */
    State state() {
        return state;
    }
}
