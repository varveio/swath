/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fail-fast disk guard for {@code --sort} staging: a large {@code --sort} listing
 * accumulates sorted staging segments throughout the (multi-hour) listing phase, then the final
 * k-way merge reads all staging AND writes the merged output concurrently, so peak disk on the
 * staging volume is empirically {@code ~2x} the final compressed output. Without a check, a
 * badly-undersized volume runs for hours before dying at the merge.
 *
 * <p><b>Two complementary checks, both built on the same pure {@link #insufficientDiskReason}
 * decision:</b>
 *
 * <ol>
 *   <li><b>Startup pre-check</b> ({@code ListCommand}, before the guarded engine dispatch): a
 *       one-shot call using whatever is ALREADY on disk (real for a {@code --resume} of a run that
 *       previously staged real segments; zero for a brand-new run). Refuses cleanly via a normal
 *       {@code OutputException} — deliberately BEFORE {@code runEngineGuarded}'s scope, so a refusal
 *       here is never mistaken for the fatal-in-process-error case that would permanently poison
 *       {@code --resume}: freeing disk (or passing the escape hatch) lets a later invocation proceed
 *       normally.</li>
 *   <li><b>Periodic runtime guard</b> ({@link #arm}, run from inside the listing phase): the same
 *       decision, polled against the LIVE {@code swath.sort.segment.bytes} counter (real bytes
 *       already flushed to staging THIS run) — the mechanism that actually catches a brand-new,
 *       never-before-attempted huge bucket early, once its true disk trajectory becomes observable
 *       (typically within minutes, not the hours it would otherwise take to reach the merge). This
 *       one does NOT throw a Java exception (which {@code runEngineGuarded} would fatal-flag the
 *       SAME way): it logs a clear diagnostic and calls {@link Runtime#halt(int)} directly, relying
 *       on the existing crash-only guarantee (proved by the real-{@code kill -9}
 *       {@code HardCrashSigkillResumeProcessIT}) that ANY abrupt process death — this one included —
 *       leaves the checkpoint durable and {@code --sort}'s own staging resumable.</li>
 * </ol>
 *
 * <p><b>Escape hatch:</b> {@code --force-sort} skips both checks entirely (a user who has already
 * sized the volume, or wants to try anyway, is never blocked).
 *
 * <p><b>Sizing guidance</b> (docs/usage.md): {@code peak_disk ≈ 2 × objects × bytes_per_object}.
 * {@link #DEFAULT_SAFETY_FACTOR} (3x, not the bare 2x the formula implies) leaves headroom for
 * segment/row-group overhead variance and the fact that the projection here is a LOWER bound
 * (staging can only grow further from whatever has been observed so far).
 */
public final class SortDiskGuard implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SortDiskGuard.class);

    /** Conservative multiplier over the issue's own {@code ~2x} peak-disk observation (headroom). */
    public static final double DEFAULT_SAFETY_FACTOR = 3.0;
    /** Absolute sanity floor: refuse regardless of staging size once free space is this low. */
    public static final long DEFAULT_MIN_FREE_BYTES = 1L * 1024 * 1024 * 1024;
    /** How often the runtime guard re-samples staging bytes vs. usable free space. */
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(30);

    private final ScheduledExecutorService scheduler;   // null when disarmed (--force-sort)
    private final LongSupplier stagingBytesSupplier;
    private final LongSupplier usableFreeBytesSupplier;
    private final double safetyFactor;
    private final long minFreeBytesFloor;
    private final Consumer<String> onInsufficient;

    SortDiskGuard(ScheduledExecutorService scheduler, LongSupplier stagingBytesSupplier,
                  LongSupplier usableFreeBytesSupplier, double safetyFactor, long minFreeBytesFloor,
                  Consumer<String> onInsufficient) {
        this.scheduler = scheduler;
        this.stagingBytesSupplier = stagingBytesSupplier;
        this.usableFreeBytesSupplier = usableFreeBytesSupplier;
        this.safetyFactor = safetyFactor;
        this.minFreeBytesFloor = minFreeBytesFloor;
        this.onInsufficient = onInsufficient;
    }

    /**
     * The pure decision (no I/O): {@code Optional.empty()} when the volume looks adequate,
     * otherwise a human-readable "need ~X, have ~Y" reason. {@code usableFreeBytes < 0} (a
     * {@code FileStore} query failure) NEVER trips a refusal — an unknown quantity must not false-
     * positive a fail-fast that only exists to save wasted compute, not to add a new failure mode.
     */
    public static Optional<String> insufficientDiskReason(long usableFreeBytes, long stagingBytesSoFar,
                                                            double safetyFactor, long minFreeBytesFloor) {
        if (usableFreeBytes < 0) {
            return Optional.empty();
        }
        if (usableFreeBytes < minFreeBytesFloor) {
            return Optional.of(String.format(Locale.ROOT,
                    "usable free space (~%.2f GiB) is below the minimum floor (~%.2f GiB) for --sort staging",
                    gib(usableFreeBytes), gib(minFreeBytesFloor)));
        }
        if (stagingBytesSoFar > 0) {
            double projectedNeed = safetyFactor * (double) stagingBytesSoFar;
            if (projectedNeed > (double) usableFreeBytes) {
                return Optional.of(String.format(Locale.ROOT,
                        "need ~%.2f GiB (%.1fx safety margin over ~%.2f GiB already staged/observed), "
                                + "have ~%.2f GiB free",
                        gib((long) projectedNeed), safetyFactor, gib(stagingBytesSoFar), gib(usableFreeBytes)));
            }
        }
        return Optional.empty();
    }

    /**
     * Arm the periodic runtime guard, or a disarmed no-op when {@code disabled} (the
     * {@code --force-sort} escape hatch). Polls {@code stagingBytesWrittenSoFar} (the live
     * {@code swath.sort.segment.bytes} counter, production) against usable free space on {@code
     * stagingVolumeDir}'s filesystem; on trip, logs and calls {@link Runtime#halt(int)} — see the
     * class javadoc for why a halt, not a cooperative cancel, is the right (and safe) response here.
     * The poller runs on a daemon thread; {@link #close()} tears it down (try-with-resources).
     */
    public static SortDiskGuard arm(Path stagingVolumeDir, LongSupplier stagingBytesWrittenSoFar,
                                     boolean disabled) {
        return arm(stagingVolumeDir, stagingBytesWrittenSoFar, disabled,
                DEFAULT_SAFETY_FACTOR, DEFAULT_MIN_FREE_BYTES, DEFAULT_POLL_INTERVAL);
    }

    static SortDiskGuard arm(Path stagingVolumeDir, LongSupplier stagingBytesWrittenSoFar, boolean disabled,
                              double safetyFactor, long minFreeBytesFloor, Duration pollInterval) {
        if (disabled) {
            return new SortDiskGuard(null, stagingBytesWrittenSoFar, () -> -1L,
                    safetyFactor, minFreeBytesFloor, reason -> { });
        }
        LongSupplier usableFreeBytesSupplier = () -> usableSpaceOrMinusOne(stagingVolumeDir);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r ->
                Thread.ofPlatform().daemon(true).name("swath-sort-disk-guard").unstarted(r));
        SortDiskGuard guard = new SortDiskGuard(scheduler, stagingBytesWrittenSoFar, usableFreeBytesSupplier,
                safetyFactor, minFreeBytesFloor, SortDiskGuard::haltOnInsufficientDisk);
        long pollMs = Math.max(1_000L, pollInterval.toMillis());
        scheduler.scheduleAtFixedRate(guard::tickSafely, pollMs, pollMs, TimeUnit.MILLISECONDS);
        return guard;
    }

    /** Swallows any error so the daemon poller never dies mid-run over an unrelated failure. */
    private void tickSafely() {
        try {
            tick();
        } catch (Throwable t) {
            log.warn("sort_disk_guard_tick_failed message={}", t.getMessage());
        }
    }

    void tick() {
        long free = usableFreeBytesSupplier.getAsLong();
        long staged = stagingBytesSupplier.getAsLong();
        insufficientDiskReason(free, staged, safetyFactor, minFreeBytesFloor).ifPresent(onInsufficient);
    }

    /**
     * Production trip action: log the clear "need ~X, have ~Y" diagnostic, carrying the
     * {@code error_class}/{@code stop_reason} classification below, then {@link
     * Runtime#halt(int)} immediately — bypassing {@code runEngineGuarded}'s catch (and its
     * fatal-error flag) entirely, since a halt never unwinds through Java exception handling.
     * Exit code 1 matches {@code OutputException}'s documented disk-full disposition.
     */
    private static void haltOnInsufficientDisk(String reason) {
        logExhaustionMarker(reason);
        Runtime.getRuntime().halt(1);
    }

    /**
     * Builds and logs the terminal exhaustion marker — split out from {@link
     * #haltOnInsufficientDisk} so a test can assert its content directly, WITHOUT invoking
     * {@link Runtime#halt(int)} (which would kill the test JVM). Mirrors {@code
     * LivenessWatchdog}'s pre-halt marker pattern: logged BEFORE the halt, since halt bypasses the
     * normal summary finalizer that would otherwise carry a classification. Carries a distinct
     * {@code error_class=sort_disk_exhausted} — distinguishable from an unclassified crash to an
     * external supervisor — and {@code stop_reason=sort_disk_exhausted} marked
     * {@code resumable=true}: this disposition IS resumable — durable staged segments mean
     * {@code --sort --resume} on a bigger volume re-uses the already-staged work, unlike a
     * genuine unrecoverable crash.
     */
    static void logExhaustionMarker(String reason) {
        log.error("sort_disk_exhaustion_imminent reason=\"{}\" error_class=sort_disk_exhausted "
                + "stop_reason=sort_disk_exhausted resumable=true exit_code=1 (aborting NOW via "
                + "Runtime.halt so the wasted-compute cost is minutes, not hours; a later --resume "
                + "— after freeing disk or increasing the volume — or --force-sort can continue "
                + "safely: the checkpoint and sort staging are durable under the crash-only recovery "
                + "contract)", reason);
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private static long usableSpaceOrMinusOne(Path dir) {
        try {
            return Files.getFileStore(dir).getUsableSpace();
        } catch (IOException | RuntimeException e) {
            return -1L;
        }
    }

    private static double gib(long bytes) {
        return bytes / (1024.0 * 1024.0 * 1024.0);
    }
}
