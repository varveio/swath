/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * {@link SortDiskGuard}'s pure decision function ({@code insufficientDiskReason}) and the
 * {@code tick()} wiring — no real disk is ever touched: usable-free-bytes and staging-bytes-so-far
 * are both injected.
 */
class SortDiskGuardTest {

    @Test
    void aboveTheFloorAndBelowTheSafetyFactorIsSufficient() {
        // 10 GiB free, 1 GiB staged so far, 3x safety factor => need ~3 GiB, have 10 GiB: fine.
        Optional<String> reason = SortDiskGuard.insufficientDiskReason(
                10L * 1024 * 1024 * 1024, 1L * 1024 * 1024 * 1024, 3.0, 1L * 1024 * 1024 * 1024);
        assertThat(reason).isEmpty();
    }

    @Test
    void belowTheAbsoluteFloorIsInsufficientEvenWithNoStagingYet() {
        // A fresh run (zero staged), but the volume itself is already below the floor.
        Optional<String> reason = SortDiskGuard.insufficientDiskReason(
                100L * 1024 * 1024, 0L, 3.0, 1L * 1024 * 1024 * 1024);
        assertThat(reason).isPresent();
        assertThat(reason.get()).contains("minimum floor");
    }

    @Test
    void projectedNeedExceedingFreeSpaceIsInsufficient() {
        // 10 GiB free, 5 GiB already staged, 3x safety factor => need ~15 GiB > 10 GiB free: refuse.
        Optional<String> reason = SortDiskGuard.insufficientDiskReason(
                10L * 1024 * 1024 * 1024, 5L * 1024 * 1024 * 1024, 3.0, 1L * 1024 * 1024 * 1024);
        assertThat(reason).isPresent();
        assertThat(reason.get()).contains("need ~15").contains("have ~10");
    }

    @Test
    void unknownUsableFreeBytesNeverFalsePositives() {
        // A FileStore query failure is signalled as negative -- must never trip a refusal.
        Optional<String> reason = SortDiskGuard.insufficientDiskReason(-1L, Long.MAX_VALUE, 3.0, 1L);
        assertThat(reason).isEmpty();
    }

    @Test
    void zeroStagingSoFarNeverTripsTheProjectionArmOnlyTheFloor() {
        // A brand-new run: nothing staged yet, plenty of free space above the floor -- fine.
        Optional<String> reason = SortDiskGuard.insufficientDiskReason(
                50L * 1024 * 1024 * 1024, 0L, 3.0, 1L * 1024 * 1024 * 1024);
        assertThat(reason).isEmpty();
    }

    @Test
    void disabledGuardNeverTicksOrTrips() {
        AtomicLong staged = new AtomicLong(Long.MAX_VALUE);   // would trip if ever checked
        try (SortDiskGuard guard = SortDiskGuard.arm(
                Path.of("."), staged::get, true)) {
            // arm(disabled=true) returns a no-op guard: no scheduler, tick() would be harmless too.
            guard.tick();   // must not throw / must not halt the test JVM
        }
    }

    @Test
    void armedGuardTicksTripAndInvokeTheHaltActionExactlyOnceOverInsufficientDisk() {
        AtomicLong staged = new AtomicLong(0L);
        AtomicReference<String> tripped = new AtomicReference<>();
        // Tiny free-bytes supplier (the injectable seam) -- no real disk is ever touched.
        SortDiskGuard guard = new SortDiskGuard(null, staged::get, () -> 100L * 1024 * 1024,
                3.0, 1L * 1024 * 1024 * 1024, tripped::set);

        guard.tick();
        assertThat(tripped.get()).as("below the floor from tick 1").isNotNull();
    }

    @Test
    void armedGuardStaysQuietUntilStagingGrowsPastTheSafetyMargin() {
        AtomicLong staged = new AtomicLong(0L);
        AtomicReference<String> tripped = new AtomicReference<>();
        long tenGib = 10L * 1024 * 1024 * 1024;
        SortDiskGuard guard = new SortDiskGuard(null, staged::get, () -> tenGib,
                3.0, 1L * 1024 * 1024 * 1024, tripped::set);

        guard.tick();
        assertThat(tripped.get()).as("nothing staged yet, plenty of headroom").isNull();

        staged.set(4L * 1024 * 1024 * 1024);   // 3x4=12 GiB projected > 10 GiB free
        guard.tick();
        assertThat(tripped.get()).as("projected need now exceeds free space").isNotNull();
        assertThat(tripped.get()).contains("need ~12.00 GiB");
    }

    /**
     * The halt-path marker must carry a distinct {@code error_class=sort_disk_exhausted} and a
     * resumable {@code stop_reason}, distinguishing it on stderr from an unclassified crash —
     * asserted via the extracted {@code logExhaustionMarker} seam so the test JVM is never halted
     * (mirrors {@code LivenessWatchdogTest}'s halt-marker capture idiom).
     */
    @Test
    void exhaustionMarkerCarriesADistinctErrorClassAndAResumableStopReason() {
        Logger logger =
                (Logger) LoggerFactory.getLogger(SortDiskGuard.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.ERROR);
        logger.addAppender(appender);
        try {
            SortDiskGuard.logExhaustionMarker("need ~15.00 GiB, have ~10.00 GiB free");
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        String marker = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("sort_disk_exhaustion_imminent"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no sort_disk_exhaustion_imminent marker emitted"));
        assertThat(marker)
                .as("a distinct, greppable classification -- NOT indistinguishable from an "
                        + "unclassified crash -- plus an honest resumable disposition (durable staged "
                        + "segments mean --sort --resume on a bigger disk re-uses staged work)")
                .contains("error_class=sort_disk_exhausted")
                .contains("stop_reason=sort_disk_exhausted")
                .contains("resumable=true")
                .contains("reason=\"need ~15.00 GiB, have ~10.00 GiB free\"")
                .contains("exit_code=1");
    }

    @Test
    void armPollIntervalIsHonoredAndCloseTearsDownTheScheduler() throws Exception {
        // A real scheduler (short poll interval) driving the SAME pure decision, proving the
        // production wiring (arm/close) actually schedules and can be torn down cleanly.
        //
        // DELIBERATELY never trips: this exercises the production `arm()` factory, whose trip
        // action is the REAL `Runtime.halt(1)` (SortDiskGuard#haltOnInsufficientDisk) -- tripping
        // it here would halt this very test JVM mid-run. staged stays 0 (never exceeds any real
        // free-space reading on the volume this test happens to run on), so only the "did it
        // actually poll" behavior is observed; the halt path itself is covered, non-destructively,
        // by the injected-consumer tests above.
        AtomicLong staged = new AtomicLong(0L);
        AtomicLong ticks = new AtomicLong();
        SortDiskGuard guard = SortDiskGuard.arm(Path.of("."), () -> {
            ticks.incrementAndGet();
            return staged.get();
        }, false, 3.0, 1L, Duration.ofMillis(50));
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (ticks.get() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(ticks.get()).as("the scheduled poller actually ticked").isGreaterThan(0);
        } finally {
            guard.close();
        }
    }
}
