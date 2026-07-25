/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.LatencyModels;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * <b>The latency-model fixture reproduces the production dense-shape collapse signature offline,
 * where a zero-latency harness cannot.</b>
 *
 * <p>Reproduces §2/§4 (the split-probe timeout -> shed -> demand-gate spiral over a collapsed Hive
 * seed): a truncated {@code date=} partition fan-out that, with {@code fanout_tiling} OFF, collapses
 * the seed to a couple of near-serial ranges. The tail then drains near-serially while structure/pivot
 * probes into COLD keyspace are slow (they let the owner's fast warm cursor race past the chosen pivot
 * — {@code RETRY.cursor_passed_pivot}) and, when they exceed the scaled attempt-timeout budget, time
 * out ({@code TRANSIENT.attempt_timeout_probe}); a cold-start timeout storm fires the sustained-timeout
 * {@code SHED/timeout_storm}, which then collapses {@code T} and suppresses further stealing (the §4
 * spiral). All of this over the {@code denseColdProbes} latency model composed with a
 * {@code PageInterceptor} that turns an over-threshold cold read into an {@code ATTEMPT_TIMEOUT} fault
 * (the documented composition contract: profiles produce Durations; faults stay the interceptor's job,
 * the {@code GaugedFetcherAttemptTimeoutEscalationTest} shape).
 *
 * <p>The comparison ({@link #off}/{@link #control1}/{@link #control2}) isolates what latency reveals:
 * <ul>
 *   <li><b>OFF + latency</b> — the collapse: low avg-in-flight / high serial_frac — and byte-exact
 *       correct output (the collapse is a PERF failure, not a correctness one).</li>
 *   <li><b>CONTROL 1 — OFF, zero latency</b> — the blind spot: the SAME keyspace/toggle run with a
 *       zero-latency, fault-free harness produces ZERO probe timeouts and ZERO sheds. The whole
 *       spiral is invisible offline without a latency fixture. (Note: {@code cursor_passed_pivot}
 *       is NOT a clean latency discriminator — a collapsed seed makes 48 thieves contend over ~2 ranges,
 *       and the instant owner drains fast, so scheduling races occur even at zero latency; the true
 *       latency-only signals are the probe timeouts and the shed. Since the strict steal bound it is
 *       INVERTED — measured 10 at zero latency against 1 with latency, because instant probes recycle
 *       the one attempt slot while a 90-160ms cold probe holds it — so this arm no longer asserts on
 *       it at all.)</li>
 *   <li><b>CONTROL 2 — ON + latency</b> — {@code fanout_tiling} ON ties the story together: the SAME
 *       hostile latency largely defuses the collapse (seed tiles pre-empt the probe race), dropping
 *       serial_frac and raising avg-in-flight materially.</li>
 * </ul>
 * <p><b>What the strict steal bound changed here.</b> This fixture was written against an engine
 * whose speculative steal probes ran concurrently fleet-wide, so probe-timeout pressure scaled with
 * the number of idle thieves and the OFF arm produced dozens of {@code attempt_timeout_probe} faults.
 * Making that bound strict (one attempt in flight fleet-wide) serializes the probes, and a
 * non-productive attempt then paces the next one out to the backoff cap — so this 1.7s collapsed tail
 * now issues exactly ONE probe, deterministically (1/1 across 8 runs). The volume thresholds this arm
 * used to assert (>15 timeouts, >5 cursor races) were therefore measuring the OLD steal policy, not
 * the collapse: they cannot be restored by re-tuning the fixture, because the pressure they counted is
 * precisely what the bound removed.
 *
 * <p>Nor can a weaker "&gt;= 1 probe timeout" stand in: whether the tail issues ANY probe before it
 * drains is machine-dependent — 1 on an 8-core dev box (8/8 runs), 0 on the 4-core CI runner. So this
 * class no longer asserts on probe timeouts in either arm. What it asserts is what is deterministic:
 * the collapse's structural signature (a ~2-range seed against 50 tiled, serial_frac at 1.0,
 * avg-in-flight far below W, byte-exact output), and the latency-only discriminator in the SHED
 * dimension — {@link #offStorm} fires a sustained-timeout shed that {@link #control1} never can,
 * driven by the workers' own cold first-page reads, which need no steal slot and so are untouched by
 * the bound.
 *
 * <p>The sustained-timeout SHED is proven in a dedicated cold-start-storm arm ({@link #offStorm}), because
 * it cannot coexist with the sustained-probe signature in ONE bounded run — see {@link
 * #STARTUP_TIMEOUT_BURST} and {@link #offStorm_coldStartStormFiresTheShedSpiral} for why (a harness
 * limitation).
 *
 * <p><b>Timing</b>: real ms-scale interruptible sleeps + the attempt-timeout modeled by the
 * interceptor throwing + a BOUNDED no-op-sleeper {@link RetryConfig} so the storm is fast. Deep tier
 * (latency-injecting): excluded from the fast per-commit suite, run with {@code -Pdeep}. Deterministic
 * (fixed seeds); asserts on robust scaled thresholds, never exact counts, so the class is CI-stable.
 */
@Tag("deep")
final class DenseShapeCollapseSignatureTest {

    // >1000 so the top `table/date=` level's first delimiter=/ page is TRUNCATED-with-CommonPrefixes
    // (1000 shown, more exist) — the exact shape fanout_tiling ON tiles and OFF collapses.
    private static final int PARTITIONS = 1100;      // ~1100 `date=` partitions (a truncated Hive-partition fan-out)
    private static final int PER_PARTITION = 120;    // objects per partition -> ~132k keys, ~132 pages
    private static final int WORKERS = 48;           // W in [32,64]
    private static final int MAX_KEYS = 1000;

    // Latency profile (ms-scale): warm sequential pages fast; cold/far jumps (probes) slow and over the
    // scaled probe-timeout threshold, so a composed fault reproduces the timeout spiral. Cold ~100x a
    // warm page (the production ~100:1 probe:page ratio, scaled): slow enough that a fast warm-cursor
    // owner races past even a far-ahead pivot during one cold probe.
    private static final long LAT_SEED = 0x5EED_C01DL;
    private static final Duration WARM_MIN = Duration.ofMillis(1);
    private static final Duration WARM_MAX = Duration.ofMillis(1);
    private static final Duration PROBE_TIMEOUT_THRESHOLD = Duration.ofMillis(40);
    private static final Duration COLD_MIN = Duration.ofMillis(90);
    private static final Duration COLD_MAX = Duration.ofMillis(160);

    // The leading cold-start timeout burst that reliably fires the sustained-timeout SHED offline (used
    // ONLY by the STORM arm). Rationale — and the harness limitation this documents: the shed's window is
    // a real 25-40s wall-clock window (NOT injectable through WorkStealingScan / EngineHarness — the
    // ConcurrencyGauge clock seam is package-private and unthreaded), so the whole fast run fits inside
    // ONE window and the shed's starvation gate (worker successes <= floor(T/32)) is only satisfiable at
    // t=0, before the first worker page commits. A thief PROBE cannot fire until its victim has committed
    // a page (victim selection needs est > 0), so the ONLY calls in the pre-success window are the
    // workers' OWN cold first-page reads into the entirely-unwarmed keyspace. So — exactly as the accepted
    // prior-art shed guard ({@code TransientTimeoutRetryEngineContractTest#attemptTimeoutStarvedBurst...}) does
    // — the first few COLD reads time out (a cold-start storm), landing >=3 timeouts with 0 coexisting
    // worker successes: the starved-burst gate. The burst is kept SMALL so each of the ~2 seed-range
    // workers faults only a few times (under the transient-retry cap) and then serves (byte-exact still
    // holds). A fired shed sets stealingAllowed=false for the (also un-scalable) 10s clean window — longer
    // than this whole run — so stealing never resumes and the sustained-probe signature vanishes; that is
    // why the shed CANNOT coexist with the sustained OFF arm and gets its own STORM arm. This is the
    // achievable proxy for production's STEADY-STATE probe-timeout shed (which needs a scaled window this
    // harness cannot provide).
    private static final int STARTUP_TIMEOUT_BURST = 6;

    // Pin massAwareSeed=false explicitly (12th arg) alongside fanoutTiling=false (10th arg): the
    // shorter constructor defaults massAwareSeed=true, which would let mass-aware sampling BAND
    // this fixture's heavy `date=` partition cut (120 objs/partition, easily sample-proven heavy)
    // instead of leaving the collapsed-seed shape this experiment needs — the `off`/`control1`/
    // `offStorm` arms specifically reproduce the collapse to a couple of near-serial ranges, and
    // banding it would invalidate the whole premise.
    private static final EngineToggles FANOUT_OFF =
            EngineToggles.DEFAULT.withFanoutTiling(false)
                    .withMassAwareSeed(false);

    /** How a run's fetcher injects latency + timeout faults (the fixture mode under test). */
    private enum Mode {
        /** Zero-latency harness: no faults — the CONTROL that cannot see the spiral. */
        ZERO_LATENCY,
        /** dense-cold-probe latency + sparse sustained cold-probe timeouts (the collapse tail, no shed). */
        SUSTAINED,
        /** SUSTAINED + a leading cold-start timeout burst that fires the shed (the storm spiral). */
        STORM
    }

    // Scenarios computed ONCE per class execution (BeforeAll); the 3x stability check is the class re-run
    // externally. One set of runs keeps class wall-time modest.
    private static Arm off;        // fanout_tiling OFF + dense-cold-probe latency (the collapse)
    private static Arm control1;   // fanout_tiling OFF + zero latency, fault-free (the zero-latency control)
    private static Arm control2;   // fanout_tiling ON  + dense-cold-probe latency (the fix, defused)
    private static Arm offStorm;   // fanout_tiling OFF + latency + cold-start storm (fires the shed)

    @BeforeAll
    static void runArms() throws Exception {
        Path base = Files.createTempDirectory("dense-shape-collapse");
        off = runArm("off", FANOUT_OFF, Mode.SUSTAINED, base.resolve("off"));
        control1 = runArm("control1", FANOUT_OFF, Mode.ZERO_LATENCY, base.resolve("control1"));
        control2 = runArm("control2", EngineToggles.DEFAULT, Mode.SUSTAINED, base.resolve("control2"));
        offStorm = runArm("offStorm", FANOUT_OFF, Mode.STORM, base.resolve("offStorm"));
    }

    // ---- OFF + latency reproduces the collapse signature (fan-out + cursor race + probe timeouts) ----

    @Test
    @Timeout(120)
    void offArm_reproducesTheProductionCollapseSignature() {
        // Correctness FIRST: the collapse is a PERF failure, not a correctness one — every key is still
        // emitted exactly once despite the near-serial drain and the probe-timeout pressure.
        EngineHarness.assertExactlyOnce(off.emitted, keyspace());

        // Low sustained concurrency / high serial fraction (the collapsed near-serial tail).
        assertThat(off.serialFrac)
                .as("OFF collapses to a near-serial tail: most wall-time at <= serial-threshold in-flight")
                .isGreaterThan(0.5);
        assertThat(off.avgInFlight)
                .as("OFF sustains only a low average in-flight (a couple of drainers, not W)")
                .isLessThan(WORKERS / 4.0);

        // NO probe-timeout assertion -- see the class javadoc. Under the fleet-wide
        // one-attempt-in-flight bound, whether this collapsed tail issues ANY steal probe before it
        // drains is machine-dependent: 1 every time on an 8-core dev box, 0 on the 4-core CI runner.
        // The latency-only discriminator the class exists to prove now lives in the shed dimension
        // (offStorm vs control1), which fires by construction rather than by timing.
    }

    // ---- CONTROL 1: the zero-latency harness is blind to the spiral ---------------------------------

    @Test
    @Timeout(120)
    void control1_zeroLatencyHarnessCannotSeeTheSpiral() {
        EngineHarness.assertExactlyOnce(control1.emitted, keyspace());

        // The SAME collapsed seed, run the zero-latency way (instant + fault-free), shows NONE of the
        // spiral: without wall-clock latency no cold read ever exceeds the budget, so there are no
        // probe timeouts and no sustained-timeout shed. This is the fixture's whole reason to exist.
        assertThat(control1.probeTimeouts)
                .as("zero latency: no cold probe ever exceeds the budget -> zero probe timeouts")
                .isZero();
        assertThat(control1.sheds)
                .as("zero latency: no timeout storm -> the sustained-timeout shed never fires")
                .isZero();

        // The blind spot as a delta, in the one dimension that survives the strict steal bound: the
        // SHED. The probe-timeout delta is gone -- the OFF arm's probe count is machine-dependent
        // (1 here, 0 on the 4-core CI runner), so it can no longer carry this claim. The storm arm's
        // shed does, and by construction: its leading cold-start burst is the workers' OWN first-page
        // reads, which need no steal slot and so are untouched by the bound.
        assertThat(offStorm.sheds)
                .as("the latency fixture reveals shed storms invisible to the zero-latency harness")
                .isGreaterThan(control1.sheds);
    }

    // ---- CONTROL 2: the fanout_tiling fix defuses the collapse under the same hostile latency -------

    @Test
    @Timeout(120)
    void control2_fanoutTilingDefusesCollapseUnderHostileLatency() {
        EngineHarness.assertExactlyOnce(control2.emitted, keyspace());

        // Same dense-cold-probe latency, but fanout_tiling ON: the seed tiles pre-empt the probe race, so
        // the collapse is materially defused — serial_frac drops sharply and avg-in-flight rises sharply.
        assertThat(control2.seedCount)
                .as("ON tiles the truncated date= fan-out into ~W ranges (OFF collapsed to ~2)")
                .isGreaterThan(off.seedCount * 4);
        assertThat(control2.serialFrac)
                .as("ON materially lowers serial_frac vs OFF under the same hostile latency (%.3f vs %.3f)",
                        control2.serialFrac, off.serialFrac)
                .isLessThan(off.serialFrac * 0.6);
        assertThat(control2.avgInFlight)
                .as("ON sustains materially higher avg-in-flight than OFF (%.2f vs %.2f)",
                        control2.avgInFlight, off.avgInFlight)
                .isGreaterThan(off.avgInFlight * 2.0);
    }

    // ---- OFF cold-start storm: the timeout storm fires the shed and the §4 suppression spiral --------

    @Test
    @Timeout(120)
    void offStorm_coldStartStormFiresTheShedSpiral() {
        // Even through the full storm -> shed -> stealing-suppression spiral, output stays byte-exact.
        EngineHarness.assertExactlyOnce(offStorm.emitted, keyspace());

        // The sustained-timeout SHED fired: the cold-start timeout storm (workers' first cold reads timing
        // out with no coexisting progress) satisfies the shed's timeout+starvation gate.
        assertThat(offStorm.sheds)
                .as("the cold-start timeout storm fired the sustained-timeout shed (SHED/timeout_storm)")
                .isGreaterThanOrEqualTo(1L);
        // ... and the shed collapsed T far below W (the §4 spiral: shed -> shrink T -> suppress stealing).
        assertThat(offStorm.effectiveT)
                .as("the shed collapsed the concurrency target far below W (%d)", offStorm.effectiveT)
                .isLessThan(WORKERS / 4);
    }

    // ---- one engine run ------------------------------------------------------------------------------

    private record Arm(String name, List<byte[]> emitted, int seedCount, double avgInFlight,
                       long peakInFlight, double serialFrac, long cursorPassedPivot, long probeTimeouts,
                       long sheds, int effectiveT) {
    }

    /**
     * Seed via {@code SHALLOW} under {@code toggles} on a plain (instant) fetcher, then drive the full
     * {@link WorkStealingScan} over a fetcher whose fixture {@code mode} sets the latency + timeout-fault
     * regime. A BOUNDED no-op-sleeper {@link RetryConfig} keeps the retry storm fast.
     */
    private static Arm runArm(String name, EngineToggles toggles, Mode mode, Path dir) throws Exception {
        Files.createDirectories(dir);
        List<byte[]> keyspace = keyspace();

        // Seeding runs BEFORE the engine on its OWN plain fetcher (deterministic, instant), so the cold-
        // read timeout faults never touch the seed's structure probes — only the engine's fetches.
        MockPageFetcher seedFetcher = MockPageFetcher.builder().keys(keyspace).build();
        MockPageFetcher.Builder engineBuilder = MockPageFetcher.builder().keys(keyspace);
        if (mode != Mode.ZERO_LATENCY) {
            engineBuilder
                    .latencyModel(LatencyModels.denseColdProbes(
                            LAT_SEED, WARM_MIN, WARM_MAX, PROBE_TIMEOUT_THRESHOLD, COLD_MIN, COLD_MAX))
                    .interceptor(coldReadTimeoutInterceptor(mode));
        }
        MockPageFetcher engineFetcher = engineBuilder.build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        int seedCount;
        ConcurrencyGauge gauge;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(name), false, false);
            List<NodeSpec> specs = SeedSteps.of(seedFetcher, new byte[0], WORKERS, null, toggles)
                    .seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            seedCount = specs.size();
            List<Node> seeds = store.loadResumable(run.id(), false);

            metrics.markRunStarted();   // arm the avg-in-flight / trajectory integration window
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles).withRetryConfig(new RetryConfig(RetryPolicy.BOUNDED, ms -> { })),
                    engineFetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);
            gauge = engine.gauge();

            PipelineDrain.collectKeys(5000, engine, emitted);
        }

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        RunSummary.TrajectorySummary traj =
                metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).trajectory();
        double serialFrac = traj != null ? traj.serialFrac() : -1.0;
        return new Arm(name, emitted, seedCount, metrics.avgInFlight(), metrics.peakInFlight(), serialFrac,
                reasons.getOrDefault("RETRY.cursor_passed_pivot", 0L),
                reasons.getOrDefault("TRANSIENT.attempt_timeout_probe", 0L),
                reasons.getOrDefault("SHED.timeout_storm", 0L),
                gauge.effectiveT());
    }

    /**
     * The documented composition: the latency model produces Durations; the FAULT stays
     * the interceptor's job. An over-threshold cold read surfaces — after the SDK's own retries — as a
     * thrown {@code ThrottleException(ATTEMPT_TIMEOUT)}, exactly as {@code
     * GaugedFetcherAttemptTimeoutEscalationTest} models it.
     *
     * <ul>
     *   <li><b>Sustained tail</b> (both modes): ~1-in-8 cold split-PROBE attempts ({@code maxKeys <= 1})
     *       time out ({@code attempt_timeout_probe}), while the other ~7-in-8 cold probes SLOW-succeed
     *       so the owner's fast warm cursor races past the far-ahead pivot during the slow probe
     *       ({@code cursor_passed_pivot}). Both counts are now incidental rather than asserted: the
     *       strict steal bound means only a handful of probes — often none at all — are issued in a
     *       run this short. Worker BULK pages ({@code maxKeys > 1}) are never faulted here —
     *       they must complete for the run to stay byte-exact.</li>
     *   <li><b>STORM only</b>: a leading cold-start burst — the first {@link #STARTUP_TIMEOUT_BURST}
     *       COLD reads (the workers' own first-page fetches into the unwarmed keyspace) time out before any
     *       worker success — fires the sustained-timeout shed (see {@link #STARTUP_TIMEOUT_BURST}). The
     *       burst is transient: those pages retry and serve, so the run still completes byte-exact.</li>
     * </ul>
     */
    private static MockPageFetcher.PageInterceptor coldReadTimeoutInterceptor(Mode mode) {
        return (req, callIndex, page) -> {
            boolean probe = req.maxKeys() <= 1;
            boolean burst = mode == Mode.STORM && callIndex < STARTUP_TIMEOUT_BURST;
            boolean sustainedProbe = probe && callIndex >= STARTUP_TIMEOUT_BURST && (callIndex % 8) == 0;
            if (burst || sustainedProbe) {
                throw ThrottleException.attemptTimeout(
                        "cold read exceeded the scaled attempt-timeout budget");
            }
            return page;
        };
    }

    // ---- fixtures / helpers --------------------------------------------------------------------------

    /** A narrow {@code table/} top over a truncated {@code date=} fan-out — the Hive-partition shape under test. */
    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>(PARTITIONS * PER_PARTITION);
        for (int p = 0; p < PARTITIONS; p++) {
            for (int o = 0; o < PER_PARTITION; o++) {
                keys.add(("table/date=%05d/part-%05d.parquet".formatted(p, o)).getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    private static RunKey key(String name) {
        return new RunKey("s3", null, "bucket", new byte[0], "dense-collapse-" + name,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

}
