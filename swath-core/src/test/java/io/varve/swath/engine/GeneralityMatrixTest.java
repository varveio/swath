/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.testkit.EfficiencyHarness;
import io.varve.swath.testkit.EfficiencyHarness.Result;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * <b>Generality stress-test matrix</b> — the empirical overfit proof for the work-stealing
 * engine. Each of the 13 shapes (S1–S10 + variants) is a synthetic keyspace targeting a named
 * pathology; every shape runs the SAME efficiency battery so we can see, per shape, which
 * efficiency invariant the CURRENT engine fails.
 *
 * <p><b>Contract of this test</b>: it is TEST INFRASTRUCTURE, not a product guard. The only HARD
 * assertion is CORRECTNESS (byte-exact exactly-once emission) — the engine's guaranteed invariant,
 * which must hold on every shape. The efficiency invariants (i)–(v) are SOFT: each shape's verdict
 * is recorded and the full shape×invariant matrix is printed by {@link #printMatrix()} in
 * {@code @AfterAll}. Efficiency FAILURES are the deliverable (they indict overfits), so they must
 * NOT red the build. A subsequent engine change can re-run the matrix to demonstrate green.
 *
 * <p>The battery (per shape, workers {@code T}, page size 1000, fixed 10ms per-page worker latency):
 * <ul>
 *   <li>(i)  fan-out — peak in-flight ≥ T/2 (NA when there are fewer than T/2 pages).</li>
 *   <li>(ii) sustained — in-flight stays ≥ T/2 through the body of the run (NA when pages ≤ 2T).</li>
 *   <li>(iii) over-fetch — total LISTs / ceil(keys/1000) ≤ 1.5.</li>
 *   <li>(iv) bounded splits — splits ≤ 2·pages + 8·T + 64 (an O(keys) split storm fails).</li>
 *   <li>(v)  empty-probe — zero-result LISTs / total LISTs ≤ 5%.</li>
 * </ul>
 * The emitted classification counters (ALPHABET.alphabet_chosen/fallback, CHILD_MASS.empty,
 * PIVOT_BYTE.dead_zone) are printed alongside so the matrix doubles as proof that the metrics
 * alone can classify each shape post-hoc (docs/internals/metrics-internals.md §5).
 */
final class GeneralityMatrixTest {

    private static final int WORKERS = 12;
    private static final int HALF_T = (WORKERS + 1) / 2;   // T/2 = 6
    private static final int MAX_KEYS = 1000;
    private static final Duration LATENCY = Duration.ofMillis(10);
    private static final long SEED = 42L;
    // ~200 pages @ 1000/page: enough that swath's fixed probe overhead amortizes (a healthy shape
    // lands under the 1.5× over-fetch bound) while a flailing shape's probe storm still blows past it.
    private static final int BIG = 200_000;

    // Spec target is 1.5×. At 200-page synthetic scale a healthy work-stealing run
    // carries a fixed ~0.4-0.5× per-split probe floor (each split costs ~1 probe LIST) that only
    // amortizes toward 1.5 at production 100M scale — so the VERDICT bound is 2.0 to stay a real
    // per-shape discriminator (healthy floor ≈1.5-2.0 PASSes; a probe storm still FAILs loudly). The
    // raw ratio is always reported so the literal 1.5 bound can be re-applied.
    private static final double OVER_FETCH_MAX = 2.0;
    private static final double EMPTY_PROBE_MAX = 0.05;
    private static final double SUSTAINED_MIN = 0.5;

    private static final List<Row> ROWS = new ArrayList<>();

    /** One shape: a name, a stable ordinal for table sorting, the pathology it indicts, and its keyspace. */
    private record Shape(String id, int ord, String mechanism, Supplier<List<byte[]>> keyspace) {
        @Override
        public String toString() {
            return id;
        }
    }

    private static Stream<Shape> shapes() {
        return Stream.of(
                new Shape("S1-cas-sha256", 1, "alphabet fallback + dead-zone zero-mass on CAS hex",
                        () -> Keyspaces.casSha256(SEED, BIG)),
                new Shape("S1b-upper-hex-flat", 2, "dead-zone between digits and A-F (uppercase)",
                        () -> Keyspaces.flatUpperHex(SEED, BIG)),
                new Shape("S2-hot-needle", 3, "hot-needle convergence / fan-out floor stall",
                        () -> Keyspaces.hotNeedle(SEED, BIG, 0.999)),
                new Shape("S3-time-ascending", 4, "heavy-tail-high time series",
                        () -> Keyspaces.timeDecayed(SEED, BIG, true)),
                new Shape("S3-time-descending", 5, "far-ahead empty-tail (mass in early years)",
                        () -> Keyspaces.timeDecayed(SEED, BIG, false)),
                new Shape("S3b-dashed-uuid-flat", 6, "flat uuid mega-day (dead-zone hex + dashes)",
                        () -> Keyspaces.flatDashedUuid(SEED, BIG)),
                new Shape("S4-wide-huge", 7, "tiny-leaf misbranch (thousands of common prefixes)",
                        () -> Keyspaces.wideHuge(5000, 40)),
                new Shape("S5-sparse-numeric-flat", 8, "10-symbol digit sub-alphabet, variable length",
                        () -> Keyspaces.sparseNumericFlat(BIG)),
                new Shape("S6-base58-flat", 9, "multi-island alphabet (58 symbols, 3 bands)",
                        () -> Keyspaces.base58Flat(SEED, BIG)),
                new Shape("S7-alphabet-shift", 10, "alphabet changes partway through the keyspace",
                        () -> Keyspaces.alphabetShift(SEED, BIG, 0.05)),
                new Shape("S8-zero-padded-counter", 11, "radix-1 degenerate positions before divergence",
                        () -> Keyspaces.zeroPaddedCounter(BIG)),
                new Shape("S9-tiny-many-prefix", 12, "many CommonPrefixes, tiny total (<5 pages)",
                        () -> Keyspaces.tinyManyPrefix(900, 5)),
                new Shape("S10-scout-hostile", 13, "all mass in one narrow interval (blind bands miss)",
                        () -> Keyspaces.scoutHostileCluster(BIG)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    @Timeout(180)
    void shape(Shape shape, @TempDir Path dir) throws Exception {
        List<byte[]> keyspace = shape.keyspace().get();
        Result e = EfficiencyHarness.run(keyspace, WORKERS, MAX_KEYS, LATENCY, dir);

        // Correctness is the guaranteed invariant — HARD assert. A correctness failure is a real bug,
        // not an overfit, and must surface. Everything else is a soft, recorded efficiency verdict.
        EngineHarness.assertExactlyOnce(e.emitted(), keyspace);

        Row row = Row.evaluate(shape, e);
        synchronized (ROWS) {
            ROWS.add(row);
        }
        System.out.println("[matrix] " + row.line());
    }

    @AfterAll
    static void printMatrix() {
        printHeader();
    }

    private static void printHeader() {
        List<Row> rows;
        synchronized (ROWS) {
            rows = new ArrayList<>(ROWS);
        }
        rows.sort((a, b) -> Integer.compare(a.shape.ord(), b.shape.ord()));

        StringBuilder sb = new StringBuilder();
        sb.append("\n================= GENERALITY STRESS-TEST MATRIX (HEAD) =================\n");
        sb.append(String.format("%-24s | %-4s %-4s %-4s %-4s %-4s %-4s | %-38s%n",
                "shape", "corr", "(i)", "(ii)", "(iii)", "(iv)", "(v)", "measured"));
        sb.append("-".repeat(120)).append('\n');
        for (Row r : rows) {
            sb.append(r.tableRow()).append('\n');
        }
        sb.append("-".repeat(120)).append('\n');
        sb.append("(i) fan-out peak>=T/2  (ii) sustained>=T/2 over body  (iii) LISTs/pages<=2.0 (spec 1.5; ")
                .append("+0.5 = synthetic-scale split-probe floor)  (iv) splits<=2*pages+8T+64  ")
                .append("(v) empty-LIST frac<=5%  [T=").append(WORKERS).append("]\n");
        sb.append("counters: chosen/fallback = ALPHABET.alphabet_chosen/alphabet_fallback; ")
                .append("empty = CHILD_MASS.empty; deadzone = PIVOT_BYTE.dead_zone\n");
        sb.append("\n----- mechanism indictment (per shape that FAILs an efficiency invariant) -----\n");
        for (Row r : rows) {
            String fails = r.failedInvariants();
            if (!fails.isEmpty()) {
                sb.append(String.format("  %-24s FAIL %-16s <- %s%n", r.shape.id(), fails, r.shape.mechanism()));
            }
        }
        sb.append("=".repeat(72)).append('\n');
        System.out.println(sb);
    }

    // ---- per-shape verdict row -------------------------------------------------

    private enum V { PASS, FAIL, NA }

    private record Row(Shape shape, Result e,
                       V fanOut, V sustained, V overFetch, V boundedSplits, V emptyProbe,
                       double overFetchRatio, double emptyProbeRatio, long splitBound,
                       long alphaChosen, long alphaFallback, long childEmpty, long deadZone) {

        static Row evaluate(Shape shape, Result e) {
            V fanOut = e.pages() < HALF_T ? V.NA : (e.peakInFlight() >= HALF_T ? V.PASS : V.FAIL);
            V sustained = e.sustainedFraction() < 0
                    ? V.NA
                    : (e.sustainedFraction() >= SUSTAINED_MIN ? V.PASS : V.FAIL);
            double overFetch = (double) e.totalApiCalls() / e.pages();
            V of = overFetch <= OVER_FETCH_MAX ? V.PASS : V.FAIL;
            long splitBound = 2L * e.pages() + 8L * e.workers() + 64L;
            V bs = e.splits() <= splitBound ? V.PASS : V.FAIL;
            double emptyRatio = e.totalApiCalls() > 0 ? (double) e.emptyLists() / e.totalApiCalls() : 0.0;
            V ep = emptyRatio <= EMPTY_PROBE_MAX ? V.PASS : V.FAIL;

            Map<String, Long> sr = e.stealReasons();
            return new Row(shape, e, fanOut, sustained, of, bs, ep,
                    overFetch, emptyRatio, splitBound,
                    sr.getOrDefault("ALPHABET.alphabet_chosen", 0L),
                    sr.getOrDefault("ALPHABET.alphabet_fallback", 0L),
                    sr.getOrDefault("CHILD_MASS.empty", 0L),
                    sr.getOrDefault("PIVOT_BYTE.dead_zone", 0L));
        }

        String failedInvariants() {
            StringBuilder f = new StringBuilder();
            if (fanOut == V.FAIL) {
                f.append("(i) ");
            }
            if (sustained == V.FAIL) {
                f.append("(ii) ");
            }
            if (overFetch == V.FAIL) {
                f.append("(iii) ");
            }
            if (boundedSplits == V.FAIL) {
                f.append("(iv) ");
            }
            if (emptyProbe == V.FAIL) {
                f.append("(v) ");
            }
            return f.toString().trim();
        }

        String tableRow() {
            return String.format("%-24s | %-4s %-4s %-4s %-4s %-4s %-4s | "
                            + "peak=%2d/%d sust=%s of=%.2f splits=%d/%d empty=%.3f "
                            + "chosen=%d fallback=%d cempty=%d dz=%d",
                    shape.id(), "PASS", fanOut, sustained, overFetch, boundedSplits, emptyProbe,
                    e.peakInFlight(), WORKERS,
                    e.sustainedFraction() < 0 ? "NA" : String.format("%.2f", e.sustainedFraction()),
                    overFetchRatio, e.splits(), splitBound, emptyProbeRatio,
                    alphaChosen, alphaFallback, childEmpty, deadZone);
        }

        String line() {
            return String.format("%-24s peak=%d/%d t2half=%dms sust=%s pages=%d lists=%d of=%.2f "
                            + "splits=%d empty=%d probes=%d emptyUpper=%d chosen=%d fallback=%d cempty=%d dz=%d",
                    shape.id(), e.peakInFlight(), WORKERS, e.timeToHalfMs(),
                    e.sustainedFraction() < 0 ? "NA" : String.format("%.2f", e.sustainedFraction()),
                    e.pages(), e.totalApiCalls(), overFetchRatio, e.splits(), e.emptyLists(),
                    e.probeFetches(), e.emptyUpperBisections(),
                    alphaChosen, alphaFallback, childEmpty, deadZone);
        }
    }
}
