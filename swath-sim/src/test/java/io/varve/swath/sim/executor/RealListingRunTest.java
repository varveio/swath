/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.model.LatencyModel;
import io.varve.swath.sim.store.SimStoreBackend;
import io.varve.swath.sim.store.SimStoreConfig;
import io.varve.swath.sim.store.SimStoreFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <b>The real policies against a real bucket's listing</b> — the leg every synthetic keyspace in this
 * module exists to stand in for. A generated fixture is a hypothesis about what a bucket looks like;
 * this runs the same fleet, at the same page regime and the same measured client cost, over a listing
 * captured from an actual object store, and reports the same columns the sensing race reports so the
 * two are read side by side.
 *
 * <p><b>Opt-in, and it brings its own fixture.</b> This module never bundles or downloads a corpus
 * listing (see the README's "Fixtures" section), so the run is gated on {@link #FIXTURE_PROPERTY}
 * naming a local sorted, stamped capture — a file or a directory of them — and is <em>skipped</em>,
 * never failed, when it is unset:
 *
 * <pre>{@code ./gradlew :swath-sim:test -PonlyPerf -Dswath.sim.listing.fixture=/path/to/fixture}</pre>
 *
 * Nothing here knows or records which bucket that is; the fixture is a path, its identity is the
 * operator's, and the run record prints the resolved backend rather than the location.
 *
 * <p><b>One store handle serves every run.</b> Opening a multi-million-key fixture costs more than
 * the runs do, and the tier resolution itself is part of what a run must be read against, so the store
 * is opened once in {@link #openFixture()}, its backend and open cost reported, and every leg below
 * drives that one handle. The arena budget is a third of the heap rather than the shipped default:
 * budgeting the whole heap to it would let it OOM before its own check could decline gracefully, which
 * is the same sizing {@code StoreThroughputBenchTest} uses and the same reason.
 *
 * <h2>What is measured, and how it is quoted</h2>
 * Three sensing variants — the shipped sensor and the two the race left standing — at the four seeds
 * the race is judged on, at the <b>measured</b> page regime, plus one leg at the bench's 100-key
 * regime for cross-comparison. Every serial/tail number carries the page size it was taken at
 * (methodology principle 13: a bench number states its regime), and every variant runs at every seed
 * (quoting a subset is cherry-picking). Nothing here asserts a magnitude: what a real listing does is
 * the measurement, and a threshold invented here would be a threshold fitted to it. What <em>is</em>
 * asserted is the pair of facts a table of numbers is worthless without — every leg completed, and
 * every leg emitted the same number of keys as the first.
 *
 * <p>The run's own cost (wall time, events, store reads) is reported alongside, because this is also
 * how a corpus sweep is sized before it is launched.
 */
@Tag("perf")
class RealListingRunTest {

    /** System property naming a local sorted, stamped listing fixture (a file or a directory of them). */
    static final String FIXTURE_PROPERTY = "swath.sim.listing.fixture";

    /**
     * System property overriding the modelled fleet size. Defaults to the sensing race's eight, so a
     * result is comparable with the synthetic benches by default; set it to the concurrency the
     * listing's own capture ran at when the question is why <em>that</em> run behaved as it did, since
     * how hard a keyspace is to divide is a statement about a fleet size, not about the bucket alone.
     */
    static final String WORKERS_PROPERTY = "swath.sim.listing.workers";

    /**
     * System property choosing which of the protocol's seeds the traced leg re-runs. Defaults to the
     * first, and exists because the seed worth tracing is whichever one the table above misbehaved at
     * — a run that collapses at one seed and not another is asking exactly the question the trace
     * answers, and tracing every seed would retain a trace per run for no reason.
     */
    static final String TRACE_SEED_PROPERTY = "swath.sim.listing.trace-seed";

    /** The variants raced on this fixture: the shipped sensor, and the two the sensing race left standing. */
    private static final List<SensingVariant> VARIANTS = List.of(
            SensingVariant.CURRENT, SensingVariant.CURSOR_ANCHORED, SensingVariant.RATE_CURSOR_ANCHORED);

    private static int workers;
    private static long traceSeed;
    private static Path fixture;
    private static SimStoreFactory.Result opened;
    private static ListingStore store;
    private static String storeLabel;

    @BeforeAll
    static void openFixture() {
        String configured = System.getProperty(FIXTURE_PROPERTY);
        assumeTrue(configured != null && !configured.isBlank(),
                "-D" + FIXTURE_PROPERTY + " is unset; no real listing to run");
        fixture = Path.of(configured);
        assertThat(Files.exists(fixture)).as("fixture at %s", fixture).isTrue();
        workers = Integer.getInteger(WORKERS_PROPERTY, SensingRaceProtocol.WORKERS);
        traceSeed = Long.getLong(TRACE_SEED_PROPERTY, SensingRaceProtocol.SEEDS[0]);

        // A third of the heap, not the whole of it: an arena sized to the heap OOMs before its own
        // budget check can decline, and declining is the outcome a giant listing is supposed to have.
        SimStoreConfig config = new SimStoreConfig(Runtime.getRuntime().maxMemory() / 3,
                SimStoreConfig.DEFAULT_STREAMING_MAX_RESIDENT_BYTES);
        Instant startedAt = Instant.now();
        opened = SimStoreFactory.open(fixture, SimStoreBackend.AUTO, config);
        Duration open = Duration.between(startedAt, Instant.now());
        store = opened.store();
        storeLabel = "real listing fixture (" + opened.resolvedBackend() + ")";
        System.out.printf(Locale.ROOT,
                "real_listing phase=open backend=%s open_ms=%d arena_budget_mb=%d heap_used_mb=%.1f "
                        + "workers=%d%n",
                opened.resolvedBackend(), open.toMillis(), config.arenaMaxEncodedBytes() >> 20, heapUsedMb(),
                workers);
    }

    @AfterAll
    static void closeFixture() {
        if (store != null) {
            store.close();
        }
    }

    /**
     * The table: every variant at every seed at the measured regime, then the same shipped sensor at
     * the bench's page regime. The two regimes are printed as two tables, never merged into one
     * ranking — pages per range is the scaling variable, so a serial fraction from one says nothing
     * about the other.
     */
    @Test
    void everySensingVariantOverTheRealListingAtBothPageRegimes() {
        List<SensingRaceProtocol.Leg> measured = new ArrayList<>();
        List<Cost> costs = new ArrayList<>();
        for (SensingVariant variant : VARIANTS) {
            for (long seed : SensingRaceProtocol.SEEDS) {
                measured.add(runLeg(variant, seed, PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                        PolicyRunFixtures.MEASURED_TAIL_LATENCY, costs));
            }
        }
        List<SensingRaceProtocol.Leg> bench = List.of(runLeg(SensingVariant.CURRENT,
                SensingRaceProtocol.SEEDS[0], SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, costs));

        SensingRaceProtocol.printTable("real listing — measured page regime ("
                + PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE + "-key pages)", measured);
        SensingRaceProtocol.printTable("real listing — bench page regime ("
                + SensingRaceProtocol.BENCH_PAGE_SIZE + "-key pages, shipped sensor only)", bench);
        printCosts(costs);
        System.out.println(measured.getFirst().result().describe());

        long keys = measured.getFirst().result().keysEmitted();
        assertThat(keys).as("the fixture served keys at all").isPositive();
        for (SensingRaceProtocol.Leg leg : measured) {
            assertThat(leg.result().keysEmitted())
                    .as("%s at seed %d emitted every key", leg.variant(), leg.seed()).isEqualTo(keys);
        }
        assertThat(bench.getFirst().result().keysEmitted())
                .as("the bench-regime leg emitted every key").isEqualTo(keys);
    }

    /**
     * <b>Where the tail lives.</b> A serial fraction says how long the fleet ran one range at a time;
     * it does not say <em>which</em> range, and on a real bucket that is the whole diagnosis — a tail
     * inside one directory is a splitting failure with an address. This leg re-runs the shipped sensor
     * with the event trace on and reports where the keys committed after the last split actually sit:
     * their longest common prefix, their first and last key, and how they distribute over the
     * second-level directories they fall in — the last being the one that survives a tail spanning two
     * siblings, where a common prefix collapses to the bucket root and says nothing.
     *
     * <p>Separate from the table above because the trace is the expensive artifact in this module (a
     * run over a giant fixture retains an entry per event), so a failure to hold it must not cost the
     * table.
     */
    @Test
    void whereTheTailLivesOnTheRealListing() {
        PolicyScenario scenario = PolicyRunFixtures
                .scenario(workers, PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                        PolicyRunFixtures.MEASURED_TAIL_LATENCY, PolicyRunFixtures.measuredCost())
                .withSeed(traceSeed)
                .withEventLog(true);
        PolicyRunResult result = SimExecutor.run(scenario, store, storeLabel, SensingVariant.CURRENT);
        assertThat(result.completed()).as("the traced leg completed").isTrue();

        List<byte[]> tailKeys = committedKeysAfter(result, result.timeline().lastSplitNanos());
        System.out.printf(Locale.ROOT,
                "real_listing phase=tail page=%d seed=%d tail_fraction=%.4f serial_fraction=%.4f "
                        + "keys_in_tail=%d traced_pages_in_tail=%d%n",
                PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE, traceSeed, result.timeline().tailFraction(),
                result.timeline().serialFraction(), result.timeline().keysInTail(), tailKeys.size());
        if (!tailKeys.isEmpty()) {
            System.out.printf(Locale.ROOT, "real_listing phase=tail common_prefix=%s%n",
                    printable(longestCommonPrefix(tailKeys)));
            System.out.printf(Locale.ROOT, "real_listing phase=tail first_key=%s last_key=%s%n",
                    printable(tailKeys.getFirst()), printable(tailKeys.getLast()));
            directoryShares(tailKeys).forEach((directory, share) -> System.out.printf(Locale.ROOT,
                    "real_listing phase=tail directory=%s share=%.4f%n", directory, share));
        }
    }

    /**
     * The share of {@code keys} falling under each second-level directory, highest first. Two levels
     * because that is where a real bucket's mass concentration shows up — one top-level namespace
     * holding several datasets, one of which is most of the bucket — and a share table over them says
     * whether a tail is one directory's fault or spread over many.
     */
    private static LinkedHashMap<String, Double> directoryShares(List<byte[]> keys) {
        Map<String, Long> counts = new HashMap<>();
        for (byte[] key : keys) {
            counts.merge(directory(key), 1L, Long::sum);
        }
        LinkedHashMap<String, Double> shares = new LinkedHashMap<>();
        counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .forEach(e -> shares.put(e.getKey(), (double) e.getValue() / keys.size()));
        return shares;
    }

    /** {@code key} up to and including its second {@code /}, or the whole key when it has fewer. */
    private static String directory(byte[] key) {
        int slashes = 0;
        for (int i = 0; i < key.length; i++) {
            if (key[i] == '/' && ++slashes == 2) {
                return printable(Arrays.copyOf(key, i + 1));
            }
        }
        return printable(key);
    }

    /** One leg, and the run cost it took to produce — the two things a sweep is sized from. */
    private static SensingRaceProtocol.Leg runLeg(SensingVariant variant, long seed, int pageSize,
                                                  LatencyModel latency, List<Cost> costs) {
        PolicyScenario scenario = PolicyRunFixtures
                .scenario(workers, pageSize, latency, PolicyRunFixtures.measuredCost())
                .withSeed(seed);
        Instant startedAt = Instant.now();
        PolicyRunResult result = SimExecutor.run(scenario, store, storeLabel, variant);
        Duration wall = Duration.between(startedAt, Instant.now());
        String label = SensingRaceProtocol.label(variant);
        if (!result.completed()) {
            throw new AssertionError("leg " + label + "/seed " + seed + "/page " + pageSize
                    + " did not complete:" + System.lineSeparator() + result.describe());
        }
        costs.add(new Cost(label, seed, pageSize, wall, result));
        return new SensingRaceProtocol.Leg(label, "real-listing", seed, pageSize, result);
    }

    /** What one leg cost to produce, as opposed to what it found. */
    private record Cost(String variant, long seed, int pageSize, Duration wall, PolicyRunResult result) {

        String row() {
            return String.format(Locale.ROOT, "%-14s %-10d %5d  %8d %9d %9d %9d %10.1f",
                    variant, seed, pageSize, wall.toMillis(), result.run().eventsProcessed(),
                    result.storeCalls(), result.storeReads(), heapUsedMb());
        }
    }

    private static void printCosts(List<Cost> costs) {
        StringBuilder out = new StringBuilder("real listing — run cost").append(System.lineSeparator());
        out.append(String.format(Locale.ROOT, "%-14s %-10s %5s  %8s %9s %9s %9s %10s",
                "variant", "seed", "page", "wall_ms", "events", "calls", "reads", "heap_mb"))
                .append(System.lineSeparator());
        for (Cost cost : costs) {
            out.append(cost.row()).append(System.lineSeparator());
        }
        System.out.print(out);
    }

    /**
     * Every key interval committed strictly after {@code fromNanos}, flattened to its endpoints. The
     * trace carries each committed page's own {@code from}/{@code to} keys precisely so a reader can
     * see which part of the keyspace a phase covered; nothing else in a run record can answer that.
     */
    private static List<byte[]> committedKeysAfter(PolicyRunResult result, long fromNanos) {
        List<byte[]> keys = new ArrayList<>();
        for (SimEventLog.Entry entry : result.log().entries()) {
            if (entry.atNanos() < fromNanos || !"page.commit".equals(entry.kind())) {
                continue;
            }
            for (String field : entry.detail().split("\\|")) {
                if ((field.startsWith("from=") || field.startsWith("to=")) && field.length() > field.indexOf('=') + 1) {
                    keys.add(HexFormat.of().parseHex(field.substring(field.indexOf('=') + 1)));
                }
            }
        }
        return keys;
    }

    private static byte[] longestCommonPrefix(List<byte[]> keys) {
        byte[] prefix = keys.getFirst();
        for (byte[] key : keys) {
            int i = 0;
            while (i < prefix.length && i < key.length && prefix[i] == key[i]) {
                i++;
            }
            prefix = Arrays.copyOf(prefix, i);
        }
        return prefix;
    }

    /** Printable form of a key: ASCII as itself, anything else as {@code \xNN}. */
    private static String printable(byte[] key) {
        StringBuilder out = new StringBuilder(key.length);
        for (byte b : key) {
            int v = b & 0xff;
            if (v >= 0x20 && v < 0x7f) {
                out.append((char) v);
            } else {
                out.append(String.format(Locale.ROOT, "\\x%02x", v));
            }
        }
        return out.toString();
    }

    /** Live heap after a collection — the on-heap half of "what did this run cost to hold". */
    private static double heapUsedMb() {
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (double) (1 << 20);
    }
}
