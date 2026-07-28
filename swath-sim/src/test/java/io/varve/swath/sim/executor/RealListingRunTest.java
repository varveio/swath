/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
     *
     * <p>Parsed strictly (see {@link #intProperty}): a typo must not silently produce a run at eight
     * workers labelled as whatever the operator meant.
     */
    static final String WORKERS_PROPERTY = "swath.sim.listing.workers";

    /**
     * System property choosing which of {@link SensingRaceProtocol#SEEDS} the traced leg re-runs.
     * Defaults to the first, and exists because the seed worth tracing is whichever one the table above
     * misbehaved at — a run that collapses at one seed and not another is asking exactly the question
     * the trace answers, and tracing every seed would retain a trace per run for no reason.
     *
     * <p>It must name one of those seeds, and is rejected otherwise: the trace exists to explain a row
     * of the table above, and a seed that produced no such row explains nothing.
     */
    static final String TRACE_SEED_PROPERTY = "swath.sim.listing.trace-seed";

    /** The variants raced on this fixture: the shipped sensor, and the two the sensing race left standing. */
    private static final List<SensingVariant> VARIANTS = List.of(
            SensingVariant.CURRENT, SensingVariant.CURSOR_ANCHORED, SensingVariant.RATE_CURSOR_ANCHORED);

    /**
     * The tiers that serve a fixture in an order they imposed rather than the order it holds, so a
     * disordered capture is simulated as if it were a real bucket's key order: the arena reads through
     * a store whose queries are {@code ORDER BY key}, and the Parquet tier re-sorts at query time.
     * Documented on {@code SimStoreFactory}; named here because {@code AUTO} can land on either.
     */
    private static final List<SimStoreBackend> ORDER_MASKED_BACKENDS =
            List.of(SimStoreBackend.ARENA, SimStoreBackend.PARQUET);

    /** The two trace kinds that move occupancy, and so the whole of the reconstruction's input. */
    private static final Set<String> CLAIM_KINDS = Set.of("range.claim", "range.complete");

    /** The pseudo-range a serial span with nothing being drained at all is attributed to. */
    private static final long FLEET_IDLE = -1L;

    /** How many serial holders one arm's decomposition names — enough to see whether it is one. */
    private static final int SERIAL_HOLDERS_REPORTED = 8;

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
        workers = intProperty(WORKERS_PROPERTY, SensingRaceProtocol.WORKERS);
        traceSeed = traceSeedProperty();

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
                        + "workers=%d fixture_keys=%s%n",
                opened.resolvedBackend(), open.toMillis(), config.arenaMaxEncodedBytes() >> 20,
                HeapPeak.liveMbAfterCollection(),
                workers, opened.keyCount().isPresent() ? opened.keyCount().getAsLong() : "unknown");
        // Which tier AUTO picked is a function of the arena budget, i.e. of the heap this JVM was
        // given -- and the harness's own documented invocation raises it (-PsimTestHeap). A run at a
        // big enough heap therefore lands on the arena, which normalises key order on the way in: a
        // disordered capture would be simulated here as if it were the bucket's real order, which is
        // the one thing these numbers must not quietly be.
        if (ORDER_MASKED_BACKENDS.contains(opened.resolvedBackend())) {
            System.out.printf(Locale.ROOT,
                    "real_listing phase=open WARNING backend=%s masks key disorder (it serves the "
                            + "fixture in an order it imposed, not the order the file holds), so these "
                            + "numbers do not prove the capture is in order — run at a smaller heap to "
                            + "put the arena over budget and land on %s, whose decode is guarded%n",
                    opened.resolvedBackend(), SimStoreBackend.STREAMING);
        }
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

        // The fixture's own total where the resolved tier knows it, so "emitted every key" is checked
        // against the bucket rather than against the other legs: a systematic loss — the shape a
        // routing or pagination bug takes — is identical in every leg and passes a cross-leg
        // comparison unnoticed. Only the Parquet tier cannot supply one, and then the weaker check is
        // the honest one and says which it used.
        long keys = measured.getFirst().result().keysEmitted();
        assertThat(keys).as("the fixture served keys at all").isPositive();
        long expected = opened.keyCount().orElse(keys);
        String against = opened.keyCount().isPresent()
                ? "the fixture's own key total" : "the first leg (backend supplies no key total)";
        for (SensingRaceProtocol.Leg leg : measured) {
            assertThat(leg.result().keysEmitted())
                    .as("%s at seed %d emitted every key, against %s", leg.variant(), leg.seed(), against)
                    .isEqualTo(expected);
        }
        assertThat(bench.getFirst().result().keysEmitted())
                .as("the bench-regime leg emitted every key, against %s", against).isEqualTo(expected);
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
        // Couples this leg's detail-string parser to the emitter: a change to the page.commit detail
        // format would otherwise leave every line below silently unprinted and the leg still green,
        // which is a no-op test wearing a diagnosis's name. The timeline counts the tail's keys
        // independently of the trace, so it is the witness that there was something to parse.
        if (result.timeline().keysInTail() > 0) {
            assertThat(tailKeys).as("the trace's page.commit entries carry the tail's key endpoints")
                    .isNotEmpty();
        }
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
     * <b>Where a losing arm's duration goes, per arm.</b> A duration table says one sensor is slower;
     * it does not say what the extra seconds were spent doing, and on a fixture where every arm emits
     * the same keys through the same fleet the answer is always one of two shapes — the run did more
     * work, or it did the same work with fewer ranges alive. This leg separates them: it reports each
     * arm's range-seconds (the integral of live ranges, i.e. the work) alongside its <b>serial</b>
     * seconds (the span during which one range or none was being drained, i.e. the packing), and then
     * names the ranges that held the fleet during those serial spans.
     *
     * <p>Naming them is the part a counter cannot do. A serial span is an address — one range nobody
     * could steal from — so the trace is walked for {@code range.claim}/{@code range.complete} to
     * rebuild occupancy exactly, and every span at occupancy ≤ 1 is attributed to whichever range was
     * still draining, with that range's own committed keys and key extent printed beside it. An arm
     * whose extra duration is one range's serial span has a straggler; an arm whose serial time is
     * spread over dozens of short spans has a placement problem that keeps re-creating one.
     *
     * <p>Separate from the table for the same reason the tail leg is: the trace is this module's
     * expensive artifact, and every arm here runs with it on.
     */
    @Test
    void whereEachArmsDurationGoesOnTheRealListing() {
        for (SensingVariant variant : VARIANTS) {
            PolicyScenario scenario = PolicyRunFixtures
                    .scenario(workers, PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                            PolicyRunFixtures.MEASURED_TAIL_LATENCY, PolicyRunFixtures.measuredCost())
                    .withSeed(traceSeed)
                    .withEventLog(true);
            PolicyRunResult result = SimExecutor.run(scenario, store, storeLabel, variant);
            String arm = SensingRaceProtocol.label(variant);
            SensingRaceProtocol.requireCompleted(result, arm + "/seed " + traceSeed + "/trace");
            PolicyRunTimeline timeline = result.timeline();
            double postSeedSeconds = (timeline.endNanos() - timeline.seedCompletedNanos()) / 1e9;
            System.out.printf(Locale.ROOT,
                    "real_listing phase=decomposition arm=%s seed=%d duration_s=%.3f seed_end_s=%.3f "
                            + "post_seed_s=%.3f range_seconds=%.1f serial_s=%.3f occupancy=%.2f "
                            + "calls=%d pages=%d%n",
                    arm, traceSeed, timeline.endNanos() / 1e9, timeline.seedCompletedNanos() / 1e9,
                    postSeedSeconds, timeline.rangeNanos() / 1e9, timeline.serialNanos() / 1e9,
                    timeline.meanOccupancy(), result.storeCalls(), result.pages());

            Map<Long, Long> serialNanosByNode = serialNanosByNode(result);
            // The trace's own reconstruction has to agree with the timeline the run accumulated
            // independently, or the addresses below belong to some other run than the one measured.
            long tracedSerialNanos = serialNanosByNode.values().stream().mapToLong(Long::longValue).sum();
            assertThat(tracedSerialNanos)
                    .as("%s: the trace's serial spans reconstruct the timeline's own serial time", arm)
                    .isCloseTo(timeline.serialNanos(), within(timeline.serialNanos() / 100 + 1_000_000L));
            Map<Long, TracedRange> ranges = tracedRanges(result);
            serialNanosByNode.entrySet().stream()
                    .sorted(Map.Entry.<Long, Long>comparingByValue().reversed())
                    .limit(SERIAL_HOLDERS_REPORTED)
                    .forEach(held -> {
                        System.out.printf(Locale.ROOT,
                                "real_listing phase=decomposition arm=%s serial_holder=%d serial_s=%.3f %s%n",
                                arm, held.getKey(), held.getValue() / 1e9,
                                ranges.containsKey(held.getKey())
                                        ? ranges.get(held.getKey()).row() : "no committed page");
                        // Its ancestry, because a straggler's birth instant is the run's finish line and
                        // "why so late" is a question about the range it was carved out of, not about it.
                        for (long ancestor = parentOf(ranges, held.getKey()); ancestor >= 0;
                                ancestor = parentOf(ranges, ancestor)) {
                            System.out.printf(Locale.ROOT,
                                    "real_listing phase=decomposition arm=%s ancestor_of=%d node=%d %s%n",
                                    arm, held.getKey(), ancestor, ranges.get(ancestor).row());
                        }
                    });
            result.counters().forEach((name, value) -> System.out.printf(Locale.ROOT,
                    "real_listing phase=counters arm=%s %s=%d%n", arm, name, value));
        }
    }

    /**
     * Serial nanoseconds by the range that was holding the fleet, rebuilt from the trace's claim and
     * complete records. A span counts when at most one range is being drained; a span at zero — every
     * worker parked between a completion and the next claim — is attributed to {@link #FLEET_IDLE},
     * because "nobody was working" and "one range was working alone" are different diagnoses and the
     * timeline's own serial fraction merges them.
     */
    private static Map<Long, Long> serialNanosByNode(PolicyRunResult result) {
        Map<Long, Long> serialNanos = new LinkedHashMap<>();
        Set<Long> live = new LinkedHashSet<>();
        long since = result.timeline().seedCompletedNanos();
        for (SimEventLog.Entry entry : result.log().entries()) {
            if (entry.atNanos() < since || !CLAIM_KINDS.contains(entry.kind())) {
                continue;
            }
            if (live.size() <= 1) {
                serialNanos.merge(live.isEmpty() ? FLEET_IDLE : live.iterator().next(),
                        entry.atNanos() - since, Long::sum);
            }
            if ("range.claim".equals(entry.kind())) {
                live.add(nodeOf(entry));
            } else {
                live.remove(nodeOf(entry));
            }
            since = entry.atNanos();
        }
        if (live.size() <= 1) {
            serialNanos.merge(live.isEmpty() ? FLEET_IDLE : live.iterator().next(),
                    result.timeline().endNanos() - since, Long::sum);
        }
        return serialNanos;
    }

    /**
     * What the trace says about each range that committed a page: when it was born and by which
     * mechanism, how long it drained, how much it held and over which keys. A serial span read without
     * these is a number; read with them it says whether the range that held the fleet was a seed cut
     * the planner made too coarse, or a child some split placed too late to be worth anything.
     */
    private static final class TracedRange {

        private long bornNanos = -1L;
        private String bornBy = "seed";
        private long parent = -1L;
        private long firstPageNanos = -1L;
        private long lastPageNanos;
        private long keys;
        private long pages;
        private byte[] firstKey;
        private byte[] lastKey;
        private final Map<String, Long> carvesRefused = new LinkedHashMap<>();

        String row() {
            return String.format(Locale.ROOT,
                    "born_by=%s parent=%d born_s=%.3f first_page_s=%.3f last_page_s=%.3f keys=%d "
                            + "pages=%d carves_refused=%s first_key=%s last_key=%s",
                    bornBy, parent, bornNanos / 1e9, firstPageNanos / 1e9, lastPageNanos / 1e9, keys,
                    pages, carvesRefused, printable(firstKey), printable(lastKey));
        }
    }

    /** The node {@code child} was split out of, or {@code -1} once the chain reaches a seed range. */
    private static long parentOf(Map<Long, TracedRange> ranges, long child) {
        TracedRange range = ranges.get(child);
        long parent = range == null ? -1L : range.parent;
        return ranges.containsKey(parent) ? parent : -1L;
    }

    /** {@link TracedRange} per node, in the order the nodes first committed. */
    private static Map<Long, TracedRange> tracedRanges(PolicyRunResult result) {
        Map<Long, TracedRange> ranges = new LinkedHashMap<>();
        for (SimEventLog.Entry entry : result.log().entries()) {
            switch (entry.kind()) {
                case "page.commit" -> {
                    TracedRange range = ranges.computeIfAbsent(nodeOf(entry), node -> new TracedRange());
                    range.keys += Long.parseLong(field(entry, "keys="));
                    range.pages++;
                    range.lastPageNanos = entry.atNanos();
                    byte[] from = HexFormat.of().parseHex(field(entry, "from="));
                    if (range.firstPageNanos < 0L) {
                        range.firstPageNanos = entry.atNanos();
                        range.firstKey = from;
                    }
                    range.lastKey = from;
                }
                case "owner_split.skip" -> ranges
                        .computeIfAbsent(nodeOf(entry), node -> new TracedRange())
                        .carvesRefused.merge(field(entry, "reason="), 1L, Long::sum);
                case "owner_split", "steal.split" -> {
                    TracedRange child = ranges.computeIfAbsent(
                            Long.parseLong(field(entry, "child=")), node -> new TracedRange());
                    child.bornNanos = entry.atNanos();
                    child.bornBy = entry.kind();
                    child.parent = Long.parseLong(
                            field(entry, "owner_split".equals(entry.kind()) ? "node=" : "victim="));
                }
                default -> {
                }
            }
        }
        return ranges;
    }

    /** The {@code node=} (or {@code victim=}) id a trace entry carries. */
    private static long nodeOf(SimEventLog.Entry entry) {
        return Long.parseLong(field(entry, "node="));
    }

    private static String field(SimEventLog.Entry entry, String name) {
        for (String field : entry.detail().split("\\|")) {
            if (field.startsWith(name)) {
                return field.substring(name.length());
            }
        }
        throw new AssertionError("trace entry " + entry.kind() + " carries no " + name + ": "
                + entry.detail());
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

    /**
     * One leg, and the run cost it took to produce — the two things a sweep is sized from.
     *
     * <p>The heap figure is taken <b>per leg, here</b>, between resetting the heap pools' peak-usage
     * marks and reading them back: a number read once at print time after every leg has finished is
     * the same number thirteen times, and says nothing about any of them.
     */
    private static SensingRaceProtocol.Leg runLeg(SensingVariant variant, long seed, int pageSize,
                                                  LatencyModel latency, List<Cost> costs) {
        PolicyScenario scenario = PolicyRunFixtures
                .scenario(workers, pageSize, latency, PolicyRunFixtures.measuredCost())
                .withSeed(seed);
        HeapPeak.reset();
        Instant startedAt = Instant.now();
        PolicyRunResult result = SimExecutor.run(scenario, store, storeLabel, variant);
        Duration wall = Duration.between(startedAt, Instant.now());
        double heapPeakMb = HeapPeak.peakMb();
        String label = SensingRaceProtocol.label(variant);
        SensingRaceProtocol.requireCompleted(result, label + "/seed " + seed + "/page " + pageSize);
        costs.add(new Cost(label, seed, pageSize, wall, heapPeakMb, result));
        return new SensingRaceProtocol.Leg(label, "real-listing", seed, pageSize, result);
    }

    /**
     * What one leg cost to produce, as opposed to what it found. {@code heapPeakMb} is the high-water
     * mark of heap <em>allocated and not yet collected</em> during that leg, not its live set — so it
     * is an upper bound on what the leg needed to hold, and it moves with GC timing as well as with
     * the run.
     *
     * <p>Two things it necessarily includes, because they are on the heap while the leg runs: the one
     * shared store handle, opened before any leg and held across all of them, and whatever the
     * previous leg left uncollected. On an {@code ARENA} resolution the store is the whole of it — the
     * arena's key column dwarfs a leg's own allocation and the column reads nearly flat. It is the
     * {@code STREAMING} resolution, whose decoded segments are <b>off-heap</b> and absent from this
     * figure, where the number is mostly the run's.
     */
    private record Cost(String variant, long seed, int pageSize, Duration wall, double heapPeakMb,
                        PolicyRunResult result) {

        String row() {
            return String.format(Locale.ROOT, "%-14s %-10d %5d  %8d %9d %9d %9d %13.1f",
                    variant, seed, pageSize, wall.toMillis(), result.run().eventsProcessed(),
                    result.storeCalls(), result.storeReads(), heapPeakMb);
        }
    }

    private static void printCosts(List<Cost> costs) {
        StringBuilder out = new StringBuilder("real listing — run cost").append(System.lineSeparator());
        out.append(String.format(Locale.ROOT, "%-14s %-10s %5s  %8s %9s %9s %9s %13s",
                "variant", "seed", "page", "wall_ms", "events", "calls", "reads", "heap_peak_mb"))
                .append(System.lineSeparator());
        for (Cost cost : costs) {
            out.append(cost.row()).append(System.lineSeparator());
        }
        System.out.print(out);
    }

    /**
     * Every key interval committed at or after {@code fromNanos}, flattened to its endpoints. The
     * trace carries each committed page's own {@code from}/{@code to} keys precisely so a reader can
     * see which part of the keyspace a phase covered; nothing else in a run record can answer that.
     *
     * <p>The endpoints are not all keys the run emitted: a {@code from} is a real key the page started
     * at, but a {@code to} is a cursor target, which for the last page of a range is the range's own
     * upper bound and so may be a synthetic successor rather than anything the bucket holds. That is
     * the right input for "where in the keyspace was the tail" — the question this feeds — and the
     * wrong one for counting keys.
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

    /**
     * {@code name}'s value as an {@code int}, or {@code fallback} when unset. Parsed explicitly rather
     * than through {@link Integer#getInteger}, which answers a malformed value with the default — a
     * run at eight workers reported as a run at whatever was typed — and reads a leading {@code 0} as
     * octal.
     */
    private static int intProperty(String name, int fallback) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("-D" + name + " must be a decimal integer, got: " + raw, e);
        }
    }

    /** {@link #TRACE_SEED_PROPERTY}, checked against the protocol's own seeds — see its javadoc. */
    private static long traceSeedProperty() {
        String raw = System.getProperty(TRACE_SEED_PROPERTY);
        if (raw == null || raw.isBlank()) {
            return SensingRaceProtocol.SEEDS[0];
        }
        long seed;
        try {
            seed = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("-D" + TRACE_SEED_PROPERTY
                    + " must be one of " + Arrays.toString(SensingRaceProtocol.SEEDS) + ", got: " + raw, e);
        }
        if (Arrays.stream(SensingRaceProtocol.SEEDS).noneMatch(s -> s == seed)) {
            throw new IllegalArgumentException("-D" + TRACE_SEED_PROPERTY + " must be one of "
                    + Arrays.toString(SensingRaceProtocol.SEEDS) + ", got: " + seed);
        }
        return seed;
    }
}
