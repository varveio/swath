/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.sim.store.SimStoreBackend;
import io.varve.swath.sim.store.SimStoreConfig;
import io.varve.swath.sim.store.SimStoreFactory;
import io.varve.swath.sort.RowGroupOrderException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.stream.Stream;

/**
 * <b>The same three sensing arms over a whole corpus of captured listings, in one JVM.</b>
 * {@link RealListingRunTest} answers "what does this bucket do"; this answers "which buckets do it",
 * which is a different question only because of what it costs: opening a multi-million-key fixture
 * dominates every run over it by an order of magnitude, and a JVM start plus a Gradle invocation
 * dominates a small fixture's open. So a corpus sweep is one process that opens each fixture once,
 * races the arms against that handle, closes it, and moves on — and writes its results out as it goes,
 * so an hour-long sweep that dies at the ninetieth fixture still yields the eighty-nine before it.
 *
 * <h2>What a fixture is here</h2>
 * A directory under the sweep root holding a {@code data/} subdirectory of capture Parquet, i.e. the
 * layout a capture is staged in. Its sibling {@code summary.json} is the capture's own run record and
 * supplies the one input the sweep cannot invent — see below. Nothing here knows what any of those
 * directories are: the root is a local path the operator supplies, the fixture's name in the output is
 * its directory name, and the repo names no corpus, bucket or location (the module README's "Fixtures"
 * rule).
 *
 * <h2>Three choices that decide what the numbers mean</h2>
 * <ol>
 *   <li><b>{@link SimStoreBackend#STREAMING}, forced.</b> Not {@code AUTO}: the arena tier declines by
 *       budget on a fixture this size anyway, and {@code AUTO} pays a DuckDB materialization to find
 *       that out, once per fixture, for nothing. Forcing it also fixes the tier across the corpus —
 *       and it is the tier whose decode is order-guarded, so a sweep on it cannot quietly simulate a
 *       disordered capture as if its order were the bucket's.</li>
 *   <li><b>Each fixture's own fleet size</b>, read from its capture's {@code max_parallel_listings}.
 *       How hard a keyspace is to divide is a statement about a fleet size and not about the bucket
 *       alone — a collapse that is exact at the concurrency the capture actually ran at is invisible at
 *       eight workers — so a corpus comparison at one arbitrary fleet size would be a comparison of
 *       the wrong thing. A capture that carries no summary is swept at {@link #FALLBACK_WORKERS} and
 *       <b>says so in its row</b>, because that row is then not comparable with the rest.</li>
 *   <li><b>Two screening seeds, four where it matters.</b> Four seeds per arm per fixture is the
 *       verdict standard ({@link SensingRaceProtocol}), and a corpus of it costs twice what a screen
 *       does for fixtures that are uninteresting at both seeds. So the sweep screens at two and
 *       escalates to four on any fixture that shows {@linkplain #divergent divergence} — and the
 *       {@code escalated} column says which, so no two-seed row can be quoted as a verdict.</li>
 * </ol>
 *
 * <h2>A fixture that cannot be read is data, not a failure</h2>
 * The streaming tier refuses a capture whose keys are not in order, either at index derive (disorder
 * across row groups makes it ineligible) or at the first row group a run faults in
 * ({@link RowGroupOrderException}, which carries the file, the row group and the row). Either way the
 * sweep records the exclusion with everything the report needs to locate it in the corpus and
 * continues to the next fixture: the exclusion list is an output of the sweep — corpus QA the operator
 * wants regardless — and one unreadable capture must not cost the other hundred.
 */
final class CorpusSweep {

    /** The subdirectory of a staged capture holding its Parquet, and so the path a store opens. */
    static final String DATA_DIRECTORY = "data";

    /** The capture's own run record, sibling of {@link #DATA_DIRECTORY} — see {@link #fleetOf}. */
    static final String SUMMARY_FILE = "summary.json";

    /** The concurrency a capture ran at, in that summary. */
    static final String MAX_PARALLEL_LISTINGS = "max_parallel_listings";

    /** The three arms: the shipped sensor, and the two the sensing race left standing. */
    static final List<SensingVariant> ARMS = List.of(
            SensingVariant.CURRENT, SensingVariant.CURSOR_ANCHORED, SensingVariant.RATE_CURSOR_ANCHORED);

    /** The two seeds every fixture is screened at — the first two of the protocol's four. */
    static final long[] SCREENING_SEEDS = {SensingRaceProtocol.SEEDS[0], SensingRaceProtocol.SEEDS[1]};

    /** The two a {@linkplain #divergent divergent} fixture adds, making up the protocol's four. */
    static final long[] CONFIRMATION_SEEDS = {SensingRaceProtocol.SEEDS[2], SensingRaceProtocol.SEEDS[3]};

    /** The fleet size a capture with no summary is swept at, and flagged as. */
    static final int FALLBACK_WORKERS = SensingRaceProtocol.WORKERS;

    /**
     * Serial fraction above which a leg is flagged <b>collapsed</b>: past half its duration spent
     * running one range at a time, a fleet is not a fleet. A flag rather than a threshold to rank on —
     * what it marks is that the run's distribution is bimodal and its mean describes no run that
     * happened.
     */
    static final double COLLAPSE_SERIAL_FRACTION = 0.5;

    /** Screened {@code CURRENT}-vs-{@code E2} mean duration gap, either way, that earns four seeds. */
    static final double DIVERGENT_DURATION_DELTA = 0.25;

    /** Screened {@code CURRENT} steal attempts finding no victim, at any seed, that earns four seeds. */
    static final double DIVERGENT_NO_VICTIM_SHARE = 0.4;

    private static final ObjectMapper JSON = new ObjectMapper();

    private CorpusSweep() {
    }

    /** Where a fixture's fleet size came from — a row swept at the fallback is not comparable. */
    enum FleetSource {
        /** The capture's own {@code max_parallel_listings}. */
        CAPTURE,
        /** {@link #FALLBACK_WORKERS}, because the capture staged no summary. */
        FALLBACK
    }

    /** The fleet one fixture is swept at, and where that number came from. */
    record Fleet(int workers, FleetSource source) {
    }

    /**
     * One row of the sweep: a leg, the fixture it ran on at the fleet and tier it ran under, and what
     * producing it cost. {@code escalated} is the <em>fixture's</em> flag, carried on every one of its
     * rows, so a reader can tell a screened two-seed row from one of four without joining tables.
     */
    record Row(String fixture, long keys, SimStoreBackend backend, Fleet fleet, Duration open,
               SensingRaceProtocol.Leg leg, Duration wall, double heapPeakMb, boolean escalated) {

        boolean collapsed() {
            return leg.serialFraction() > COLLAPSE_SERIAL_FRACTION;
        }
    }

    /**
     * A fixture the sweep did not measure, and everything needed to find it in the corpus. Classified
     * from {@link RowGroupOrderException#reason()} and the exception's type rather than from a message,
     * which is what the typed failure exists for.
     */
    record Exclusion(String fixture, String reason, String where) {
    }

    /**
     * A leg that produced a number nobody may use: it did not finish, or it did not emit the fixture's
     * own key total. Collected rather than thrown so one bad fixture cannot cost the sweep the other
     * hundred — the caller fails on a non-empty list once the sweep is done.
     */
    record Problem(String fixture, String leg, String what) {
    }

    /** What a whole sweep produced. */
    record Result(List<Row> rows, List<Exclusion> exclusions, List<Problem> problems) {
    }

    /**
     * Every fixture directory under {@code root}, in name order — a directory holding a
     * {@link #DATA_DIRECTORY} with at least one Parquet part. Name order because a sweep interrupted
     * halfway and resumed by hand must have covered a prefix of the corpus, not an arbitrary subset.
     */
    static List<Path> fixtures(Path root) throws IOException {
        try (Stream<Path> children = Files.list(root)) {
            return children.filter(Files::isDirectory)
                    .filter(CorpusSweep::hasParquet)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
        }
    }

    private static boolean hasParquet(Path fixture) {
        Path data = fixture.resolve(DATA_DIRECTORY);
        if (!Files.isDirectory(data)) {
            return false;
        }
        try (Stream<Path> parts = Files.list(data)) {
            return parts.anyMatch(part -> part.getFileName().toString().endsWith(".parquet"));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list " + data, e);
        }
    }

    /**
     * The fleet {@code fixture} is swept at: its capture's own {@code max_parallel_listings}, or
     * {@link #FALLBACK_WORKERS} when the capture staged no summary or the summary does not carry the
     * field. Never silently the fallback — the {@link Fleet#source()} it returns is a column.
     */
    static Fleet fleetOf(Path fixture) throws IOException {
        Path summary = fixture.resolve(SUMMARY_FILE);
        if (!Files.isRegularFile(summary)) {
            return new Fleet(FALLBACK_WORKERS, FleetSource.FALLBACK);
        }
        JsonNode workers;
        try (var in = Files.newInputStream(summary)) {
            workers = JSON.readTree(in).path("config").path(MAX_PARALLEL_LISTINGS);
        }
        if (!workers.isIntegralNumber() || workers.asInt() < 1) {
            return new Fleet(FALLBACK_WORKERS, FleetSource.FALLBACK);
        }
        return new Fleet(workers.asInt(), FleetSource.CAPTURE);
    }

    /**
     * The three readings of one screened leg that decide whether its fixture earns the other two seeds
     * — the escalation rule is about these, not about a whole run record, and stating them as a value
     * is what lets the rule be exercised at readings a two-fixture test cannot provoke.
     */
    record Screen(String arm, double serialFraction, double durationNanos, double noVictimShare) {

        static Screen of(SensingRaceProtocol.Leg leg) {
            return new Screen(leg.variant(), leg.serialFraction(), leg.result().virtualNanos(),
                    leg.noVictimShare());
        }
    }

    /**
     * Whether a fixture's two-seed screen has earned the other two seeds. Any one of three readings is
     * enough, and they are deliberately different kinds of interesting:
     * <ul>
     *   <li><b>a collapsed leg at any arm or seed</b> — the pathology itself, and the reading a
     *       two-seed screen is least able to trust, since it is bimodal at a fixed configuration;</li>
     *   <li><b>a mean {@code CURRENT}-vs-{@code E2} duration gap either way</b> — a cure worth
     *       confirming, and equally a <em>regression</em> worth confirming;</li>
     *   <li><b>{@code CURRENT} steal attempts mostly finding no victim, at either seed</b> — the
     *       mechanism upstream of a collapse, which can be present at a seed where the duration is
     *       not yet.</li>
     * </ul>
     */
    static boolean divergent(List<Screen> screening) {
        for (Screen screen : screening) {
            if (screen.serialFraction() > COLLAPSE_SERIAL_FRACTION) {
                return true;
            }
        }
        double current = meanDurationNanos(screening, SensingVariant.CURRENT);
        double anchored = meanDurationNanos(screening, SensingVariant.CURSOR_ANCHORED);
        if (current > 0 && Math.abs(current - anchored) / current > DIVERGENT_DURATION_DELTA) {
            return true;
        }
        for (Screen screen : screening) {
            if (screen.arm().equals(SensingRaceProtocol.label(SensingVariant.CURRENT))
                    && screen.noVictimShare() > DIVERGENT_NO_VICTIM_SHARE) {
                return true;
            }
        }
        return false;
    }

    private static double meanDurationNanos(List<Screen> screening, SensingVariant variant) {
        String arm = SensingRaceProtocol.label(variant);
        return screening.stream()
                .filter(screen -> screen.arm().equals(arm))
                .mapToDouble(Screen::durationNanos)
                .average().orElse(0);
    }

    /**
     * Sweeps every fixture under {@code root}, appending each fixture's rows to {@code results} as they
     * are produced and printing its table as it goes.
     */
    static Result sweep(Path root, Path results) throws IOException {
        List<Path> fixtures = fixtures(root);
        List<Row> rows = new ArrayList<>();
        List<Exclusion> exclusions = new ArrayList<>();
        List<Problem> problems = new ArrayList<>();
        Instant sweepStarted = Instant.now();
        try (BufferedWriter out = Files.newBufferedWriter(results, StandardCharsets.UTF_8)) {
            out.write(HEADER);
            out.newLine();
            out.flush();
            for (int i = 0; i < fixtures.size(); i++) {
                Path fixture = fixtures.get(i);
                String name = fixture.getFileName().toString();
                System.out.printf(Locale.ROOT, "corpus_sweep phase=fixture index=%d of=%d fixture=%s%n",
                        i + 1, fixtures.size(), name);
                List<Row> swept;
                try {
                    swept = sweepOne(fixture, name, problems);
                } catch (RowGroupOrderException disordered) {
                    exclusions.add(excluded(name, disordered));
                    continue;
                } catch (RuntimeException refused) {
                    exclusions.add(new Exclusion(name, refused.getClass().getSimpleName(),
                            String.valueOf(refused.getMessage())));
                    System.out.printf(Locale.ROOT, "corpus_sweep phase=excluded fixture=%s reason=%s %s%n",
                            name, refused.getClass().getSimpleName(), refused.getMessage());
                    continue;
                }
                rows.addAll(swept);
                for (Row row : swept) {
                    out.write(row(row));
                    out.newLine();
                }
                out.flush();
                SensingRaceProtocol.printTable("corpus sweep — " + name,
                        swept.stream().map(Row::leg).toList());
            }
        }
        System.out.printf(Locale.ROOT,
                "corpus_sweep phase=done fixtures=%d swept=%d excluded=%d legs=%d problems=%d wall_s=%.1f%n",
                fixtures.size(), fixtures.size() - exclusions.size(), exclusions.size(), rows.size(),
                problems.size(), Duration.between(sweepStarted, Instant.now()).toMillis() / 1000.0);
        for (Exclusion exclusion : exclusions) {
            System.out.printf(Locale.ROOT, "corpus_sweep phase=exclusion fixture=%s reason=%s where=%s%n",
                    exclusion.fixture(), exclusion.reason(), exclusion.where());
        }
        return new Result(List.copyOf(rows), List.copyOf(exclusions), List.copyOf(problems));
    }

    private static Exclusion excluded(String fixture, RowGroupOrderException disordered) {
        String where = String.format(Locale.ROOT, "file=%s row_group=%d row=%d", disordered.file(),
                disordered.rowGroup(), disordered.row());
        System.out.printf(Locale.ROOT, "corpus_sweep phase=excluded fixture=%s reason=%s %s%n",
                fixture, disordered.reason(), where);
        return new Exclusion(fixture, disordered.reason(), where);
    }

    /**
     * One fixture: open it once, screen the arms at two seeds, add the other two if the screen
     * diverged, close it. The handle is closed here and not held across fixtures — the streaming tier's
     * decoded segments are off-heap and bounded per handle, so a sweep that kept every fixture open
     * would hold the whole corpus's working set at once.
     */
    private static List<Row> sweepOne(Path fixture, String name, List<Problem> problems) throws IOException {
        Fleet fleet = fleetOf(fixture);
        Instant openStarted = Instant.now();
        SimStoreFactory.Result opened = SimStoreFactory.open(fixture.resolve(DATA_DIRECTORY),
                SimStoreBackend.STREAMING, SimStoreConfig.fromSystemProperties());
        Duration open = Duration.between(openStarted, Instant.now());
        long keys = opened.keyCount().orElse(-1);
        System.out.printf(Locale.ROOT,
                "corpus_sweep phase=open fixture=%s backend=%s keys=%d workers=%d fleet_source=%s open_ms=%d%n",
                name, opened.resolvedBackend(), keys, fleet.workers(), fleet.source(), open.toMillis());
        try (ListingStore store = opened.store()) {
            String label = "corpus fixture (" + opened.resolvedBackend() + ")";
            List<Measured> measured =
                    legs(store, label, fleet.workers(), SCREENING_SEEDS, name, opened.keyCount(), problems);
            boolean escalated = divergent(measured.stream().map(leg -> Screen.of(leg.leg())).toList());
            if (escalated) {
                measured = new ArrayList<>(measured);
                measured.addAll(legs(store, label, fleet.workers(), CONFIRMATION_SEEDS, name,
                        opened.keyCount(), problems));
            }
            List<Row> rows = new ArrayList<>(measured.size());
            for (Measured leg : measured) {
                rows.add(new Row(name, keys, opened.resolvedBackend(), fleet, open, leg.leg(), leg.wall(),
                        leg.heapPeakMb(), escalated));
            }
            return rows;
        }
    }

    /** One leg and what producing it cost — the wall clock and heap mark the leg itself does not carry. */
    private record Measured(SensingRaceProtocol.Leg leg, Duration wall, double heapPeakMb) {
    }

    /**
     * Every arm at every one of {@code seeds}, against one open handle, at the measured page regime and
     * the composite measured client cost — the configuration every other real-listing number in this
     * campaign was taken at.
     */
    private static List<Measured> legs(ListingStore store, String label, int workers, long[] seeds,
                                       String fixture, OptionalLong keys, List<Problem> problems) {
        List<Measured> measured = new ArrayList<>();
        for (SensingVariant arm : ARMS) {
            for (long seed : seeds) {
                PolicyScenario scenario = PolicyRunFixtures
                        .scenario(workers, PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                                PolicyRunFixtures.MEASURED_TAIL_LATENCY, PolicyRunFixtures.measuredCost())
                        .withSeed(seed);
                HeapPeak.reset();
                Instant startedAt = Instant.now();
                PolicyRunResult result = SimExecutor.run(scenario, store, label, arm);
                Duration wall = Duration.between(startedAt, Instant.now());
                double heapPeakMb = HeapPeak.peakMb();
                String leg = SensingRaceProtocol.label(arm) + "/seed " + seed;
                if (!result.completed()) {
                    problems.add(new Problem(fixture, leg, "did not complete: " + result.stopReason()));
                } else if (keys.isPresent() && result.keysEmitted() != keys.getAsLong()) {
                    problems.add(new Problem(fixture, leg, "emitted " + result.keysEmitted() + " of "
                            + keys.getAsLong() + " keys"));
                }
                measured.add(new Measured(new SensingRaceProtocol.Leg(SensingRaceProtocol.label(arm),
                        fixture, seed, PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE, result),
                        wall, heapPeakMb));
            }
        }
        return measured;
    }

    // ---- the results file ----------------------------------------------------------------

    /** The columns, tab separated — one row per leg. */
    static final String HEADER = String.join("\t", "fixture", "keys", "backend", "workers",
            "fleet_source", "open_ms", "arm", "seed", "page", "completed", "serial", "tail", "est_zero",
            "est_ignores", "reval_loss", "owner_children", "thief_children", "steal_attempts",
            "no_victim", "occupancy", "store_calls", "duration_s", "wall_ms", "events", "heap_peak_mb",
            "escalated", "collapsed");

    /** One {@link Row} under {@link #HEADER}. */
    static String row(Row row) {
        SensingRaceProtocol.Leg leg = row.leg();
        PolicyRunResult result = leg.result();
        return String.join("\t", row.fixture(), Long.toString(row.keys()), row.backend().toString(),
                Integer.toString(row.fleet().workers()), row.fleet().source().toString().toLowerCase(Locale.ROOT),
                Long.toString(row.open().toMillis()), leg.variant(), Long.toString(leg.seed()),
                Integer.toString(leg.pageSize()), Boolean.toString(result.completed()),
                decimal(leg.serialFraction()), decimal(leg.tailFraction()), decimal(leg.estZeroShare()),
                decimal(leg.estIgnoresKeysShare()), decimal(leg.revalidationLossShare()),
                Long.toString(result.ownerSplitChildren()), Long.toString(result.thiefChildren()),
                Long.toString(leg.stealAttempts()), decimal(leg.noVictimShare()),
                String.format(Locale.ROOT, "%.3f", result.timeline().meanOccupancy()),
                Long.toString(result.storeCalls()),
                String.format(Locale.ROOT, "%.3f", result.virtualNanos() / 1e9),
                Long.toString(row.wall().toMillis()), Long.toString(result.run().eventsProcessed()),
                String.format(Locale.ROOT, "%.1f", row.heapPeakMb()),
                Boolean.toString(row.escalated()), Boolean.toString(row.collapsed()));
    }

    private static String decimal(double value) {
        return String.format(Locale.ROOT, "%.4f", value);
    }
}
