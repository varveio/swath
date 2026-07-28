/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.sim.store.IneligibleFixtureException;
import io.varve.swath.sim.store.SimStoreBackend;
import io.varve.swath.sim.store.SimStoreConfig;
import io.varve.swath.sim.store.SimStoreFactory;
import io.varve.swath.sort.RowGroupOrderException;
import io.varve.swath.sort.SortedFileIndex;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

/**
 * <b>The same four sensing arms over a whole corpus of captured listings, in one JVM.</b>
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
 * <p>A directory the sweep passes over is <b>named and reasoned in the output</b> ({@link Skipped}),
 * never silently dropped: a corpus is staged by hand, and a sweep whose covering set is invisible is a
 * sweep whose missing bucket looks exactly like a bucket with nothing to say. Two things are passed
 * over — a directory holding no {@code data/*.parquet} (not a staged capture at all), and a capture
 * whose footers declare more rows than {@link #MAX_KEYS_PROPERTY} allows. The ceiling exists because a
 * corpus directory outlives the tier that was staged into it: a leftover fixture an order of magnitude
 * larger than the rest would be swept at the fallback fleet, for hours, and its rows would not be
 * comparable with anything. Reading it costs one footer per part and no decode
 * ({@link SortedFileIndex#rowCount}).
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
 *       <b>says so in its row</b>, because that row is then not comparable with the rest. The same
 *       goes for two captures that ran at <em>different</em> fleets, which is why the sweep closes by
 *       printing the fleets it actually ran at and naming every fixture off the corpus's modal one
 *       ({@link #fleets}) — comparability is a reading of the sweep, not an assumption of it.</li>
 *   <li><b>Two screening seeds, four where it matters.</b> Four seeds per arm per fixture is the
 *       verdict standard ({@link SensingRaceProtocol}), and a corpus of it costs twice what a screen
 *       does for fixtures that are uninteresting at both seeds. So the sweep screens at two and
 *       escalates to four on any fixture that shows {@linkplain #divergent divergence} — and the
 *       {@code escalated} column says which, so no two-seed row can be quoted as a verdict.</li>
 * </ol>
 *
 * <h2>A fixture that cannot be read is data, not a failure — and nothing else is</h2>
 * The streaming tier refuses a capture whose keys are not in order in two typed shapes, and the sweep
 * files exactly those two as exclusions: at index derive, where disorder across row groups (or any
 * other eligibility defect) refuses the whole file set ({@link IneligibleFixtureException}, which
 * carries the reason and the files), and at the first row group a run faults in
 * ({@link RowGroupOrderException}, which carries the file, the row group and the row). Either way the
 * sweep records the exclusion with everything the report needs to locate it in the corpus and
 * continues to the next fixture: the exclusion list is an output of the sweep — corpus QA the operator
 * wants regardless — and one unreadable capture must not cost the other hundred.
 *
 * <p><b>Any other runtime failure is this program being wrong, and is filed as a {@link Problem}</b>
 * — which fails the run. Catching it as an exclusion instead would turn every bug in the sweep, and
 * every misconfigured invocation of it, into a quiet corpus-QA data point: a table with fifty fixtures
 * missing and a plausible story about why.
 */
final class CorpusSweep {

    /** The subdirectory of a staged capture holding its Parquet, and so the path a store opens. */
    static final String DATA_DIRECTORY = "data";

    /** The capture's own run record, sibling of {@link #DATA_DIRECTORY} — see {@link #fleetOf}. */
    static final String SUMMARY_FILE = "summary.json";

    /** The concurrency a capture ran at, in that summary. */
    static final String MAX_PARALLEL_LISTINGS = "max_parallel_listings";

    /**
     * The four arms: the shipped sensor, the two the sensing race left standing, and the carve-admission
     * candidate that round left standing in turn — the combination with the geometry band's lower half
     * removed. It is swept beside its own incumbent rather than in place of it: what a promotion turns
     * on is the head-to-head over the same corpus at the same seeds, and an arm the sweep dropped is an
     * arm every later comparison has to re-run to get back.
     */
    static final List<SensingVariant> ARMS = List.of(
            SensingVariant.CURRENT, SensingVariant.CURSOR_ANCHORED, SensingVariant.RATE_CURSOR_ANCHORED,
            SensingVariant.RATE_ANCHORED_LIFT_ONLY);

    /**
     * The arms the screen reads <em>against</em> {@link SensingVariant#CURRENT} — every one but the
     * shipped sensor. Every one, not the first: a fixture the sweep is quiet about is one nobody
     * looks at again, so a divergence only the last candidate shows must earn the same four seeds
     * the first candidate's would. Screening against one arm and promoting on all of them is how a
     * corpus of arms-in-agreement gets asserted from a corpus that was never asked.
     */
    static final List<SensingVariant> CANDIDATE_ARMS =
            ARMS.stream().filter(arm -> arm != SensingVariant.CURRENT).toList();

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

    /**
     * Screened {@code CURRENT}-vs-candidate duration gap, either way, that earns four seeds — read
     * both over the screening seeds' mean and at each seed on its own; see {@link #divergent}.
     */
    static final double DIVERGENT_DURATION_DELTA = 0.25;

    /** Screened steal attempts finding no victim, at any arm and any seed, that earns four seeds. */
    static final double DIVERGENT_NO_VICTIM_SHARE = 0.4;

    /**
     * System property capping the fixture size the sweep will open, in rows the capture's footers
     * declare. Default {@link #DEFAULT_MAX_KEYS}.
     */
    static final String MAX_KEYS_PROPERTY = "swath.sim.listing.corpus-max-keys";

    /**
     * The default ceiling: twenty million keys, an order of magnitude above the largest capture a
     * comparable corpus tier holds and well below the size at which one fixture's legs cost more than
     * the other hundred put together.
     */
    static final long DEFAULT_MAX_KEYS = 20_000_000L;

    /** {@link Skipped#reason()}: the directory holds no {@code data/*.parquet}, so it is no capture. */
    static final String NOT_A_CAPTURE = "not_a_capture";

    /** {@link Skipped#reason()}: the capture's footers declare more rows than the ceiling allows. */
    static final String OVER_KEY_CEILING = "over_key_ceiling";

    /** {@link Problem#leg()} for a failure that is the whole fixture's rather than any one leg's. */
    static final String WHOLE_FIXTURE = "fixture";

    /** Every seed — the screen's own, when a reading is taken over all of them rather than one. */
    private static final LongPredicate ANY_SEED = seed -> true;

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
     * A fixture the sweep opened and could not read, and everything needed to find it in the corpus.
     * Classified from the exception's type and its own {@code reason()} rather than from a message,
     * which is what the typed failures exist for. {@code where} is the failure's <b>redacted</b>
     * report — file names, no directories: this record is quoted into a report, and the operator's
     * filesystem layout is not part of the finding. The full path stays in the console log.
     */
    record Exclusion(String fixture, String reason, String where) {
    }

    /**
     * A directory under the root the sweep never opened, and why — see the class javadoc. Carried in
     * the {@link Result} rather than only printed, because "which fixtures did this sweep actually
     * cover" is a question its own output has to answer.
     */
    record Skipped(String fixture, String reason, String detail) {
    }

    /** What the root holds: the fixtures to sweep, in name order, and the directories passed over. */
    record Corpus(List<Path> fixtures, List<Skipped> skipped) {
    }

    /**
     * A leg that produced a number nobody may use: it did not finish, or it did not emit the fixture's
     * own key total. Collected rather than thrown so one bad fixture cannot cost the sweep the other
     * hundred — the caller fails on a non-empty list once the sweep is done.
     */
    record Problem(String fixture, String leg, String what) {
    }

    /**
     * Which arms a sweep races and whether it screens before confirming — the two choices that decide
     * what a sweep <em>is</em>, rather than what it finds.
     *
     * <p>{@link #SCREEN} is the corpus sweep's own: {@link #ARMS}, two seeds, four where the screen
     * diverges. A round convened to resolve verdicts rather than to find them sets
     * {@code confirmEverySeed}, and then no row it writes is a screen — which is what lets its
     * {@code escalated} column be read as "every leg here is one of four" instead of as a filter.
     *
     * @param arms             the arms to race, in table order, the control included
     * @param confirmEverySeed run all four of {@code SensingRaceProtocol.SEEDS} on every fixture,
     *                         skipping the screening tier and its divergence rule entirely
     */
    record Race(List<SensingVariant> arms, boolean confirmEverySeed) {

        Race {
            arms = List.copyOf(arms);
        }
    }

    /** The corpus sweep's own race: every sensing arm, screened at two seeds and escalated. */
    static final Race SCREEN = new Race(ARMS, false);

    /** What a whole sweep produced. */
    record Result(List<Row> rows, List<Exclusion> exclusions, List<Skipped> skipped,
                  List<Problem> problems) {
    }

    /**
     * Every directory under {@code root}, split into the fixtures the sweep will open — a directory
     * holding a {@link #DATA_DIRECTORY} with at least one Parquet part, whose footers declare no more
     * rows than the ceiling — and the ones it will not, each with its reason. Name order because a
     * sweep interrupted halfway and resumed by hand must have covered a prefix of the corpus, not an
     * arbitrary subset.
     */
    static Corpus fixtures(Path root) throws IOException {
        long ceiling = maxKeys();
        List<Path> children;
        try (Stream<Path> stream = Files.list(root)) {
            children = stream.filter(Files::isDirectory)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
        }
        List<Path> fixtures = new ArrayList<>();
        List<Skipped> skipped = new ArrayList<>();
        for (Path child : children) {
            String name = child.getFileName().toString();
            List<Path> parts = parquetParts(child);
            if (parts.isEmpty()) {
                skipped.add(new Skipped(name, NOT_A_CAPTURE, "no " + DATA_DIRECTORY + "/*.parquet"));
                continue;
            }
            long rows = 0;
            for (Path part : parts) {
                rows += SortedFileIndex.rowCount(part);
            }
            if (rows > ceiling) {
                skipped.add(new Skipped(name, OVER_KEY_CEILING, rows + " rows above the " + ceiling
                        + "-row ceiling (-D" + MAX_KEYS_PROPERTY + ")"));
                continue;
            }
            fixtures.add(child);
        }
        return new Corpus(List.copyOf(fixtures), List.copyOf(skipped));
    }

    /** The Parquet parts of a staged capture, in key order, or empty when the directory holds none. */
    private static List<Path> parquetParts(Path fixture) throws IOException {
        Path data = fixture.resolve(DATA_DIRECTORY);
        return Files.isDirectory(data) ? SortedFixtures.resolveFiles(data) : List.of();
    }

    /**
     * {@link #MAX_KEYS_PROPERTY}, parsed explicitly rather than through {@link Long#getLong}, which
     * answers a malformed value with the default — a corpus silently swept at a ceiling nobody asked
     * for is the exact failure this property exists to prevent.
     */
    private static long maxKeys() {
        String raw = System.getProperty(MAX_KEYS_PROPERTY);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_MAX_KEYS;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("-D" + MAX_KEYS_PROPERTY
                    + " must be a decimal row count, got: " + raw, e);
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
    record Screen(String arm, long seed, double serialFraction, double durationNanos,
                  double noVictimShare) {

        static Screen of(SensingRaceProtocol.Leg leg) {
            return new Screen(leg.variant(), leg.seed(), leg.serialFraction(), leg.result().virtualNanos(),
                    leg.noVictimShare());
        }
    }

    /**
     * Whether a fixture's two-seed screen has earned the other two seeds. Any one of four readings is
     * enough, and they are deliberately different kinds of interesting:
     * <ul>
     *   <li><b>a collapsed leg at any arm or seed</b> — the pathology itself, and the reading a
     *       two-seed screen is least able to trust, since it is bimodal at a fixed configuration;</li>
     *   <li><b>steal attempts mostly finding no victim, at any arm and either seed</b> — the mechanism
     *       upstream of a collapse, which can be present at a seed where the duration is not yet.
     *       Read at <em>every</em> arm and not only the shipped one, because a candidate that starves
     *       its own thieves is a regression the screen exists to catch;</li>
     *   <li><b>a mean {@code CURRENT}-vs-candidate duration gap either way, at any candidate arm</b>
     *       — a cure worth confirming, and equally a <em>regression</em> worth confirming;</li>
     *   <li><b>the same gap at a <em>single</em> screening seed</b>. Not redundant with the mean, and
     *       this is the reading a mean is structurally worst at: what a collapse-prone fixture does is
     *       bimodal, so one leg 55% down and one leg level average to a 23% gap that clears no
     *       threshold — the fixture then reads as uninteresting precisely because it is unstable. A
     *       mean over two seeds is a summary of two numbers; the sweep has both.</li>
     * </ul>
     */
    static boolean divergent(List<Screen> screening) {
        for (Screen screen : screening) {
            if (screen.serialFraction() > COLLAPSE_SERIAL_FRACTION
                    || screen.noVictimShare() > DIVERGENT_NO_VICTIM_SHARE) {
                return true;
            }
        }
        long[] seeds = screening.stream().mapToLong(Screen::seed).distinct().toArray();
        for (SensingVariant candidate : CANDIDATE_ARMS) {
            if (gap(screening, candidate, ANY_SEED) > DIVERGENT_DURATION_DELTA) {
                return true;
            }
            for (long seed : seeds) {
                if (gap(screening, candidate, screened -> screened == seed) > DIVERGENT_DURATION_DELTA) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * How far {@code candidate} sits from {@link SensingVariant#CURRENT} over {@code seeds}, as a
     * fraction of the shipped sensor's own duration — <b>0 when either side has no reading there</b>,
     * because an arm that did not run is not a divergence, and an absent arm read as a zero duration
     * would escalate every fixture in the corpus.
     */
    private static double gap(List<Screen> screening, SensingVariant candidate, LongPredicate seeds) {
        OptionalDouble current = meanDurationNanos(screening, SensingVariant.CURRENT, seeds);
        OptionalDouble measured = meanDurationNanos(screening, candidate, seeds);
        if (current.isEmpty() || measured.isEmpty() || current.getAsDouble() <= 0) {
            return 0;
        }
        return Math.abs(current.getAsDouble() - measured.getAsDouble()) / current.getAsDouble();
    }

    private static OptionalDouble meanDurationNanos(List<Screen> screening, SensingVariant variant,
                                                    LongPredicate seeds) {
        String arm = SensingRaceProtocol.label(variant);
        return screening.stream()
                .filter(screen -> screen.arm().equals(arm) && seeds.test(screen.seed()))
                .mapToDouble(Screen::durationNanos)
                .average();
    }

    /**
     * Sweeps every fixture under {@code root}, appending each fixture's rows to {@code results} as they
     * are produced and printing its table as it goes. {@code results} must not already exist: the file
     * is the run's only durable record, and a sweep that truncated the one before it would destroy the
     * raw data a published finding cites.
     */
    static Result sweep(Path root, Path results) throws IOException {
        return sweep(root, results, SCREEN);
    }

    /** @see #sweep(Path, Path) */
    static Result sweep(Path root, Path results, Race race) throws IOException {
        Corpus corpus = fixtures(root);
        List<Path> fixtures = corpus.fixtures();
        for (Skipped skipped : corpus.skipped()) {
            System.out.printf(Locale.ROOT, "corpus_sweep phase=skipped fixture=%s reason=%s detail=%s%n",
                    skipped.fixture(), skipped.reason(), skipped.detail());
        }
        List<Row> rows = new ArrayList<>();
        List<Exclusion> exclusions = new ArrayList<>();
        List<Problem> problems = new ArrayList<>();
        Instant sweepStarted = Instant.now();
        try (BufferedWriter out = Files.newBufferedWriter(results, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
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
                    swept = sweepOne(fixture, name, race, problems);
                } catch (RuntimeException failure) {
                    Optional<Exclusion> refused = refusal(name, failure);
                    if (refused.isEmpty()) {
                        problems.add(new Problem(name, WHOLE_FIXTURE, "failed: " + failure));
                        System.out.printf(Locale.ROOT, "corpus_sweep phase=failed fixture=%s %s%n",
                                name, failure);
                        continue;
                    }
                    // The legs that ran before the refusal reached its first bad row group belong to a
                    // fixture the sweep is now not measuring at all, so whatever they reported is a
                    // symptom of the exclusion rather than a defect the run must fail on.
                    problems.removeIf(problem -> problem.fixture().equals(name));
                    exclusions.add(refused.get());
                    System.out.printf(Locale.ROOT, "corpus_sweep phase=excluded fixture=%s reason=%s %s%n",
                            name, refused.get().reason(), failure.getMessage());
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
        System.out.printf(Locale.ROOT, "corpus_sweep phase=done fixtures=%d swept=%d excluded=%d "
                        + "skipped=%d legs=%d problems=%d wall_s=%.1f%n",
                fixtures.size(), rows.stream().map(Row::fixture).distinct().count(), exclusions.size(),
                corpus.skipped().size(), rows.size(), problems.size(),
                Duration.between(sweepStarted, Instant.now()).toMillis() / 1000.0);
        for (Exclusion exclusion : exclusions) {
            System.out.printf(Locale.ROOT, "corpus_sweep phase=exclusion fixture=%s reason=%s where=%s%n",
                    exclusion.fixture(), exclusion.reason(), exclusion.where());
        }
        printFleets(rows);
        return new Result(List.copyOf(rows), List.copyOf(exclusions), corpus.skipped(),
                List.copyOf(problems));
    }

    /**
     * The exclusion {@code failure} is, or empty when it is not one. Only the two typed refusals the
     * streaming tier raises are corpus data; a bare {@link IllegalStateException} or
     * {@link IllegalArgumentException} — a bug here, a malformed {@code swath.sim.*} property — is
     * this program failing, and filing it as an exclusion would publish it as a fact about a bucket.
     */
    static Optional<Exclusion> refusal(String fixture, RuntimeException failure) {
        return switch (failure) {
            case RowGroupOrderException disordered ->
                    Optional.of(new Exclusion(fixture, disordered.reason(), disordered.redactedMessage()));
            case IneligibleFixtureException ineligible ->
                    Optional.of(new Exclusion(fixture, ineligible.reason(), ineligible.redactedMessage()));
            default -> Optional.empty();
        };
    }

    /**
     * The fixtures the sweep measured, by the fleet it measured them at — see the class javadoc's
     * second choice. A corpus swept at one fleet compares across itself; one swept at several does
     * not, and this is the reading that says which of the two the operator has.
     */
    static Map<Integer, List<String>> fleets(List<Row> rows) {
        Map<Integer, List<String>> byFleet = new TreeMap<>();
        for (Row row : rows) {
            List<String> fixtures = byFleet.computeIfAbsent(row.fleet().workers(), workers -> new ArrayList<>());
            if (!fixtures.contains(row.fixture())) {
                fixtures.add(row.fixture());
            }
        }
        return byFleet;
    }

    private static void printFleets(List<Row> rows) {
        Map<Integer, List<String>> byFleet = fleets(rows);
        if (byFleet.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, List<String>> fleet : byFleet.entrySet()) {
            System.out.printf(Locale.ROOT, "corpus_sweep phase=fleet workers=%d fixtures=%d%n",
                    fleet.getKey(), fleet.getValue().size());
        }
        int modal = byFleet.entrySet().stream()
                .max(Comparator.comparingInt(fleet -> fleet.getValue().size()))
                .orElseThrow().getKey();
        for (Map.Entry<Integer, List<String>> fleet : byFleet.entrySet()) {
            if (fleet.getKey() == modal) {
                continue;
            }
            for (String fixture : fleet.getValue()) {
                System.out.printf(Locale.ROOT,
                        "corpus_sweep phase=fleet_mismatch fixture=%s workers=%d modal=%d%n",
                        fixture, fleet.getKey(), modal);
            }
        }
    }

    /**
     * One fixture: open it once, screen the arms at two seeds, add the other two if the screen
     * diverged, close it. The handle is closed here and not held across fixtures — the streaming tier's
     * decoded segments are off-heap and bounded per handle, so a sweep that kept every fixture open
     * would hold the whole corpus's working set at once.
     */
    private static List<Row> sweepOne(Path fixture, String name, Race race, List<Problem> problems)
            throws IOException {
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
            List<Measured> measured = legs(store, label, fleet.workers(),
                    race.confirmEverySeed() ? SensingRaceProtocol.SEEDS : SCREENING_SEEDS, race.arms(),
                    name, opened.keyCount(), problems);
            boolean escalated = race.confirmEverySeed()
                    || divergent(measured.stream().map(leg -> Screen.of(leg.leg())).toList());
            if (escalated && !race.confirmEverySeed()) {
                measured = new ArrayList<>(measured);
                measured.addAll(legs(store, label, fleet.workers(), CONFIRMATION_SEEDS, race.arms(), name,
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
                                       List<SensingVariant> arms, String fixture, OptionalLong keys,
                                       List<Problem> problems) {
        List<Measured> measured = new ArrayList<>();
        for (SensingVariant arm : arms) {
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
