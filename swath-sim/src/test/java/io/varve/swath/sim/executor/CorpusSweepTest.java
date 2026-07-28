/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.sim.store.SimStoreBackend;
import io.varve.swath.sim.store.SimStoreConfig;
import io.varve.swath.sort.RowGroupOrderException;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedParquetWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The sweep's own mechanics, over fixtures small enough to be written here — because a corpus sweep is
 * a program whose inputs are somebody else's machine, and every one of the ways it can be wrong
 * produces a plausible table rather than an error: a fixture skipped, a fleet size silently defaulted,
 * a refused capture taking the other hundred down with it, a two-seed screen quoted as a verdict.
 *
 * <p>The keyspaces here are a handful of keys; nothing about their <em>numbers</em> is under test and
 * nothing here asserts one. What is under test is that the loop covers what it should, records where
 * each input came from, survives a fixture it cannot read, and writes a file a reader can parse.
 */
class CorpusSweepTest {

    /** The two seeds a screen is taken at — named here only so a screen can be paired across arms. */
    private static final long SEED_A = CorpusSweep.SCREENING_SEEDS[0];

    private static final long SEED_B = CorpusSweep.SCREENING_SEEDS[1];

    @TempDir
    private Path root;

    @Test
    void everyFixtureUnderTheRootIsSweptAndEveryDirectoryPassedOverIsNamedWithItsReason()
            throws IOException {
        fixture("beta", capture(64), keys(40));
        fixture("alpha", capture(64), keys(30));
        Files.createDirectory(root.resolve("not-a-capture"));
        Files.createDirectories(root.resolve("empty-capture").resolve(CorpusSweep.DATA_DIRECTORY));

        CorpusSweep.Result swept = CorpusSweep.sweep(root, root.resolve("results.tsv"));

        assertThat(swept.exclusions()).isEmpty();
        assertThat(swept.problems()).isEmpty();
        assertThat(swept.rows()).extracting(CorpusSweep.Row::fixture)
                .as("both captures swept, in name order, and neither non-capture directory")
                .startsWith("alpha").endsWith("beta").containsOnly("alpha", "beta");
        assertThat(rowsOf(swept, "alpha")).extracting(row -> row.leg().result().keysEmitted())
                .as("every leg emitted its own fixture's keys, not the other's").containsOnly(30L);
        assertThat(rowsOf(swept, "beta")).extracting(row -> row.leg().result().keysEmitted())
                .containsOnly(40L);
        assertThat(swept.skipped())
                .as("a directory the sweep passed over is an output, not a silent omission — a corpus "
                        + "is staged by hand and a missing bucket reads exactly like a quiet one")
                .extracting(CorpusSweep.Skipped::fixture, CorpusSweep.Skipped::reason)
                .containsExactly(tuple("empty-capture", CorpusSweep.NOT_A_CAPTURE),
                        tuple("not-a-capture", CorpusSweep.NOT_A_CAPTURE));
    }

    /**
     * The ceiling, which exists because a corpus directory outlives the tier staged into it: a
     * leftover fixture an order of magnitude larger than the rest would be swept at whatever fleet it
     * could name, for as long as it took, and its rows would be comparable with nothing. It is read
     * from the footers, so a fixture over the line costs no decode at all.
     */
    @Test
    void aCaptureOverTheKeyCeilingIsNeverOpenedAndSaysSoInTheOutput() throws IOException {
        fixture("small", capture(4), keys(20));
        fixture("huge", capture(4), keys(60));
        System.setProperty(CorpusSweep.MAX_KEYS_PROPERTY, "40");
        try {
            CorpusSweep.Result swept = CorpusSweep.sweep(root, root.resolve("results.tsv"));

            assertThat(swept.rows()).extracting(CorpusSweep.Row::fixture).containsOnly("small");
            assertThat(swept.skipped()).singleElement().satisfies(skipped -> {
                assertThat(skipped.fixture()).isEqualTo("huge");
                assertThat(skipped.reason()).isEqualTo(CorpusSweep.OVER_KEY_CEILING);
                assertThat(skipped.detail()).contains("60").contains("40");
            });
            assertThat(swept.exclusions()).as("over the ceiling is not unreadable").isEmpty();
            assertThat(swept.problems()).isEmpty();
        } finally {
            System.clearProperty(CorpusSweep.MAX_KEYS_PROPERTY);
        }
    }

    /**
     * The results file is the run's only durable record — the per-fixture tables scroll past — so a
     * re-run that pointed at the same path used to silently destroy the raw data an already-published
     * finding cites.
     */
    @Test
    void aSweepRefusesToWriteOverResultsThatAlreadyExist() throws IOException {
        fixture("one", capture(4), keys(20));
        Path results = root.resolve("results.tsv");
        CorpusSweep.sweep(root, results);
        long written = Files.size(results);

        assertThatThrownBy(() -> CorpusSweep.sweep(root, results))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThat(Files.size(results)).as("the earlier sweep's rows are still there").isEqualTo(written);
    }

    @Test
    void everyArmRunsAtEveryScreeningSeedAndOnlyAtFourWhenTheScreenDiverged() throws IOException {
        fixture("one", capture(4), keys(60));

        CorpusSweep.Result swept = CorpusSweep.sweep(root, root.resolve("results.tsv"));

        boolean escalated = swept.rows().getFirst().escalated();
        int seeds = escalated
                ? CorpusSweep.SCREENING_SEEDS.length + CorpusSweep.CONFIRMATION_SEEDS.length
                : CorpusSweep.SCREENING_SEEDS.length;
        assertThat(swept.rows()).hasSize(CorpusSweep.ARMS.size() * seeds);
        assertThat(swept.rows()).allSatisfy(row -> assertThat(row.escalated())
                .as("the escalation flag is the fixture's, carried on all of its rows").isEqualTo(escalated));
        assertThat(swept.rows()).extracting(row -> row.leg().variant())
                .containsAll(CorpusSweep.ARMS.stream().map(SensingRaceProtocol::label).toList());
        assertThat(swept.rows()).extracting(row -> row.leg().seed())
                .contains(CorpusSweep.SCREENING_SEEDS[0], CorpusSweep.SCREENING_SEEDS[1]);
    }

    /**
     * A round convened to resolve verdicts asks for the arms it names, at all four seeds, and never
     * screens — so its rows are four per arm whatever the divergence rule would have said about them,
     * and its {@code escalated} column reads true for the same reason.
     */
    @Test
    void aRaceThatConfirmsEverySeedRunsItsOwnArmsAtFourWithoutScreening() throws IOException {
        fixture("one", capture(4), keys(60));
        List<SensingVariant> arms =
                List.of(SensingVariant.CURRENT, SensingVariant.RATE_ANCHORED_LIFT_ONLY);

        CorpusSweep.Result raced = CorpusSweep.sweep(root, root.resolve("race.tsv"),
                new CorpusSweep.Race(arms, true));

        assertThat(raced.rows()).hasSize(arms.size() * SensingRaceProtocol.SEEDS.length);
        assertThat(raced.rows()).extracting(row -> row.leg().variant())
                .containsOnly(arms.stream().map(SensingRaceProtocol::label).toArray(String[]::new));
        assertThat(raced.rows()).extracting(row -> row.leg().seed())
                .containsAll(Arrays.stream(SensingRaceProtocol.SEEDS).boxed().toList());
        assertThat(raced.rows()).allSatisfy(row -> assertThat(row.escalated())
                .as("no row of a four-seed race is a screen").isTrue());
    }

    @Test
    void aFixtureIsSweptAtItsOwnCapturesFleetSize() throws IOException {
        Path fixture = fixture("captured", capture(37), keys(20));

        assertThat(CorpusSweep.fleetOf(fixture))
                .isEqualTo(new CorpusSweep.Fleet(37, CorpusSweep.FleetSource.CAPTURE));
    }

    @Test
    void aCaptureWithNoUsableSummaryFallsBackAndSaysSo() throws IOException {
        Path missing = fixture("no-summary", null, keys(20));
        Path noField = fixture("no-field", "{ \"config\" : { \"region\" : \"somewhere\" } }", keys(20));
        Path notANumber = fixture("not-a-number",
                "{ \"config\" : { \"" + CorpusSweep.MAX_PARALLEL_LISTINGS + "\" : \"64\" } }", keys(20));

        CorpusSweep.Fleet fallback =
                new CorpusSweep.Fleet(CorpusSweep.FALLBACK_WORKERS, CorpusSweep.FleetSource.FALLBACK);
        assertThat(CorpusSweep.fleetOf(missing)).isEqualTo(fallback);
        assertThat(CorpusSweep.fleetOf(noField)).isEqualTo(fallback);
        assertThat(CorpusSweep.fleetOf(notANumber))
                .as("a summary that carries the field as something other than a count is not a reading")
                .isEqualTo(fallback);
    }

    /**
     * The sort guard's own consequence for a sweep. The fixture is stamped, complete and
     * sorted-<em>eligible</em> by every check a reader can make from the footer, and disordered inside
     * its one row group — so it is refused where its keys are decoded, mid-run, which is the case that
     * would otherwise abort the whole sweep with a hundred fixtures still to go.
     */
    @Test
    void aFixtureRefusedForDisorderIsRecordedAndTheSweepCarriesOn() throws IOException {
        fixture("aaa-healthy", capture(4), keys(20));
        fixture("bbb-disordered", capture(4), List.of("k/1", "k/3", "k/2", "k/4"));
        fixture("ccc-healthy", capture(4), keys(20));

        CorpusSweep.Result swept = CorpusSweep.sweep(root, root.resolve("results.tsv"));

        assertThat(swept.exclusions()).singleElement().satisfies(excluded -> {
            assertThat(excluded.fixture()).isEqualTo("bbb-disordered");
            assertThat(excluded.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER);
            assertThat(excluded.where()).contains("row group 0").contains("key 2");
            assertThat(excluded.where())
                    .as("the record a report quotes carries the file name, never the operator's tree")
                    .contains("part-00001.parquet").doesNotContain(root.toString());
        });
        assertThat(swept.rows()).extracting(CorpusSweep.Row::fixture)
                .as("the fixtures on either side of the refused one were both measured")
                .contains("aaa-healthy", "ccc-healthy").doesNotContain("bbb-disordered");
        assertThat(swept.problems()).as("a refused capture is corpus data, not an unusable leg").isEmpty();
    }

    /**
     * The <em>other</em> half of the sort guard, and the one no test reached before: disorder that
     * spans row groups is caught a level earlier than a decode, at index derive, so the fixture is
     * refused as ineligible before a single leg runs. That refusal used to be an untyped
     * {@link IllegalArgumentException} naming only the file list, which the sweep filed by class name
     * — so the exclusion row for the two halves of "unsorted" looked like two unrelated corpus facts.
     */
    @Test
    void aFixtureIneligibleAtOpenIsRecordedWithItsEligibilityReasonAndTheSweepCarriesOn()
            throws IOException {
        fixture("aaa-healthy", capture(4), keys(20));
        descendingFixture("bbb-ineligible", capture(4));
        fixture("ccc-healthy", capture(4), keys(20));

        CorpusSweep.Result swept = CorpusSweep.sweep(root, root.resolve("results.tsv"));

        assertThat(swept.exclusions()).singleElement().satisfies(excluded -> {
            assertThat(excluded.fixture()).isEqualTo("bbb-ineligible");
            assertThat(excluded.reason())
                    .as("the eligibility reason itself, not the exception's class name")
                    .isEqualTo(SortedFixtures.SANITY_FAILED);
            assertThat(excluded.where()).contains("part-00001.parquet").doesNotContain(root.toString());
        });
        assertThat(swept.rows()).extracting(CorpusSweep.Row::fixture)
                .contains("aaa-healthy", "ccc-healthy").doesNotContain("bbb-ineligible");
        assertThat(swept.problems()).isEmpty();
    }

    /**
     * Everything that is not one of those two typed refusals. The sweep used to catch
     * {@code RuntimeException} outright and file whatever it caught as an exclusion, which made every
     * bug in the sweep — and every misconfigured invocation of it, as here — indistinguishable from a
     * corpus of unreadable captures: a table with fixtures missing and a plausible story about why.
     */
    @Test
    void aRuntimeFailureThatIsNotARefusalFailsTheRunInsteadOfBecomingCorpusData() throws IOException {
        fixture("one", capture(4), keys(20));
        System.setProperty(SimStoreConfig.STREAMING_MAX_RESIDENT_BYTES_PROPERTY, "0");
        try {
            CorpusSweep.Result swept = CorpusSweep.sweep(root, root.resolve("results.tsv"));

            assertThat(swept.exclusions())
                    .as("a misconfigured run is not a fact about anyone's bucket").isEmpty();
            assertThat(swept.problems()).singleElement().satisfies(problem -> {
                assertThat(problem.fixture()).isEqualTo("one");
                assertThat(problem.leg()).isEqualTo(CorpusSweep.WHOLE_FIXTURE);
                assertThat(problem.what()).contains("max-resident-bytes");
            });
            assertThat(swept.rows()).isEmpty();
        } finally {
            System.clearProperty(SimStoreConfig.STREAMING_MAX_RESIDENT_BYTES_PROPERTY);
        }
    }

    @Test
    void onlyTheTwoTypedRefusalsAreExclusions() {
        assertThat(CorpusSweep.refusal("fx", new IllegalStateException("a bug in the sweep")))
                .as("an unrelated runtime failure is never an exclusion").isEmpty();
        assertThat(CorpusSweep.refusal("fx",
                RowGroupOrderException.at(Path.of("part-00001.parquet"), 3, 7, "descending")))
                .get().extracting(CorpusSweep.Exclusion::reason)
                .isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER);
    }

    /**
     * A corpus swept at one fleet compares across itself and one swept at several does not, so which
     * of the two the operator has is a reading of the sweep rather than an assumption of it.
     */
    @Test
    void theSweepReportsTheFleetsItActuallyRanAt() throws IOException {
        fixture("captured", capture(37), keys(20));
        fixture("no-summary", null, keys(20));

        CorpusSweep.Result swept = CorpusSweep.sweep(root, root.resolve("results.tsv"));

        assertThat(CorpusSweep.fleets(swept.rows()))
                .containsExactly(entry(CorpusSweep.FALLBACK_WORKERS, List.of("no-summary")),
                        entry(37, List.of("captured")));
    }

    @Test
    void aCollapsedLegAtAnyArmOrSeedEarnsTheConfirmationSeeds() {
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURRENT, SEED_B, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_B,
                        CorpusSweep.COLLAPSE_SERIAL_FRACTION + 0.01, 100, 0.1))))
                .isTrue();
    }

    @Test
    void aCurrentVersusCandidateDurationGapEarnsThemInEitherDirection() {
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 70, 0.1))))
                .as("the anchored arm 30%% faster — a cure worth confirming").isTrue();
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 130, 0.1))))
                .as("the anchored arm 30%% slower — a regression worth confirming just as much").isTrue();
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 90, 0.1))))
                .as("ten per cent apart is not divergence").isFalse();
    }

    /**
     * The gap is read at <em>every</em> candidate arm. Screening one arm and promoting on all of them
     * is how a corpus of arms-in-agreement gets asserted from a corpus that was never asked: a
     * fixture where only the second candidate moves is never escalated, so it is never quoted, so
     * nothing about it is ever contradicted.
     */
    @Test
    void aGapAtTheSecondCandidateArmAloneEarnsThemToo() {
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.RATE_CURSOR_ANCHORED, SEED_A, 0.02, 60, 0.1))))
                .isTrue();
    }

    /**
     * And at a single seed, not only over their mean. What a collapse-prone fixture does is bimodal,
     * so a mean over two screening seeds is the reading least able to see it: one leg far down and one
     * level average to a gap that clears no threshold, and the fixture reads as uninteresting exactly
     * because it is unstable.
     */
    @Test
    void aGapAtOneScreeningSeedEarnsThemEvenWhenTheMeanOverBothDoesNot() {
        List<CorpusSweep.Screen> bimodal = List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURRENT, SEED_B, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 130, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_B, 0.02, 80, 0.1));

        assertThat(CorpusSweep.divergent(bimodal))
                .as("the two legs mean out to a 5%% gap; one of them is 30%%").isTrue();
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURRENT, SEED_B, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 110, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_B, 0.02, 90, 0.1))))
                .as("two legs that are each within the band are not a divergence at either").isFalse();
    }

    @Test
    void steadyNoVictimStealingAtAnyArmEarnsThemOnItsOwn() {
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100,
                        CorpusSweep.DIVERGENT_NO_VICTIM_SHARE + 0.01),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 100, 0.1))))
                .isTrue();
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, SEED_A, 0.02, 100, 0.9))))
                .as("a candidate that starves its own thieves is a regression worth four seeds too")
                .isTrue();
    }

    /** An arm that produced no reading is not a divergence — and must not read as a zero duration. */
    @Test
    void anArmThatDidNotRunIsNotAGap() {
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, SEED_A, 0.02, 100, 0.1),
                screen(SensingVariant.CURRENT, SEED_B, 0.02, 100, 0.1))))
                .isFalse();
    }

    @Test
    void everyRowCarriesEveryColumnTheHeaderNames() throws IOException {
        fixture("one", capture(4), keys(30));
        Path results = root.resolve("results.tsv");

        CorpusSweep.Result swept = CorpusSweep.sweep(root, results);

        List<String> lines = Files.readAllLines(results, StandardCharsets.UTF_8);
        int columns = CorpusSweep.HEADER.split("\t", -1).length;
        assertThat(lines.getFirst()).isEqualTo(CorpusSweep.HEADER);
        assertThat(lines).hasSize(swept.rows().size() + 1);
        assertThat(lines.subList(1, lines.size())).allSatisfy(line ->
                assertThat(line.split("\t", -1)).hasSize(columns));

        CorpusSweep.Row first = swept.rows().getFirst();
        assertThat(column(lines.get(1), "fixture")).isEqualTo("one");
        assertThat(column(lines.get(1), "keys")).isEqualTo("30");
        assertThat(column(lines.get(1), "backend")).isEqualTo(SimStoreBackend.STREAMING.toString());
        assertThat(column(lines.get(1), "workers")).isEqualTo("4");
        assertThat(column(lines.get(1), "fleet_source")).isEqualTo("capture");
        assertThat(column(lines.get(1), "arm")).isEqualTo(first.leg().variant());
        assertThat(column(lines.get(1), "seed")).isEqualTo(Long.toString(CorpusSweep.SCREENING_SEEDS[0]));
        assertThat(column(lines.get(1), "page"))
                .isEqualTo(Integer.toString(PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE));
        assertThat(column(lines.get(1), "completed")).isEqualTo("true");
        assertThat(column(lines.get(1), "escalated")).isEqualTo(Boolean.toString(first.escalated()));
        assertThat(column(lines.get(1), "collapsed")).isEqualTo(Boolean.toString(first.collapsed()));
    }

    // ---- fixtures -----------------------------------------------------------------------

    private static String column(String line, String name) {
        List<String> names = List.of(CorpusSweep.HEADER.split("\t", -1));
        return line.split("\t", -1)[names.indexOf(name)];
    }

    private static List<CorpusSweep.Row> rowsOf(CorpusSweep.Result swept, String fixture) {
        return swept.rows().stream().filter(row -> row.fixture().equals(fixture)).toList();
    }

    private static CorpusSweep.Screen screen(SensingVariant arm, long seed, double serial,
                                             double durationNanos, double noVictim) {
        return new CorpusSweep.Screen(SensingRaceProtocol.label(arm), seed, serial, durationNanos,
                noVictim);
    }

    /** {@code count} keys over a two-level tree, so a run has something to split at. */
    private static List<String> keys(int count) {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format(Locale.ROOT, "d%02d/f%04d", i % 5, i));
        }
        return keys.stream().sorted().toList();
    }

    /** A capture's own run record, carrying the one field the sweep reads out of it. */
    private static String capture(int maxParallelListings) {
        return "{ \"config\" : { \"" + CorpusSweep.MAX_PARALLEL_LISTINGS + "\" : "
                + maxParallelListings + " } }";
    }

    /**
     * A staged capture: a {@code data/} directory of stamped, complete Parquet holding {@code keys} in
     * exactly the order given, and — unless {@code summary} is null — the capture's own summary beside
     * it. Written straight through {@link SortedParquetWriter} rather than the sorter, because that is
     * the only way to produce the disordered-but-eligible case one of these tests needs.
     */
    private Path fixture(String name, String summary, List<String> keys) throws IOException {
        return fixture(name, summary, keys, SortConfigs.base());
    }

    private Path fixture(String name, String summary, List<String> keys, SortConfig config)
            throws IOException {
        Path fixture = Files.createDirectories(root.resolve(name).resolve(CorpusSweep.DATA_DIRECTORY));
        try (SortedFileWriter writer = new SortedParquetWriter(fixture.resolve("part-00001.parquet"),
                config, SortMode.OBJECTS, 1)) {
            writer.markFinal();
            for (String key : keys) {
                writer.write(ObjectEntries.bare(key));
            }
        }
        if (summary != null) {
            Files.writeString(fixture.getParent().resolve(CorpusSweep.SUMMARY_FILE), summary,
                    StandardCharsets.UTF_8);
        }
        return fixture.getParent();
    }

    /**
     * A capture written wholly descending over 1 KiB row groups of ~200-byte keys, so the disorder is
     * visible <em>between</em> row-group first keys and the streaming tier therefore refuses it at
     * index derive rather than at the first group a run faults in.
     */
    private Path descendingFixture(String name, String summary) throws IOException {
        int rows = 600;
        List<String> keys = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            keys.add(String.format(Locale.ROOT, "%08d", rows - i) + "x".repeat(190));
        }
        return fixture(name, summary, keys, SortConfigs.manySmallRowGroups());
    }
}
