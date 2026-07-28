/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.sim.store.SimStoreBackend;
import io.varve.swath.sort.RowGroupOrderException;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedParquetWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

    @TempDir
    private Path root;

    @Test
    void everyFixtureUnderTheRootIsSweptAndDirectoriesThatAreNotFixturesAreNot() throws IOException {
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
            assertThat(excluded.where()).contains("row_group=0").contains("row=2");
        });
        assertThat(swept.rows()).extracting(CorpusSweep.Row::fixture)
                .as("the fixtures on either side of the refused one were both measured")
                .contains("aaa-healthy", "ccc-healthy").doesNotContain("bbb-disordered");
        assertThat(swept.problems()).as("a refused capture is corpus data, not an unusable leg").isEmpty();
    }

    @Test
    void aCollapsedLegAtAnyArmOrSeedEarnsTheConfirmationSeeds() {
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, 0.02, 100, 0.1),
                screen(SensingVariant.CURRENT, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, CorpusSweep.COLLAPSE_SERIAL_FRACTION + 0.01, 100, 0.1))))
                .isTrue();
    }

    @Test
    void aCurrentVersusAnchoredDurationGapEarnsThemInEitherDirection() {
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, 0.02, 70, 0.1))))
                .as("the anchored arm 30%% faster — a cure worth confirming").isTrue();
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, 0.02, 130, 0.1))))
                .as("the anchored arm 30%% slower — a regression worth confirming just as much").isTrue();
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, 0.02, 90, 0.1))))
                .as("ten per cent apart is not divergence").isFalse();
    }

    @Test
    void steadyNoVictimStealingUnderTheShippedSensorEarnsThemOnItsOwn() {
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, 0.02, 100, CorpusSweep.DIVERGENT_NO_VICTIM_SHARE + 0.01),
                screen(SensingVariant.CURSOR_ANCHORED, 0.02, 100, 0.1))))
                .isTrue();
        assertThat(CorpusSweep.divergent(List.of(
                screen(SensingVariant.CURRENT, 0.02, 100, 0.1),
                screen(SensingVariant.CURSOR_ANCHORED, 0.02, 100, 0.9))))
                .as("the reading is the shipped sensor's; a candidate's own is not the screen")
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

    private static CorpusSweep.Screen screen(SensingVariant arm, double serial, double durationNanos,
                                             double noVictim) {
        return new CorpusSweep.Screen(SensingRaceProtocol.label(arm), serial, durationNanos, noVictim);
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
        Path fixture = Files.createDirectories(root.resolve(name).resolve(CorpusSweep.DATA_DIRECTORY));
        try (SortedFileWriter writer = new SortedParquetWriter(fixture.resolve("part-00001.parquet"),
                SortConfigs.base(), SortMode.OBJECTS, 1)) {
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
}
