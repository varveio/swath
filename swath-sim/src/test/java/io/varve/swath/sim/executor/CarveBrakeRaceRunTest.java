/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.CarveBrakeMode;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.sim.store.SimStoreBackend;
import io.varve.swath.sim.store.SimStoreConfig;
import io.varve.swath.sim.store.SimStoreFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link CarveBrakeSweepProtocol}, run.</b> The machinery {@link CarveAdmissionRaceRunTest} drives,
 * over the same kind of operator-staged roster, with the sensing arms held fixed at the shipped
 * default ({@link io.varve.swath.sim.executor.SensingVariant#CURRENT} — which, through {@code
 * EngineToggles.DEFAULT.rateAnchoredSensing()}, already installs the 0.2.0 promoted sensor) and only
 * {@code carve_brake} varied across {@link CarveBrakeSweepProtocol#ARMS}.
 *
 * <pre>{@code ./gradlew :swath-sim:test -PonlyPerf \
 *     -Dswath.sim.listing.corpus=/path/to/staged/roster \
 *     -Dswath.sim.listing.results=/path/to/carve-brake-race.tsv}</pre>
 *
 * <p>Opt-in and fixture-free on the same two properties {@link CorpusSweepRunTest} declares, and for
 * the same reasons: the roster is a local directory the operator supplies, the repo names none of it,
 * the results file must not already exist, and with the corpus property unset the run <em>skips</em>
 * rather than fails.
 *
 * <p>This round cannot reuse {@link CorpusSweep#sweep} directly — that machinery varies {@link
 * SensingVariant}, and this round varies {@code carve_brake} instead, holding sensing at the shipped
 * default throughout. So the fixture-discovery, fleet-sizing and exclusion-handling helpers are reused
 * from {@link CorpusSweep} ({@link CorpusSweep#fixtures}, {@link CorpusSweep#fleetOf}, {@link
 * CorpusSweep#refusal}) while the per-leg loop is this round's own, and every leg still lands in a
 * {@link CorpusSweep.Row} so the printed tables and the {@link CarveAdmissionRaceProtocol} yardstick are
 * exactly the ones every other round in this package uses.
 *
 * <p><b>Nothing asserts a magnitude.</b> The protocol's F1–F4 are read off the tables this prints; what
 * is asserted is what every round of this campaign asserts: that the race measured something, and that
 * no leg produced a number nobody may use.
 */
@Tag("perf")
class CarveBrakeRaceRunTest {

    @Test
    void everyCarveBrakeModeAgainstTheIncumbentOverTheRoster() throws IOException {
        CorpusSweepRunTest.Staged staged = CorpusSweepRunTest.staged("roster", "race");

        CorpusSweep.Corpus corpus = CorpusSweep.fixtures(staged.root());
        for (CorpusSweep.Skipped skipped : corpus.skipped()) {
            System.out.printf(Locale.ROOT, "carve_brake_race phase=skipped fixture=%s reason=%s detail=%s%n",
                    skipped.fixture(), skipped.reason(), skipped.detail());
        }

        List<CorpusSweep.Row> rows = new ArrayList<>();
        List<CorpusSweep.Exclusion> exclusions = new ArrayList<>();
        List<CorpusSweep.Problem> problems = new ArrayList<>();

        try (BufferedWriter out = Files.newBufferedWriter(staged.results(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            out.write(CorpusSweep.HEADER);
            out.newLine();
            out.flush();
            for (Path fixture : corpus.fixtures()) {
                String name = fixture.getFileName().toString();
                System.out.printf(Locale.ROOT, "carve_brake_race phase=fixture fixture=%s%n", name);
                List<CorpusSweep.Row> swept;
                try {
                    swept = raceOneFixture(fixture, name, problems);
                } catch (RuntimeException failure) {
                    Optional<CorpusSweep.Exclusion> refused = CorpusSweep.refusal(name, failure);
                    if (refused.isEmpty()) {
                        problems.add(new CorpusSweep.Problem(name, CorpusSweep.WHOLE_FIXTURE,
                                "failed: " + failure));
                        System.out.printf(Locale.ROOT, "carve_brake_race phase=failed fixture=%s %s%n",
                                name, failure);
                        continue;
                    }
                    problems.removeIf(problem -> problem.fixture().equals(name));
                    exclusions.add(refused.get());
                    System.out.printf(Locale.ROOT, "carve_brake_race phase=excluded fixture=%s reason=%s %s%n",
                            name, refused.get().reason(), failure.getMessage());
                    continue;
                }
                rows.addAll(swept);
                for (CorpusSweep.Row row : swept) {
                    out.write(CorpusSweep.row(row));
                    out.newLine();
                }
                out.flush();
                SensingRaceProtocol.printTable("carve-brake race — " + name,
                        swept.stream().map(CorpusSweep.Row::leg).toList());
                printMechanism(name, swept);
            }
        }
        for (CorpusSweep.Exclusion exclusion : exclusions) {
            System.out.printf(Locale.ROOT, "carve_brake_race phase=exclusion fixture=%s reason=%s where=%s%n",
                    exclusion.fixture(), exclusion.reason(), exclusion.where());
        }

        CarveAdmissionRaceProtocol.printVerdicts(
                "carve-brake race — paired relative duration against carve_brake=off, same seed",
                CarveAdmissionRaceProtocol.verdicts(rows, CarveBrakeSweepProtocol.CONTROL_ARM));

        assertThat(rows).as("the race measured at least one leg").isNotEmpty();
        assertThat(problems).as("legs whose numbers are unusable").isEmpty();
    }

    /**
     * One fixture: open it once, race every {@link CarveBrakeSweepProtocol#ARMS} entry at all four of
     * {@code SensingRaceProtocol.SEEDS} — no screening tier, this round exists to resolve a verdict —
     * close it. Mirrors {@link CorpusSweep#sweepOne}'s open/close discipline for the same reason: the
     * streaming tier's decoded segments are off-heap and bounded per handle.
     */
    private static List<CorpusSweep.Row> raceOneFixture(Path fixture, String name,
                                                         List<CorpusSweep.Problem> problems) throws IOException {
        CorpusSweep.Fleet fleet = CorpusSweep.fleetOf(fixture);
        SimStoreFactory.Result opened = SimStoreFactory.open(fixture.resolve(CorpusSweep.DATA_DIRECTORY),
                SimStoreBackend.STREAMING, SimStoreConfig.fromSystemProperties());
        long keys = opened.keyCount().orElse(-1);
        System.out.printf(Locale.ROOT,
                "carve_brake_race phase=open fixture=%s backend=%s keys=%d workers=%d fleet_source=%s%n",
                name, opened.resolvedBackend(), keys, fleet.workers(), fleet.source());
        List<CorpusSweep.Row> rows = new ArrayList<>();
        try (ListingStore store = opened.store()) {
            for (CarveBrakeMode mode : CarveBrakeSweepProtocol.ARMS) {
                for (long seed : SensingRaceProtocol.SEEDS) {
                    rows.add(runLeg(mode, name, store, fleet, seed, keys, problems));
                }
            }
        }
        return rows;
    }

    /**
     * One (arm, seed) leg, at the measured page regime and the live store's own call-class profile —
     * the configuration every other real-listing number in this campaign was taken at ({@link
     * CorpusSweep#legs}'s own choice, mirrored here). Sensing is held at {@link SensingVariant#CURRENT}
     * throughout: this round varies {@code carve_brake} alone.
     */
    private static CorpusSweep.Row runLeg(CarveBrakeMode mode, String fixture, ListingStore store,
                                          CorpusSweep.Fleet fleet, long seed, long keys,
                                          List<CorpusSweep.Problem> problems) {
        EngineToggles toggles = EngineToggles.DEFAULT.withCarveBrake(mode);
        PolicyScenario base = PolicyRunFixtures.scenario(fleet.workers(),
                PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE, PolicyRunFixtures.LIVE_S3_LATENCY,
                PolicyRunFixtures.measuredCost()).withSeed(seed);
        PolicyScenario scenario = new PolicyScenario(base.seed(), base.workerCount(), base.pageSize(),
                base.scanPrefix(), base.seedMode(), toggles, base.latency(), base.clientCost(),
                base.budgets(), base.faultDisposition(), base.storeServerCapacity(), base.recordEventLog(),
                base.maxEvents());
        String label = "carve-brake roster fixture (" + fixture + ")";
        PolicyRunResult result = SimExecutor.run(scenario, store, label, SensingVariant.CURRENT);
        String leg = mode.code() + "/" + fixture + "/seed " + seed;
        if (!result.completed()) {
            problems.add(new CorpusSweep.Problem(fixture, leg, "did not complete: " + result.stopReason()));
        } else if (keys >= 0 && result.keysEmitted() != keys) {
            problems.add(new CorpusSweep.Problem(fixture, leg, "emitted " + result.keysEmitted() + " of "
                    + keys + " keys"));
        }
        SensingRaceProtocol.Leg races = new SensingRaceProtocol.Leg(mode.code(), fixture, seed,
                PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE, result);
        // Every leg in this round runs the roster's own forced STREAMING tier (CorpusSweep's own
        // choice, unchanged here), so the row's backend column is this constant rather than a
        // re-derivation from the handle.
        return new CorpusSweep.Row(fixture, keys, SimStoreBackend.STREAMING, fleet, Duration.ZERO, races,
                Duration.ZERO, 0.0, true);
    }

    /**
     * The brake's own engagement counters, per arm — attributing the outcome to the mechanism rather
     * than reporting only the duration delta. {@code carve_braked} is a refused carve; {@code
     * carve_brake_probe} is the {@code Pth} would-be-braked carve let through by the probe escape; both
     * are zero by construction under {@code OFF}. Owner- and thief-split children are printed alongside
     * so a reader can see whether a mode that brakes more also splits less overall, or merely relocates
     * where the splitting happens.
     */
    private static void printMechanism(String fixture, List<CorpusSweep.Row> rows) {
        StringBuilder out = new StringBuilder("carve-brake mechanism — ").append(fixture)
                .append(System.lineSeparator());
        out.append(String.format(Locale.ROOT, "%-10s %-10s %10s %10s %10s %10s %10s%n", "arm", "seed",
                "own_split", "thf_split", "braked", "brk_probe", "occ_mean"));
        for (CorpusSweep.Row row : rows) {
            var result = row.leg().result();
            out.append(String.format(Locale.ROOT, "%-10s %-10d %10d %10d %10d %10d %10.3f%n",
                    row.leg().variant(), row.leg().seed(), result.ownerSplitChildren(),
                    result.thiefChildren(),
                    result.counter(SimExecutor.OWNER_SPLIT_CATEGORY + ".carve_braked"),
                    result.counter(SimExecutor.OWNER_SPLIT_CATEGORY + ".carve_brake_probe"),
                    result.timeline().meanOccupancy()));
        }
        System.out.print(out);
    }
}
