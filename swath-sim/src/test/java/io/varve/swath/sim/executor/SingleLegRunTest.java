/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.sim.store.SimStoreBackend;
import io.varve.swath.sim.store.SimStoreConfig;
import io.varve.swath.sim.store.SimStoreFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <b>One sim leg, chosen from the command line</b> — the single-run companion to
 * {@link RealListingRunTest}'s tables. Exists for artifact-producing diagnosis runs whose sinks
 * refuse to be shared across legs (the {@code swath.sim.gate-dump} file is {@code CREATE_NEW} — one
 * run per dump, by design), where a table harness would fail loudly on its second leg.
 *
 * <p>Opt-in and parameterized entirely by system properties, all under the same
 * {@code swath.sim.listing.*} namespace as the sibling harnesses:
 *
 * <pre>{@code
 * ./gradlew :swath-sim:test -PonlyPerf -PsimTestHeap=6g \
 *   -Dswath.sim.listing.fixture=/path/to/fixture \
 *   -Dswath.sim.listing.workers=64 \
 *   -Dswath.sim.listing.arm=RATE_ANCHORED_FLOOR_QUARTER \
 *   -Dswath.sim.listing.seed=424242 \
 *   -Dswath.sim.gate-dump=/path/to/dump.tsv \
 *   --tests '*SingleLegRunTest*'
 * }</pre>
 *
 * <p>The arm must name a {@link SensingVariant}; the seed is any long (the sweep's verdict standard
 * of four seeds does not apply — this is a diagnosis instrument, and the seed worth dumping is
 * whichever one the table misbehaved at). Page regime is the measured one
 * ({@link PolicyRunFixtures#MEASURED_TAIL_PAGE_SIZE}) at the live store's own call-class latency
 * profile ({@link PolicyRunFixtures#LIVE_S3_LATENCY}), like every other real-listing leg; the run
 * prints the same phase summary the
 * race tables print, so its row can be read against theirs.
 */
@Tag("perf")
class SingleLegRunTest {

    static final String ARM_PROPERTY = "swath.sim.listing.arm";
    static final String SEED_PROPERTY = "swath.sim.listing.seed";

    @Test
    void oneLegAtTheMeasuredRegime() {
        String configured = System.getProperty(RealListingRunTest.FIXTURE_PROPERTY);
        assumeTrue(configured != null && !configured.isBlank(),
                "no " + RealListingRunTest.FIXTURE_PROPERTY + " supplied; skipping");
        String armName = System.getProperty(ARM_PROPERTY);
        assumeTrue(armName != null && !armName.isBlank(), "no " + ARM_PROPERTY + " supplied; skipping");

        Path fixture = Path.of(configured);
        assertThat(Files.exists(fixture)).as("fixture at %s", fixture).isTrue();
        SensingVariant arm = parseArm(armName);
        long seed = Long.parseLong(System.getProperty(SEED_PROPERTY, "1"));
        int workers = Integer.parseInt(
                System.getProperty(RealListingRunTest.WORKERS_PROPERTY,
                        String.valueOf(SensingRaceProtocol.WORKERS)));

        SimStoreConfig config = new SimStoreConfig(Runtime.getRuntime().maxMemory() / 3,
                SimStoreConfig.DEFAULT_STREAMING_MAX_RESIDENT_BYTES);
        SimStoreFactory.Result opened = SimStoreFactory.open(fixture, SimStoreBackend.AUTO, config);
        ListingStore store = opened.store();
        try {
            String storeLabel = "single leg (" + opened.resolvedBackend() + ")";
            PolicyScenario scenario = PolicyRunFixtures
                    .scenario(workers, PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                            PolicyRunFixtures.LIVE_S3_LATENCY, PolicyRunFixtures.measuredCost())
                    .withSeed(seed);
            PolicyRunResult result = SimExecutor.run(scenario, store, storeLabel, arm);

            assertThat(result.completed()).as("the leg completed").isTrue();
            long expected = opened.keyCount().orElse(result.keysEmitted());
            assertThat(result.keysEmitted()).as("the leg emitted every key").isEqualTo(expected);
            System.out.printf(Locale.ROOT,
                    "single_leg arm=%s seed=%d workers=%d backend=%s%n%s%n",
                    arm, seed, workers, opened.resolvedBackend(), result.describe());
        } finally {
            store.close();
        }
    }

    /**
     * {@link SensingVariant#valueOf} on a mistyped arm throws {@code "No enum constant ..."}, which
     * names what was wrong but not what would have been right. This harness is driven entirely by
     * hand-typed {@code -D} properties at a shell, so the arm name is the likeliest thing to get
     * wrong and the alternatives are the only thing worth printing.
     */
    static SensingVariant parseArm(String raw) {
        String name = raw.trim().toUpperCase(Locale.ROOT);
        try {
            return SensingVariant.valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "unknown -D%s=%s; valid arms: %s".formatted(ARM_PROPERTY, raw.trim(),
                            Arrays.stream(SensingVariant.values()).map(Enum::name).collect(Collectors.joining(", "))),
                    e);
        }
    }
}
