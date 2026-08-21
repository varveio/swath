/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.engine.policy.VictimScan;
import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The opt-in gate dump: what a run's two gate chains actually read, as two TSVs to diff against a
 * replay trace of the same listing.
 *
 * <p>The load-bearing claim is <b>completeness</b> — a dump with rows missing would read as a gate
 * that never fired, which is exactly the finding the artifact exists to support or refute — so the
 * row counts are asserted as equalities against the run's own counters rather than as "some rows
 * came out". The owner file's identity: every {@code decide} past the open-frontier early-out lands
 * in exactly one {@code OWNER_SPLIT.*} counter, a skip through its own gate's engagement and a carve
 * through the executor's published/aborted/confetti-suppressed outcome. The scan file's is simpler
 * still: selection runs once per steal attempt.
 */
class SimGateDumpTest {

    /** The counters a decision past the open-frontier early-out lands in, one apiece. */
    private static final List<String> CARVE_OUTCOMES = List.of("self_published", "self_aborted");

    @Test
    void everyGateChainOutcomeTheRunCountedIsADumpedRow(@TempDir Path dir) throws IOException {
        Path dump = dir.resolve("gate-inputs.tsv");

        PolicyRunResult result = runDumping(dump);

        List<String> rows = Files.readAllLines(dump, StandardCharsets.UTF_8);
        assertThat(rows.getFirst()).isEqualTo(SimGateDump.DECISION_HEADER);
        assertThat(rows).as("one row per owner-split decision the run's counters report")
                .hasSize(1 + (int) ownerDecisions(result));

        List<String> columns = List.of(SimGateDump.DECISION_HEADER.split("\t"));
        assertThat(column(rows, columns, "far_ahead_fraction"))
                .as("a gate terminating above where the chain computes f reports the NOT_COMPUTED "
                        + "sentinel verbatim -- a TSV has no JSON's ban on a non-finite double")
                .contains("NaN");
        assertThat(column(rows, columns, "hi"))
                .as("the open-frontier early-out reads no gate and is the one decision NOT dumped, so "
                        + "no dumped row can be missing its upper bound")
                .doesNotContain("");
        assertThat(column(rows, columns, "cursor_to"))
                .as("a decision is only taken on a commit that emitted keys, so it has a cursor")
                .doesNotContain("");
        assertThat(column(rows, columns, "node_id"))
                .as("the ledger's own id space, so a tail range's rows can be isolated")
                .allMatch(id -> Long.parseLong(id) >= 0L);
    }

    @Test
    void everyVictimScanTheRunAttemptedIsADumpedRow(@TempDir Path dir) throws IOException {
        Path dump = dir.resolve("gate-inputs.tsv");

        PolicyRunResult result = runDumping(dump);

        List<String> rows = Files.readAllLines(Path.of(dump + SimGateDump.SCAN_PATH_SUFFIX),
                StandardCharsets.UTF_8);
        assertThat(rows.getFirst()).isEqualTo(SimGateDump.SCAN_HEADER);
        assertThat(rows).as("selection runs exactly once per steal attempt")
                .hasSize(1 + (int) result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER));

        List<String> columns = List.of(SimGateDump.SCAN_HEADER.split("\t"));
        int chosen = columns.indexOf("chosen_node_id");
        int bestEst = columns.indexOf("best_est");
        int reason = columns.indexOf("reason");
        assertThat(rows.stream().skip(1).map(row -> row.split("\t", -1))
                .filter(fields -> fields[chosen].equals(String.valueOf(SimGateDump.NO_CHOSEN_VICTIM))))
                .as("a scan that refused carries its reason and the argmax's own unseeded best")
                .isNotEmpty()
                .allSatisfy(fields -> {
                    assertThat(fields[reason]).isNotEmpty();
                    assertThat(fields[bestEst]).isEqualTo("-Infinity");
                });
    }

    @Test
    void aKeyThatIsNotValidUtf8IsRefusedRatherThanWrittenAsReplacementCharacters(@TempDir Path dir) {
        Path dump = dir.resolve("gate-inputs.tsv");
        System.setProperty(SimGateDump.DUMP_PATH_PROPERTY, dump.toString());
        try (SimGateDump gateDump = SimGateDump.fromSystemProperties()) {
            byte[] malformed = {(byte) 0xC3, (byte) 0x28};
            VictimScan scan = new VictimScan(1, 0, 0, 0, 1.0);
            assertThatThrownBy(() -> gateDump.victimScan(0L, scan, 1L, null, malformed, malformed, malformed))
                    .as("a key that decodes as replacement characters would silently corrupt the "
                            + "byte-exact diff the dump exists to support")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not valid UTF-8");
        } finally {
            System.clearProperty(SimGateDump.DUMP_PATH_PROPERTY);
        }
    }

    @Test
    void aRunThatWasNotAskedForADumpWritesNothing(@TempDir Path dir) throws IOException {
        assertThat(System.getProperty(SimGateDump.DUMP_PATH_PROPERTY))
                .as("the property is the only switch, so a leaked one would make this test vacuous")
                .isNull();

        SimExecutor.run(scenario(), store(), "in-memory observation archive");

        try (Stream<Path> written = Files.list(dir)) {
            assertThat(written).isEmpty();
        }
    }

    /** The run every dumping case here shares, with the property set for its duration only. */
    private static PolicyRunResult runDumping(Path dump) {
        System.setProperty(SimGateDump.DUMP_PATH_PROPERTY, dump.toString());
        try {
            return SimExecutor.run(scenario(), store(), "in-memory observation archive");
        } finally {
            System.clearProperty(SimGateDump.DUMP_PATH_PROPERTY);
        }
    }

    /**
     * Every owner-split decision the run counted. The open-frontier early-out is excluded because it
     * engages no counter and is dumped as no row: it decides nothing and reads nothing.
     */
    private static long ownerDecisions(PolicyRunResult result) {
        long decisions = 0;
        for (OwnerSplitSkipReason skip : OwnerSplitSkipReason.values()) {
            if (skip != OwnerSplitSkipReason.OPEN_FRONTIER) {
                decisions += result.counter(SimExecutor.OWNER_SPLIT_CATEGORY + "." + skip.code());
            }
        }
        for (String carved : CARVE_OUTCOMES) {
            decisions += result.counter(SimExecutor.OWNER_SPLIT_CATEGORY + "." + carved);
        }
        return decisions;
    }

    private static List<String> column(List<String> rows, List<String> columns, String name) {
        int index = columns.indexOf(name);
        return rows.stream().skip(1).map(row -> row.split("\t", -1)[index]).toList();
    }

    private static PolicyScenario scenario() {
        return PolicyRunFixtures.scenario(8, 100, PolicyRunFixtures.REMOTE_LATENCY,
                PolicyRunFixtures.measuredCost());
    }

    private static ListingFixtureStore store() {
        return new ListingFixtureStore(KeyspaceFixtures.observationArchive(4, 6, 8, 40));
    }
}
