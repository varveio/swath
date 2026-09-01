/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.finalize.SortTestSupport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Staging ownership when a cascade group fails before publication. The failed attempt must leave the
 * checkpoint's source set and the previously published dataset exactly as it found them, and must
 * leave behind neither a finished nor an unfinished cascade intermediate — both are disposable, and
 * both would otherwise be counted by the startup disk pre-check that runs before the next attempt's
 * kickoff sweep could reclaim them.
 */
class SortedDatasetCoordinatorCascadeCleanupTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void aCascadeFailingAfterACompletedGroupLeavesNothingForTheRetryToReclaim(@TempDir Path root)
            throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> sources = SortTestSupport.writeCascadeSources(staging);
        Map<Path, byte[]> originalBytes = snapshot(sources);
        Path damaged = SortTestSupport.corruptLateCascadeSourcePage(sources);
        byte[] damagedBytes = Files.readAllBytes(damaged);
        byte[] priorGeneration = "prior published output".getBytes(StandardCharsets.UTF_8);
        Path priorFinal = Files.write(output.resolve("part-00000.parquet"), priorGeneration);

        SortedDatasetCoordinator coordinator = coordinator(SortConfigs.base().withFanIn(2));
        assertThatThrownBy(() -> coordinator.transform(sources, output, staging,
                SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("record CRC32C mismatch");

        assertThat(priorFinal)
                .as("a pre-publication failure never touches the published generation")
                .hasBinaryContent(priorGeneration);
        assertThat(damaged).hasBinaryContent(damagedBytes);
        try (Stream<Path> remaining = Files.list(staging)) {
            assertThat(remaining)
                    .as("the checkpoint's exact source set survives, and nothing else does")
                    .containsExactlyInAnyOrderElementsOf(sources);
        }
        assertThat(stagingBytes(staging))
                .as("what the startup disk pre-check would sum is the source set alone")
                .isEqualTo(byteTotal(sources));

        // The transient read failure clears; the retry re-enters the merge from exactly the segments
        // the checkpoint already recorded, with no manual deletion and no new listing.
        Files.write(damaged, originalBytes.get(damaged));
        SortedDatasetResult retried = coordinator.transform(sources, output, staging,
                SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP);

        assertThat(retried.cascadedPasses()).as("the retry really cascaded").isGreaterThan(0);
        assertThat(retried.totalRows()).isEqualTo(SortTestSupport.cascadeSourceRows());
        assertThat(keys(retried.finalFiles()))
                .hasSize(SortTestSupport.cascadeSourceRows()).isSorted().doesNotHaveDuplicates();
        assertThat(Files.exists(staging)).as("staging reclaimed once the retry publishes").isFalse();
    }

    private SortedDatasetCoordinator coordinator(SortConfig config) {
        return new SortedDatasetCoordinator(new SortRun(config, cmp, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
    }

    private static Map<Path, byte[]> snapshot(List<Path> paths) throws IOException {
        Map<Path, byte[]> bytes = new LinkedHashMap<>();
        for (Path path : paths) {
            bytes.put(path, Files.readAllBytes(path));
        }
        return bytes;
    }

    private static long byteTotal(List<Path> paths) throws IOException {
        long total = 0;
        for (Path path : paths) {
            total += Files.size(path);
        }
        return total;
    }

    /** The quantity the CLI's startup disk pre-check sums: every regular file under staging. */
    private static long stagingBytes(Path staging) throws IOException {
        try (Stream<Path> walk = Files.walk(staging)) {
            long total = 0;
            for (Path path : walk.filter(Files::isRegularFile).toList()) {
                total += Files.size(path);
            }
            return total;
        }
    }

    private static List<String> keys(List<Path> files) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path file : files) {
            try (ParquetEntryReader reader = new ParquetEntryReader(file)) {
                while (reader.hasNext()) {
                    keys.add(reader.next().key().asString());
                }
            }
        }
        return keys;
    }
}
