/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.filter.FilterChain;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.s3.S3Config;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * End-to-end companion to {@code PartRotationCliTest#singleFileDestinationOverridesNonZeroRotationFlagsToDisabled},
 * which only checks that the RESOLVED spec has zero cadence fields. This drives an actual
 * listing run through {@link ListRunner#runToParquet} with a single-file destination
 * AND aggressive cadence flags (a row-count cap of 10, far below the object count, plus the
 * CLI-minimum 100ms interval) over enough keys that, if either trigger were live, it would
 * rotate hundreds of times. The contract: a single-file run
 * stays exactly one part regardless of the cadence flags supplied.
 */
class SingleFileCadenceE2ETest {

    /** {@code parquetSpec} only reads {@code region()} off this (for the JSON-summary run-config), unused here. */
    private static final S3Config TEST_CONFIG = new S3Config(
            null, null, false, 64, S3Config.DEFAULT_MAX_ATTEMPTS, S3Config.DEFAULT_ATTEMPT_TIMEOUT,
            S3Config.DEFAULT_API_CALL_TIMEOUT, null, S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);

    private static ListCommand parse(String... args) throws Exception {
        ListCommand list = new ListCommand();
        String[] full = new String[args.length + 3];
        full[0] = "s3://bucket";
        System.arraycopy(args, 0, full, 1, args.length);
        full[full.length - 2] = "--checkpoint";
        full[full.length - 1] = "none";
        new CommandLine(list).parseArgs(full);
        return list;
    }

    private static List<Path> parts(Path dir) throws Exception {
        return DatasetLayout.of(dir).dataParts();   // parts live under data/
    }

    @Test
    void singleFileWithAggressiveCadenceFlagsProducesExactlyOnePartAndOneManifestEntry(@TempDir Path dir)
            throws Exception {
        int n = 5_000;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();

        // Aggressive cadence flags: a 10-row cap would rotate ~500 times over 5,000 keys, and
        // 100ms is the CLI-enforced floor (MIN_PART_ROTATION_INTERVAL) — both would fire
        // repeatedly if the single-file destination did not force them off.
        ListCommand list = parse("--part-rotation-interval", "100ms", "--part-rotation-max-rows", "10");
        list.output.resolvedKind = OutputOptions.DestinationKind.FILE;   // as resolveOutput() would set it for -o out.parquet

        S3Uri s3uri = S3Uri.parse("s3://bucket");
        ListRunner.ParquetSpec spec = list.parquetSpec(s3uri, 1000, 1000, FilterChain.EMPTY, "hash", TEST_CONFIG);
        assertThat(spec.numWriters()).as("a single-file destination collapses to one lane").isEqualTo(1);
        assertThat(spec.rotationIntervalNanos()).as("a single-file destination forces the interval trigger off").isZero();
        assertThat(spec.rotationMaxRows()).as("a single-file destination forces the row-count trigger off").isZero();

        var stats = new ListRunner().runToParquet(RunContext.create(), fetcher, dir, spec);
        assertThat(stats.objects()).isEqualTo(n);

        List<Path> parts = parts(dir);
        assertThat(parts)
                .as("a single-file destination must stay exactly one part regardless of the cadence flags").hasSize(1);

        String manifest = Files.readString(DatasetLayout.of(dir).manifest());
        assertThat(manifest).contains("\"key\": \"data/part-");
        long partEntries = manifest.lines().filter(l -> l.contains("\"key\":")).count();
        assertThat(partEntries).as("exactly one part entry in the manifest").isEqualTo(1);
    }
}
