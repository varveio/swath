/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortFinalization;
import io.varve.swath.sort.StagingRetention;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import picocli.CommandLine;

class TuneOptionsTest {

    @Test
    void repeatableTypedValuesReachTheExistingOptionFields() throws Exception {
        ListCommand cmd = parsed(
                "--tune", "engine.readahead=on",
                "--tune", "seed.mode=none",
                "--tune", "parquet.writers=4",
                "--tune", "summary.interval=2s",
                "--tune", "sort.merge-parallelism=16",
                "--tune", "sort.finalization=pipeline",
                "--tune", "sort.keep-staging=on",
                "--tune", "sort.ignore-disk-check=on");

        assertThat(cmd.tune.apply(cmd.output, cmd.engine, cmd.sorting,
                new PrintWriter(new StringWriter()))).isFalse();
        assertThat(cmd.engine.resolveToggles().readahead()).isTrue();
        assertThat(cmd.engine.resolveSeedMode().name()).isEqualTo("NONE");
        assertThat(cmd.output.parquetWriters).isEqualTo(4);
        assertThat(cmd.resolveSummaryJsonInterval()).isEqualTo(Duration.ofSeconds(2));
        assertThat(cmd.sorting.resolveConfig().mergeParallelism()).isEqualTo(16);
        assertThat(cmd.sorting.resolveConfig().finalization()).isEqualTo(SortFinalization.PIPELINE);
        assertThat(cmd.sorting.resolveConfig().stagingRetention().retainsOriginals()).isTrue();
        assertThat(cmd.sorting.forceSort).isTrue();
    }

    @Test
    void unknownKeyIsExitTwoAndListsEveryValidKey() {
        CommandLine cli = App.commandLine();
        StringWriter err = new StringWriter();
        cli.setErr(new PrintWriter(err));

        int exit = cli.execute("list", "s3://bucket/prefix", "--tune", "engine.readhed=on");

        assertThat(exit).isEqualTo(2);
        assertThat(err.toString()).contains("unknown key 'engine.readhed'")
                .contains("did you mean 'engine.readahead'?");
        for (String key : TuneOptions.KEYS) {
            assertThat(err.toString()).contains(key);
        }
    }

    @Test
    void invalidValuesNameTheirKeyAndExpectedTypeOrRange() {
        String[][] bad = {
                {"engine.readahead=maybe", "on|off"},
                {"seed.mode=random", "shallow|none|hints"},
                {"parquet.writers=65", "integer 2..64"},
                {"summary.interval=zero", "positive duration"},
                {"sort.merge-parallelism=0", "integer 1..16"},
                {"sort.merge-parallelism=17", "integer 1..16"},
                {"sort.finalization=lanes", "pipeline"},
                {"sort.keep-staging=yes", "on|off"},
                {"sort.ignore-disk-check=yes", "on|off"},
        };
        for (String[] example : bad) {
            CommandLine cli = App.commandLine();
            StringWriter err = new StringWriter();
            cli.setErr(new PrintWriter(err));
            int exit = cli.execute("list", "s3://bucket/prefix", "--tune", example[0]);
            assertThat(exit).as(example[0]).isEqualTo(2);
            assertThat(err.toString()).contains(example[0].substring(0, example[0].indexOf('=')))
                    .contains(example[1]);
        }
    }

    @Test
    void helpAndKeyQuestionPrintWithoutStartingAListing() {
        CommandLine all = App.commandLine();
        StringWriter allOut = new StringWriter();
        all.setOut(new PrintWriter(allOut));
        assertThat(all.execute("list", "--tune", "help")).isZero();
        assertThat(allOut.toString()).contains("Tune keys:")
                .contains("engine.readahead")
                .contains("default=off")
                .contains("stability=experimental")
                .contains("resume=identity")
                .contains("applies=fresh list");

        CommandLine one = App.commandLine();
        StringWriter oneOut = new StringWriter();
        one.setOut(new PrintWriter(oneOut));
        assertThat(one.execute("list", "--tune", "seed.mode=?")).isZero();
        assertThat(oneOut.toString()).contains("seed.mode:")
                .contains("values=shallow|none|hints")
                .contains("default=shallow")
                .contains("resume=identity");
    }

    @Test
    void contradictoryRepeatedValuesAreRejectedButIdenticalRepeatsPass() throws Exception {
        CommandLine cli = App.commandLine();
        StringWriter err = new StringWriter();
        cli.setErr(new PrintWriter(err));

        int exit = cli.execute("list", "--tune", "seed.mode=none",
                "--tune", "seed.mode=shallow");

        assertThat(exit).isEqualTo(2);
        assertThat(err.toString()).contains("seed.mode")
                .contains("'none'")
                .contains("'shallow'")
                .contains("contradictory");

        ListCommand identical = parsed("--tune", "seed.mode=none",
                "--tune", "seed.mode=none");
        assertThat(identical.tune.apply(identical.output, identical.engine, identical.sorting,
                new PrintWriter(new StringWriter()))).isFalse();
        assertThat(identical.engine.seed).isEqualTo("none");
    }

    @Test
    void verboseModePrintsRegistryOrderedEffectiveTuneValuesIncludingDefaults() {
        CommandLine cli = App.commandLine();
        StringWriter err = new StringWriter();
        cli.setErr(new PrintWriter(err));

        int exit = cli.execute("list", "-v", "--tune", "parquet.writers=4");

        assertThat(exit).isEqualTo(2); // no URI: stops after tune validation/echo, before store I/O
        assertThat(err.toString()).contains(
                        "tune effective: engine.readahead=off, seed.mode=shallow, "
                        + "parquet.writers=4, summary.interval=PT30S, "
                        + "sort.merge-parallelism=" + SortConfig.DEFAULT.mergeParallelism() + ", "
                        + "sort.finalization=pipeline, "
                        + "sort.keep-staging=off, "
                        + "sort.ignore-disk-check=off");
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void verboseModeReportsSystemPropertyMergeParallelismUsedByTheRun() {
        String key = "swath.sort.merge-parallelism";
        String previous = System.getProperty(key);
        try {
            System.setProperty(key, "7");
            CommandLine cli = App.commandLine();
            StringWriter err = new StringWriter();
            cli.setErr(new PrintWriter(err));

            int exit = cli.execute("list", "-v");

            assertThat(exit).isEqualTo(2); // no URI: stops after tune validation/echo
            assertThat(err.toString()).contains("sort.merge-parallelism=7");
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    @Test
    void mergeParallelismIsResumeSafeAndOverridesTheResolvedSortConfig() throws Exception {
        TuneOptions tune = new TuneOptions();
        // The computed default is capped at 8, so 16 proves that the typed value wins.
        tune.entries = List.of("sort.merge-parallelism=16");
        SortOptions sorting = new SortOptions();

        assertThat(tune.applyForResume(sorting, new PrintWriter(new StringWriter()))).isFalse();
        assertThat(sorting.resolveConfig().mergeParallelism()).isEqualTo(16);
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void finalizationIsResumeSafe() throws Exception {
        String property = SortConfig.FINALIZATION_PROPERTY;
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "pipeline");
            TuneOptions tune = new TuneOptions();
            tune.entries = List.of(SortConfig.FINALIZATION_TUNE_KEY + "=pipeline");
            SortOptions sorting = new SortOptions();

            assertThat(tune.applyForResume(sorting, new PrintWriter(new StringWriter()))).isFalse();
            assertThat(sorting.resolveConfig().finalization()).isEqualTo(SortFinalization.PIPELINE);
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    @Test
    void keepStagingIsResumeSafeAndOverridesTheResolvedSortConfig() throws Exception {
        TuneOptions tune = new TuneOptions();
        tune.entries = List.of(SortConfig.KEEP_STAGING_TUNE_KEY + "=on");
        SortOptions sorting = new SortOptions();

        assertThat(tune.applyForResume(sorting, new PrintWriter(new StringWriter()))).isFalse();
        assertThat(sorting.resolveConfig().stagingRetention()).isEqualTo(StagingRetention.RETAIN_ORIGINALS);
    }

    @Test
    void tuningDocumentationKeyAndRangeTableMatchesTheRegistry() throws Exception {
        Path doc = Path.of("..", "docs", "configuration.md");
        if (!Files.exists(doc)) {
            doc = Path.of("docs", "configuration.md");
        }
        Map<String, String> documented = new LinkedHashMap<>();
        boolean inTuneTable = false;
        for (String line : Files.readAllLines(doc)) {
            if (line.startsWith("## Tuning (`--tune`)")) {
                inTuneTable = true;
                continue;
            }
            if (inTuneTable && line.startsWith("#")) {
                break; // the next heading closes the tune-knob table window
            }
            if (!inTuneTable || !line.startsWith("| `")) {
                continue;
            }
            int keyEnd = line.indexOf('`', 3);
            int rangeStart = line.indexOf('|', keyEnd) + 1;
            int rangeEnd = line.indexOf('|', rangeStart);
            String key = line.substring(3, keyEnd);
            String range = line.substring(rangeStart, rangeEnd).trim().replace("`", "");
            documented.put(key, range);
        }

        Map<String, String> registered = new LinkedHashMap<>();
        for (TuneOptions.KeySpec spec : TuneOptions.registry()) {
            registered.put(spec.key(), spec.documentationRange());
        }
        assertThat(documented).isEqualTo(registered);
    }

    private static ListCommand parsed(String... options) {
        ListCommand cmd = new ListCommand();
        String[] args = new String[options.length + 1];
        args[0] = "s3://bucket/prefix";
        System.arraycopy(options, 0, args, 1, options.length);
        new CommandLine(cmd).parseArgs(args);
        cmd.syncArgGroups();
        return cmd;
    }
}
