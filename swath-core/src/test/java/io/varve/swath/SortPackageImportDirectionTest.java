/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Freezes today's sort/output seam inventory so it cannot silently grow before module extraction.
 *
 * <p>This is deliberately a source scan: package access is not a useful proxy for the intended
 * future module graph.
 */
class SortPackageImportDirectionTest {

    private static final Path SOURCE_ROOT = Path.of("src/main/java");
    private static final Pattern IMPORT = Pattern.compile(
            "^import\\s+(?:static\\s+)?(io\\.varve\\.swath(?:\\.[\\w]+|\\.\\*)+);\\s*$",
            Pattern.MULTILINE);
    private static final Set<String> SORTED_OUTPUT_SORT_IMPORTS = Set.of(
            "io.varve.swath.sort.FinalPart",
            "io.varve.swath.sort.FinalPassListener",
            "io.varve.swath.sort.SortConfig",
            "io.varve.swath.sort.SortMetrics",
            "io.varve.swath.sort.SortRun",
            "io.varve.swath.sort.finalize.PreparedSortedParts",
            "io.varve.swath.sort.finalize.SortFinalizer",
            "io.varve.swath.sort.spill.PageRunFormat");

    @Test
    void sortPackageImportsRespectTheFrozenModuleExtractionSeams() throws IOException {
        assertThat(SOURCE_ROOT).isDirectory();
        try (Stream<Path> paths = Files.walk(SOURCE_ROOT.resolve("io/varve/swath/sort"))) {
            for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                for (String imported : imports(source)) {
                    assertSortImportAllowed(source, imported);
                }
            }
        }
    }

    @Test
    void sortedOutputImportsOnlyTheFrozenSortRequestAndResultInventory() throws IOException {
        Path sortedOutput = SOURCE_ROOT.resolve("io/varve/swath/output/sorted");
        try (Stream<Path> paths = Files.walk(sortedOutput)) {
            for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                for (String imported : imports(source)) {
                    if (imported.startsWith("io.varve.swath.sort.")
                            && !SORTED_OUTPUT_SORT_IMPORTS.contains(imported)) {
                        violation(source, imported,
                                "output.sorted may import only the frozen facade/finalize/spill request-result inventory");
                    }
                }
            }
        }
    }

    private static void assertSortImportAllowed(Path source, String imported) {
        String relative = SOURCE_ROOT.relativize(source).toString().replace('\\', '/');
        if (relative.startsWith("io/varve/swath/sort/stage/")) {
            if (imported.startsWith("io.varve.swath.sort.finalize.")) {
                violation(source, imported, "sort.stage must never import sort.finalize");
            }
            if (imported.startsWith("io.varve.swath.output.") && !stagingAuthority(imported)) {
                violation(source, imported,
                        "sort.stage may import output.sorted only through the four staging-authority seams");
            }
        }

        if (relative.startsWith("io/varve/swath/sort/finalize/")
                && imported.startsWith("io.varve.swath.sort.stage.")) {
            violation(source, imported, "sort.finalize may import sort.spill and the facade, never sort.stage");
        }

        if (relative.startsWith("io/varve/swath/sort/spill/")) {
            if (imported.startsWith("io.varve.swath.sort.stage.")
                    || imported.startsWith("io.varve.swath.sort.finalize.")) {
                violation(source, imported,
                        "sort.spill may import only the facade, io.varve.swath.model, and JDK/compression/logging");
            }
            if (imported.startsWith("io.varve.swath.checkpoint.")
                    || imported.startsWith("io.varve.swath.runtime.")
                    || imported.startsWith("io.varve.swath.observability.")
                    || imported.startsWith("io.varve.swath.output.parquet.")) {
                violation(source, imported,
                        "sort.spill may import only the facade, io.varve.swath.model, and JDK/compression/logging");
            }
            if (imported.startsWith("io.varve.swath.")
                    && !imported.startsWith("io.varve.swath.model.")
                    && !imported.startsWith("io.varve.swath.sort.")
                    && !imported.startsWith("io.varve.swath.output.")) {
                violation(source, imported,
                        "sort.spill may import only the facade, io.varve.swath.model, and JDK/compression/logging");
            }
            if (imported.startsWith("io.varve.swath.output.")
                    && !imported.equals("io.varve.swath.output.dataset.DurableFiles")
                    && !stagingAuthority(imported)) {
                violation(source, imported,
                        "sort.spill may import only the facade, io.varve.swath.model, and JDK/compression/logging");
            }
        }

        if (imported.startsWith("io.varve.swath.output.")) {
            assertOutputSeamAllowed(source, imported);
        }
    }

    private static void assertOutputSeamAllowed(Path source, String imported) {
        String fileName = source.getFileName().toString();
        if (stagingAuthority(imported)) {
            // To-be-inverted module-extraction seam: staging authority remains output.sorted-owned.
            return;
        }
        if (fileName.equals("Durability.java")
                && imported.equals("io.varve.swath.output.dataset.DurableFiles")) {
            // To-be-inverted module-extraction seam: DurableFiles remains output.dataset-owned.
            return;
        }
        if (fileName.equals("CaptureSorter.java") && Set.of(
                "io.varve.swath.output.parquet.ParquetParts",
                "io.varve.swath.output.parquet.fixture.ParquetEntryReader",
                "io.varve.swath.output.parquet.sorted.SortedParquetWriterFactory",
                "io.varve.swath.output.sorted.SortedDatasetCommitter",
                "io.varve.swath.output.sorted.SortedDatasetCoordinator",
                "io.varve.swath.output.sorted.SortedDatasetResult").contains(imported)) {
            // To-be-relocated module-extraction seam: CaptureSorter is the fixture orchestrator.
            return;
        }
        if (fileName.equals("DefaultSortedFileWriter.java")
                && (imported.equals("io.varve.swath.output.parquet.ParquetSchema")
                || imported.equals("io.varve.swath.output.parquet.PartWriter"))) {
            // To-be-inverted module-extraction seam: plain part-writer implementation pending the
            // writer-SPI reachability re-audit and module move.
            return;
        }
        if (fileName.equals("SortFinalizer.java")
                && imported.equals("io.varve.swath.output.parquet.sorted.SortedParquetIndex")) {
            // To-be-inverted module-extraction seam: SPI carries the concrete index type until the
            // swath-sort module extraction abstracts it.
            return;
        }
        violation(source, imported, "sort packages may not import output types outside the frozen named seams");
    }

    private static boolean stagingAuthority(String imported) {
        return imported.equals("io.varve.swath.output.sorted.StagingNames")
                || imported.equals("io.varve.swath.output.sorted.StagingReconciliation")
                || imported.equals("io.varve.swath.output.sorted.StagingRetention")
                || imported.equals("io.varve.swath.output.sorted.StaleFinalSweep");
    }

    private static List<String> imports(Path source) throws IOException {
        return IMPORT.matcher(Files.readString(source)).results().map(match -> match.group(1)).toList();
    }

    private static void violation(Path source, String imported, String rule) {
        throw new AssertionError("Import direction violation: \"" + rule + "\"; "
                + SOURCE_ROOT.relativize(source) + " imports " + imported);
    }
}
