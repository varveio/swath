/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.sort.SortMode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides sorted-serving eligibility for a resolved file set — extracted out of {@code
 * ReplayServingFactory} (its original, and still its only server-side caller) so a second caller
 * outside {@code .server} can reuse the exact decision rather than restating it. Every resolved
 * file — not just the first — must carry a recognized sortedness stamp (§0.9), be {@code objects}
 * mode (§0.6), and a {@code format_version} this reader knows. The resolved set must then prove
 * multi-file completeness ({@link #multiFileCompletenessViolation}) before the index is even
 * attempted, and finally load a globally strictly-ascending, provably pure-{@code OBJECT} index
 * (§0.5 / the mixed-row-type fix).
 */
public final class SortedEligibility {

    private static final Logger log = LoggerFactory.getLogger(SortedEligibility.class);

    private SortedEligibility() {
    }

    /** The outcome of {@link #decide}: either the routing index, or a typed decline reason. */
    public sealed interface Result {

        record Eligible(List<IndexEntry> index) implements Result {
        }

        record Ineligible(String reason) implements Result {
        }
    }

    /**
     * Decides whether {@code files} can be served sorted, deriving the routing index as a side
     * effect of the decision (the derive work is unavoidable either way — the eligibility checks
     * that don't need it run first, cheaply, so a fixture that fails on stamp/mode/completeness
     * grounds never pays for it).
     *
     * @param recordFallbackOnFailure forwarded to {@link SortedFixtures#loadIndex} — {@code false}
     *                                when a decline is a hard failure rather than a "fallback"
     */
    public static Result decide(List<Path> files, FixtureMetrics fixtureMetrics,
                                boolean recordFallbackOnFailure) {
        if (files.isEmpty()) {
            return new Result.Ineligible(SortedFixtures.NO_STAMP);
        }
        List<SortedParquetStamp> stamps = new ArrayList<>(files.size());
        for (Path file : files) {
            Optional<SortedParquetStamp> stamp = detect(file);
            if (stamp.isEmpty()) {
                log.info("sorted_eligibility: {} carries no recognized sortedness stamp", file);
                return new Result.Ineligible(SortedFixtures.NO_STAMP);
            }
            if (stamp.get().mode() != SortMode.OBJECTS) {
                log.info("sorted_eligibility: {} is stamped mode={}, not objects", file, stamp.get().mode());
                return new Result.Ineligible(SortedFixtures.UNSUPPORTED_MODE);
            }
            if (!stamp.get().isKnownFormatVersion()) {
                log.info("sorted_eligibility: {} is stamped with an unrecognized format_version={}",
                        file, stamp.get().formatVersion());
                return new Result.Ineligible(SortedFixtures.UNKNOWN_FORMAT_VERSION);
            }
            stamps.add(stamp.get());
        }
        String violation = multiFileCompletenessViolation(files, stamps);
        if (violation != null) {
            log.info("sorted_eligibility: multi-file completeness check failed for {} — {}", files, violation);
            return new Result.Ineligible(SortedFixtures.INCOMPLETE_MULTIFILE);
        }
        IndexLoadResult loaded = loadIndex(files, fixtureMetrics, recordFallbackOnFailure);
        return switch (loaded) {
            case IndexLoadResult.Loaded l -> new Result.Eligible(l.entries());
            case IndexLoadResult.SanityFailed f -> new Result.Ineligible(f.reason());
        };
    }

    /**
     * Proves the resolved file set is a COMPLETE multi-file (or single-file) sorted
     * output, self-describingly from each file's own stamp — never a sidecar. For {@code N} files
     * actually present, their {@code file_index} values must be exactly {@code 1..N} (contiguous, no
     * gaps, no duplicates — a gap or an out-of-range index means a file is missing or extra), and
     * exactly one file — the one at index {@code N} — must carry {@code file_final=true}. This is
     * exactly what a crash between renaming files {@code 1..k} and {@code k+1} of an {@code N}-file
     * publish leaves behind: the observed set is {@code 1..k} (contiguous — so the gap alone doesn't
     * catch it), but NONE of them is stamped final (the true final file, index {@code N > k}, was
     * never renamed into place) — so the missing-final check catches it. A stray/duplicated file
     * reusing an index, or a file whose index exceeds the observed count, fails the contiguity check
     * instead. Returns {@code null} when the set is complete; otherwise a detail string for logs.
     */
    static String multiFileCompletenessViolation(List<Path> files, List<SortedParquetStamp> stamps) {
        int n = files.size();
        boolean[] seen = new boolean[n + 1];   // 1-based; seen[0] unused
        int finalCount = 0;
        int finalIndex = -1;
        for (int i = 0; i < n; i++) {
            SortedParquetStamp stamp = stamps.get(i);
            int index = stamp.fileIndex();
            if (index < 1 || index > n || seen[index]) {
                return "file " + files.get(i) + " reports file_index=" + index
                        + ", which is not a valid position among " + n + " file(s) actually present "
                        + "(indices must be exactly 1.." + n + ", contiguous, no duplicates)";
            }
            seen[index] = true;
            if (stamp.fileFinal()) {
                finalCount++;
                finalIndex = index;
            }
        }
        if (finalCount != 1) {
            return "expected exactly one file stamped file_final=true across " + n
                    + " file(s), found " + finalCount + " — a crash mid-multi-file-publish leaves the true "
                    + "final file unrenamed, so this set is an incomplete prefix of the true output";
        }
        if (finalIndex != n) {
            return "the file_final=true flag is on file_index=" + finalIndex + ", expected the max index " + n;
        }
        return null;
    }

    private static Optional<SortedParquetStamp> detect(Path file) {
        try {
            return SortedFixtures.detect(file);
        } catch (IOException e) {
            // An unreadable/foreign footer is simply "not stamped" for routing purposes.
            return Optional.empty();
        }
    }

    private static IndexLoadResult loadIndex(List<Path> files, FixtureMetrics fixtureMetrics,
                                             boolean recordFallbackOnFailure) {
        try {
            return SortedFixtures.loadIndex(files, fixtureMetrics, recordFallbackOnFailure);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to derive the sorted-fixture index", e);
        }
    }
}
