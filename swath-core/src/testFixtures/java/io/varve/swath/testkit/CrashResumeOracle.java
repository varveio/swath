/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.fail;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.output.parquet.DatasetLayout;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The crash-resume "identical-to-clean" oracle. Compares a <b>resumed</b> swath output
 * directory against a <b>clean-baseline</b> output directory — both are swath dataset roots
 * ({@code <root>/data/*.parquet}) — and asserts the resume produced exactly the output a single
 * un-killed run would have.
 *
 * <p>The parametric external-kill harness consumes this via {@link
 * #assertResumeMatchesBaseline(Path, Path, boolean)}: given the two roots and a {@code sorted}
 * flag, one call asserts the whole contract. The pieces are also exposed individually so a test
 * can assert just the set-only contract (unsorted) or just the global-order contract (sorted).
 *
 * <p>Three invariants:
 * <ol>
 *   <li><b>Same object set</b> — the resumed key set equals the baseline's, and
 *       {@code count == distinct == baseline count} (no missing, no extra, no duplicate).</li>
 *   <li><b>Exactly-once</b> — across all parts the union size equals the distinct size (no key
 *       appears in two parts, or twice in one).</li>
 *   <li><b>--sort global order</b> (the key assertion) — concatenating every part in <b>part-name lexical
 *       order</b> yields a key sequence that is (a) strictly ascending in unsigned-byte order with
 *       zero out-of-order adjacent pairs and (b) identical, row-for-row, to the baseline's
 *       concatenated ordered sequence. "Byte-identical global order" is this ordered-key-sequence
 *       equality — NOT literal file bytes (Parquet footer/metadata legitimately varies).</li>
 * </ol>
 *
 * <p>Parts are enumerated through {@link DatasetLayout#dataParts()} (globs {@code data/*.parquet},
 * ordered by filename); the oracle never assumes a specific part-file name prefix. A footerless /
 * open part (the {@code kill -9} signature) is skipped exactly as the resume disk-sweep discards it;
 * any keys that were genuinely lost thereby surface as a <i>missing-key</i> failure in the set
 * comparison.
 *
 * <p>All failures are AssertionErrors with an actionable message naming the first offending key.
 */
public final class CrashResumeOracle {

    private CrashResumeOracle() {
    }

    /**
     * Assert the resumed output is identical-to-clean. Always checks the same-object-set and
     * exactly-once contracts; when {@code sorted} is true additionally checks the global-order
     * contract. This is the single entry point the external-kill harness calls.
     */
    public static void assertResumeMatchesBaseline(Path baselineRoot, Path resumedRoot, boolean sorted)
            throws IOException {
        List<String> baseline = orderedKeys(baselineRoot);
        List<String> resumed = orderedKeys(resumedRoot);
        assertExactlyOnce(resumed);
        assertSameObjectSet(baseline, resumed);
        if (sorted) {
            assertSortedGlobalOrder(baseline, resumed);
        }
    }

    /**
     * The set-only contract (unsorted output): resumed key set equals baseline's and
     * {@code count == distinct == baseline count}. Also enforces exactly-once on the resumed side.
     */
    public static void assertSameObjectSet(Path baselineRoot, Path resumedRoot) throws IOException {
        List<String> baseline = orderedKeys(baselineRoot);
        List<String> resumed = orderedKeys(resumedRoot);
        assertExactlyOnce(resumed);
        assertSameObjectSet(baseline, resumed);
    }

    /**
     * The global-order contract (sorted output): the resumed concatenation is strictly ascending
     * (unsigned-byte order, zero out-of-order pairs) and identical row-for-row to the baseline's.
     * Also enforces the set and exactly-once contracts, so this is a superset check for {@code
     * --sort} runs.
     */
    public static void assertSortedGlobalOrderMatches(Path baselineRoot, Path resumedRoot) throws IOException {
        List<String> baseline = orderedKeys(baselineRoot);
        List<String> resumed = orderedKeys(resumedRoot);
        assertExactlyOnce(resumed);
        assertSameObjectSet(baseline, resumed);
        assertSortedGlobalOrder(baseline, resumed);
    }

    // ------------------------------------------------------------------------
    // Contract checks (operate on already-materialized key sequences).
    // ------------------------------------------------------------------------

    private static void assertExactlyOnce(List<String> resumed) {
        Set<String> seen = new LinkedHashSet<>(resumed.size());
        for (String key : resumed) {
            if (!seen.add(key)) {
                fail("exactly-once violated: key appears more than once across parts (in part-name order) "
                        + "— first duplicate: '%s' (union=%d, distinct=%d)", key, resumed.size(), seen.size());
            }
        }
    }

    private static void assertSameObjectSet(List<String> baseline, List<String> resumed) {
        Set<String> baseSet = new LinkedHashSet<>(baseline);
        Set<String> resSet = new LinkedHashSet<>(resumed);

        // Missing: in baseline, absent from resumed — the data-loss failure a crash could cause.
        for (String key : baseline) {
            if (!resSet.contains(key)) {
                fail("same-object-set violated: resumed output is MISSING a baseline key "
                        + "— first missing: '%s' (baseline distinct=%d, resumed distinct=%d)",
                        key, baseSet.size(), resSet.size());
            }
        }
        // Extra: in resumed, absent from baseline — a foreign / spurious key.
        for (String key : resumed) {
            if (!baseSet.contains(key)) {
                fail("same-object-set violated: resumed output has an EXTRA key not in baseline "
                        + "— first extra: '%s' (baseline distinct=%d, resumed distinct=%d)",
                        key, baseSet.size(), resSet.size());
            }
        }
        // Counts: count == distinct == baseline count (duplicate keys already caught above, but this
        // keeps the invariant explicit and gives a clean count-mismatch message).
        if (resumed.size() != baseline.size()) {
            fail("same-object-set violated: resumed count %d != baseline count %d "
                    + "(baseline distinct=%d, resumed distinct=%d)",
                    resumed.size(), baseline.size(), baseSet.size(), resSet.size());
        }
    }

    private static void assertSortedGlobalOrder(List<String> baseline, List<String> resumed) {
        // (a) strictly ascending in unsigned-byte order, zero out-of-order adjacent pairs.
        for (int i = 1; i < resumed.size(); i++) {
            String prev = resumed.get(i - 1);
            String cur = resumed.get(i);
            int cmp = compareUnsigned(prev, cur);
            if (cmp > 0) {
                fail("global-order violated: resumed keys are OUT OF ORDER at part-order index %d "
                        + "— '%s' precedes '%s' but sorts after it", i, prev, cur);
            }
            if (cmp == 0) {
                fail("global-order violated: resumed keys are NOT STRICTLY ascending at part-order index %d "
                        + "— '%s' is repeated", i, cur);
            }
        }
        // (b) identical, row-for-row, to the baseline's concatenated ordered sequence.
        int n = Math.min(baseline.size(), resumed.size());
        for (int i = 0; i < n; i++) {
            if (!baseline.get(i).equals(resumed.get(i))) {
                fail("global-order violated: resumed order differs from baseline at part-order index %d "
                        + "— baseline has '%s', resumed has '%s'", i, baseline.get(i), resumed.get(i));
            }
        }
        if (baseline.size() != resumed.size()) {
            fail("global-order violated: resumed sequence length %d != baseline length %d",
                    resumed.size(), baseline.size());
        }
    }

    // ------------------------------------------------------------------------
    // Reading: concatenate every part's keys in part-name lexical order.
    // ------------------------------------------------------------------------

    /**
     * Every {@code key} value under {@code <root>/data/}, concatenated across parts in part-name
     * lexical order (via {@link DatasetLayout#dataParts()}). A footerless / unreadable part is
     * skipped — a completed resume never leaves one, and any keys thereby lost surface as a
     * missing-key failure in {@link #assertSameObjectSet}.
     */
    static List<String> orderedKeys(Path root) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(root).dataParts()) {
            try {
                keys.addAll(ParquetReads.keys(part));
            } catch (IOException footerlessOpenPart) {
                // kill -9 signature: an in-flight part with no footer. Resume discards it; skip here.
            }
        }
        return keys;
    }

    private static int compareUnsigned(String a, String b) {
        return KeyBytes.compareUnsigned(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
