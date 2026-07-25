/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins {@link PageAwareMerger}'s two counter LITERALS to the constants of the same name.
 *
 * <p><b>Why this exists.</b> {@code scripts/ci/check-instrumentation-drift.sh} statically reconciles
 * every {@code recordStealReason} call site against the §5 registry table and can only resolve string
 * LITERALS — a named constant makes the counter a "ghost row" and fails the build. So {@code plan()}
 * must pass literals. But {@link PageRunSegmentReader#relabelled} re-labels those same two reasons onto
 * its own route's counters via an {@code equals} match against {@link PageAwareMerger#WHOLE_PAGE_EMITTED}
 * / {@link PageAwareMerger#OVERLAP_KEYMERGE}. The value is therefore duplicated by necessity — literal
 * at the emitter, constant at the matcher.
 *
 * <p>If a literal in {@code plan()} ever drifts from the constant beside it, the re-label would SILENTLY
 * stop matching: the intra-segment read signal would leak out under the cross-segment merge counter,
 * quietly corrupting the one metric ({@code page_overlap_keymerge}) that is documented as an invariant
 * alarm expected to be 0 in production. Nothing else would fail.
 *
 * <p>This test asserts on the reasons the merger ACTUALLY EMITS — not on the constants compared to
 * themselves, which would be tautological. Change a literal without changing its constant and this goes
 * red.
 */
class PageAwareMergerCounterNamesTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private int seq;

    @Test
    void theReasonsTheMergerEmitsAreExactlyTheConstantsOtherCodeMatchesOn(@TempDir Path dir)
            throws IOException {
        // Three pages that force BOTH paths in one run: [a..c] and [b..d] OVERLAP (key-merge fallback),
        // while [x..z] is disjoint from everything ahead of it (whole-page fast path). Page mins ascend
        // (a, b, x), so this is a perfectly legal segment — the min-regression guard stays silent.
        Path segment = dir.resolve("seg.pageseg");
        PageRunRawFixtures.writeRawPageRun(
                segment,
                List.of(
                        List.of(entry("a"), entry("c")),
                        List.of(entry("b"), entry("d")),
                        List.of(entry("x"), entry("z"))),
                cmp);

        List<String> reasons = new ArrayList<>();
        SortMetrics capturing = (outcome, reason) -> {
            assertThat(outcome).isEqualTo("SORT");
            reasons.add(reason);
        };

        List<String> merged = new ArrayList<>();
        try (PageFrontierReader frontier = new PageFrontierReader(segment, capturing);
                PageAwareMerger merger =
                        new PageAwareMerger(List.of(frontier), cmp, DuplicateHook.NO_OP, capturing)) {
            while (merger.hasNext()) {
                merged.add(merger.next().key().asString());
            }
        }

        // Sanity: the fixture really did exercise the merger (and produced a sorted run).
        assertThat(merged).containsExactly("a", "b", "c", "d", "x", "z");

        // The key assertion: every reason emitted equals one of the two constants, and BOTH paths fired. If a
        // literal in plan() drifts from the constant beside it, the emitted string stops equalling the
        // constant and this goes red — which is exactly the silent-drift case PageRunSegmentReader's
        // equals-match depends on. Asserting the constants against themselves would prove nothing;
        // these are the strings the merger ACTUALLY emitted.
        assertThat(reasons)
                .containsOnly(PageAwareMerger.WHOLE_PAGE_EMITTED, PageAwareMerger.OVERLAP_KEYMERGE)
                .contains(PageAwareMerger.WHOLE_PAGE_EMITTED)
                .contains(PageAwareMerger.OVERLAP_KEYMERGE);
    }

    private ListEntry entry(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null,
                "v" + String.format("%08d", seq), false, null, null, null, null);
    }
}
