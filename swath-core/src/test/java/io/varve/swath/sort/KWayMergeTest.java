/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.drain;
import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@link KWayMerge} over in-memory sorted segments: ordering, whole-page disjoint forwarding,
 * overlapping-page row merge, duplicate-hook coverage, cascade beyond a tiny fan-in, and the
 * fd-bound note.
 */
class KWayMergeTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void mergesDisjointSegmentsInOrder() throws IOException {
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer a = io.add(List.of(object("a"), object("b"), object("c")));
        Integer b = io.add(List.of(object("x"), object("y"), object("z")));

        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, DuplicateHook.NO_OP, SortMetrics.NO_OP);
        List<ListEntry> out = drain(merge.merge(new ArrayList<>(List.of(a, b))));

        assertThat(out).containsExactly(object("a"), object("b"), object("c"),
                object("x"), object("y"), object("z"));
        assertThat(merge.mergePasses()).isEqualTo(1);
        assertThat(merge.cascadedPasses()).isZero();
    }

    @Test
    void interleavedSegmentsMergeInOrder() throws IOException {
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer a = io.add(List.of(object("a"), object("c"), object("e")));
        Integer b = io.add(List.of(object("b"), object("d"), object("f")));

        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, DuplicateHook.NO_OP, SortMetrics.NO_OP);
        List<ListEntry> out = drain(merge.merge(new ArrayList<>(List.of(a, b))));

        assertThat(out).containsExactly(object("a"), object("b"), object("c"),
                object("d"), object("e"), object("f"));
    }

    @Test
    void disjointSegmentsExerciseThePageWholePath() throws IOException {
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        // A wholly precedes B, so both pages can be forwarded whole.
        Integer a = io.add(List.of(object("a"), object("b"), object("c")));
        Integer b = io.add(List.of(object("x"), object("y"), object("z")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, DuplicateHook.NO_OP, metrics);
        drain(merge.merge(new ArrayList<>(List.of(a, b))));

        assertThat(metrics.count("SORT.cascade_page_whole_merge")).isEqualTo(2);
        assertThat(metrics.count("SORT.cascade_page_overlap_merge")).isZero();
    }

    @Test
    void interleavedSegmentsUseTheOverlapPath() throws IOException {
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer a = io.add(List.of(object("a"), object("c")));
        Integer b = io.add(List.of(object("b"), object("d")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, DuplicateHook.NO_OP, metrics);
        drain(merge.merge(new ArrayList<>(List.of(a, b))));

        assertThat(metrics.count("SORT.cascade_page_overlap_merge")).isEqualTo(1);
    }

    @Test
    void emptySegmentsAreReportedOnceWithoutChangingNonEmptyClassification() throws IOException {
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer emptyA = io.add(List.of());
        Integer nonEmpty = io.add(List.of(object("a"), object("b")));
        Integer emptyB = io.add(List.of());

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, DuplicateHook.NO_OP, metrics);
        List<ListEntry> out = drain(merge.merge(new ArrayList<>(List.of(emptyA, nonEmpty, emptyB))));

        assertThat(out).containsExactly(object("a"), object("b"));
        assertThat(metrics.count("SORT.cascade_page_empty_segment")).isEqualTo(2);
        assertThat(metrics.count("SORT.cascade_page_whole_merge")).isEqualTo(1);
        assertThat(metrics.count("SORT.cascade_page_overlap_merge")).isZero();
        assertThat(metrics.count("SORT.merge_disjoint_copyable")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_interleaved_segment")).isZero();
    }

    @Test
    void duplicateHookFiresOnAdjacentEqualEntries() throws IOException {   // I3
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer a = io.add(List.of(object("dup"), object("later")));
        Integer b = io.add(List.of(object("dup")));   // equal key+version+type

        List<String> reported = new ArrayList<>();
        DuplicateHook hook = (prev, cur) -> reported.add(cur.key().asString());

        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, hook, SortMetrics.NO_OP);
        List<ListEntry> out = drain(merge.merge(new ArrayList<>(List.of(a, b))));

        assertThat(out).hasSize(3);   // never dropped — swath is a sorter, not a deduper
        assertThat(reported).containsExactly("dup");
    }

    @Test
    void duplicateHookDoesNotFireWithoutAdjacentEquals() throws IOException {   // I4
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer a = io.add(List.of(object("a"), object("b")));
        Integer b = io.add(List.of(object("c"), object("d")));

        List<String> reported = new ArrayList<>();
        DuplicateHook hook = (prev, cur) -> reported.add(cur.key().asString());

        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, hook, SortMetrics.NO_OP);
        drain(merge.merge(new ArrayList<>(List.of(a, b))));

        assertThat(reported).isEmpty();
    }

    @Test
    void fullyDisjointSegmentsAreAllReportedCopyable() throws IOException {
        // 3 segments whose key ranges never overlap another's are each
        // emitted as one uninterrupted run -> all 3 are "disjoint copyable", none "interleaved".
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer a = io.add(List.of(object("a"), object("b"), object("c")));
        Integer b = io.add(List.of(object("d"), object("e")));
        Integer c = io.add(List.of(object("x"), object("y"), object("z")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, DuplicateHook.NO_OP, metrics);
        drain(merge.merge(new ArrayList<>(List.of(a, b, c))));

        assertThat(metrics.count("SORT.merge_disjoint_copyable")).isEqualTo(3);
        assertThat(metrics.count("SORT.merge_interleaved_segment")).isZero();
    }

    @Test
    void interleavedSegmentsAreReportedInterleavedNotCopyable() throws IOException {
        // A and B's ranges overlap (a,c,e vs b,d,f), so the merge repeatedly switches away from and
        // back to each stream -> both are "interleaved", neither "disjoint copyable". C is untouched
        // by either and stays a single run -> copyable.
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer a = io.add(List.of(object("a"), object("c"), object("e")));
        Integer b = io.add(List.of(object("b"), object("d"), object("f")));
        Integer c = io.add(List.of(object("x"), object("y")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 512, io, DuplicateHook.NO_OP, metrics);
        drain(merge.merge(new ArrayList<>(List.of(a, b, c))));

        assertThat(metrics.count("SORT.merge_interleaved_segment")).isEqualTo(2);
        assertThat(metrics.count("SORT.merge_disjoint_copyable")).isEqualTo(1);
    }

    @Test
    void disjointCountsAccumulateAcrossACascadePassAndTheFinalPass() throws IOException {
        // fanIn=2 with 3 segments forces exactly one cascade pass (KWayMerge.onePass), so this
        // exercises the onePass lambda's accumulation (KWayMerge.java's passCopyable/passInterleaved +
        // recordDisjoint), not just merge()'s final-streaming-pass wiring.
        //
        // Cascade pass (onePass), fanIn=2 groups the 3 segments as [a, b] then [c]:
        //   group [a, b]: a=[a,c], b=[b,d] overlap -> both interleaved (+2 interleaved this group).
        //   group [c]:    a lone segment is trivially one run -> copyable (+1 copyable this group).
        //   -> cascade pass totals: copyable=1, interleaved=2.
        // Final pass (merge()) over the cascade's 2 intermediate segments, [a,b,c,d] and [x,y]:
        //   disjoint ranges -> both copyable (+2 copyable, +0 interleaved).
        // Grand totals across the whole merge: copyable=1+2=3, interleaved=2+0=2.
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        Integer a = io.add(List.of(object("a"), object("c")));
        Integer b = io.add(List.of(object("b"), object("d")));
        Integer c = io.add(List.of(object("x"), object("y")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 2, io, DuplicateHook.NO_OP, metrics);
        List<ListEntry> out = drain(merge.merge(new ArrayList<>(List.of(a, b, c))));

        assertThat(out).containsExactly(object("a"), object("b"), object("c"), object("d"),
                object("x"), object("y"));
        assertThat(merge.cascadedPasses()).isEqualTo(1);   // sanity: the cascade path actually ran
        assertThat(metrics.count("SORT.merge_disjoint_copyable")).isEqualTo(3);
        assertThat(metrics.count("SORT.merge_interleaved_segment")).isEqualTo(2);
    }

    @Test
    void cascadesWhenSegmentCountExceedsFanIn() throws IOException {
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        List<Integer> segments = new ArrayList<>();
        List<String> keys = List.of("e", "b", "d", "a", "c");
        for (String k : keys) {
            segments.add(io.add(List.of(object(k))));
        }

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 2, io, DuplicateHook.NO_OP, metrics);
        List<ListEntry> out = drain(merge.merge(segments));

        assertThat(out).containsExactly(object("a"), object("b"), object("c"), object("d"), object("e"));
        assertThat(merge.mergePasses()).isGreaterThan(1);
        assertThat(merge.cascadedPasses()).isGreaterThan(0);
        assertThat(metrics.count("SORT.merge_pass_cascaded")).isEqualTo((int) merge.cascadedPasses());
    }

    @Test
    void cascadeDeletesOnlyItsOwnIntermediatesKeepingScratchBounded() throws IOException {
        // fd-bound / scratch-bound note: with 5 inputs at fan-in 2, intermediates the merge created on
        // an EARLIER pass are deleted once a later pass folds them in, so scratch stays bounded; but
        // ORIGINALS are never deleted by the merge itself — only the caller, after a successful
        // publish, may reclaim them (crash-recoverability fix; see KWayMerge's class javadoc and the
        // crashMidCascade... regression at SortMergeReentryContractTest).
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        List<Integer> segments = new ArrayList<>();
        for (String k : List.of("e", "b", "d", "a", "c")) {
            segments.add(io.add(List.of(object(k))));
        }
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 2, io, DuplicateHook.NO_OP, SortMetrics.NO_OP);
        try (SortedCursor cursor = merge.merge(segments)) {
            while (cursor.hasNext()) {
                cursor.next();
            }
        }
        // Every original still exists — never deleted by the merge itself.
        assertThat(segments).allMatch(io::isLive);
        // Plus at most fanIn surviving intermediates from the final round (also not deleted by the
        // merge — the caller reclaims those alongside the originals, post-publish).
        assertThat(io.liveCount() - segments.size()).isLessThanOrEqualTo(2);
    }

    @Test
    void effectiveFanInCapsPeakOpenStreamsPerPassUnderATinyMergeBudget() throws IOException {
        // I11: each open stream holds ≈ one merge-per-stream-bytes of
        // heap, so the pass width KWayMerge is CONSTRUCTED with is what bounds merge-phase memory —
        // SortConfig#effectiveFanIn() (via merge-budget-bytes / merge-per-stream-bytes), not the raw
        // fan-in knob alone. Proof: a generous raw fan-in (512) would let 10 segments merge in a single
        // pass (peak open = 10); a tiny merge-budget (3 × 64 KiB) forces effectiveFanIn()=3, and the
        // observed peak must never exceed that.
        SortConfig config = SortConfig.fromProperties(key -> switch (key) {
            case "swath.sort.fan-in" -> "512";
            case "swath.sort.merge-per-stream-bytes" -> String.valueOf(64L << 10);
            case "swath.sort.merge-budget-bytes" -> String.valueOf(3L * (64L << 10));
            default -> null;
        });
        assertThat(config.effectiveFanIn()).isEqualTo(3);

        SortTestSupport.PeakTrackingSegments io = new SortTestSupport.PeakTrackingSegments();
        List<Integer> segments = new ArrayList<>();
        for (String k : List.of("j", "i", "h", "g", "f", "e", "d", "c", "b", "a")) {
            segments.add(io.add(List.of(object(k))));
        }

        KWayMerge<Integer> merge = new KWayMerge<>(cmp, config.effectiveFanIn(), io, DuplicateHook.NO_OP,
                SortMetrics.NO_OP);
        List<ListEntry> out = drain(merge.merge(segments));

        assertThat(io.peakOpen()).isLessThanOrEqualTo(3);
        assertThat(out).containsExactly(object("a"), object("b"), object("c"), object("d"), object("e"),
                object("f"), object("g"), object("h"), object("i"), object("j"));   // cascade absorbs the rest
        assertThat(merge.cascadedPasses()).isGreaterThan(0);   // the budget, not the raw fan-in, forced this
    }

    @Test
    void progressCallbackAdvancesDuringIntermediateCascadePassesNotJustTheFinalPass() throws IOException {
        // merge() runs every cascade pass eagerly BEFORE returning the final cursor, and wires the
        // progress callback into every cascade pass's drain too, not just the FINAL streaming pass's
        // by the caller while draining — so a cascade that takes minutes advances
        // swath.progress.units throughout. fanIn=2 with 5 segments
        // forces TWO cascade passes (5 -> 3 -> 2 intermediates) before the final streaming pass.
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        List<Integer> segments = new ArrayList<>();
        for (String k : List.of("e", "b", "d", "a", "c")) {
            segments.add(io.add(List.of(object(k))));
        }
        KWayMerge<Integer> merge = new KWayMerge<>(cmp, 2, io, DuplicateHook.NO_OP, SortMetrics.NO_OP);

        List<Long> batches = new ArrayList<>();
        // merge() runs every cascade pass to completion before returning the cursor at all (see
        // KWayMerge#merge), so every intermediate-pass batch is already in hand at this point —
        // strictly BEFORE the final (unwrapped, caller-owned) streaming pass below is ever drained.
        SortedCursor cursor = merge.merge(segments, batches::add);
        assertThat(merge.cascadedPasses()).isEqualTo(2);   // sanity: this really did cascade twice
        long fromCascadesBeforeFinalPassDrained = batches.stream().mapToLong(Long::longValue).sum();
        assertThat(fromCascadesBeforeFinalPassDrained).isGreaterThan(0);

        List<ListEntry> out = drain(cursor);   // exhausts the final streaming pass
        assertThat(out).containsExactly(object("a"), object("b"), object("c"), object("d"), object("e"));

        // Every cascade pass re-derives the full row set (5 rows), so 2 cascade passes report
        // 2 * 5 = 10 -- strictly more than the 5 rows the final pass alone drains, proving the
        // intermediate passes are genuinely counted here, not just piggybacking on the final drain.
        assertThat(batches.stream().mapToLong(Long::longValue).sum()).isEqualTo(10L);
    }

    @Test
    void aCrashMidCascadeCanStillBeRedoneBecauseOriginalsSurvive() throws IOException {
        // A cascade (multi-pass) merge that crashes
        // partway through must be redoable from the SAME original segment list the checkpoint named —
        // which only holds if the merge never deleted them. Simulate the "crash" by simply never
        // draining the first merge's cursor (as if the process died mid-merge) and then re-running a
        // FRESH merge over the identical original segment list.
        SortTestSupport.InMemorySegments io = new SortTestSupport.InMemorySegments();
        List<Integer> segments = new ArrayList<>();
        for (String k : List.of("e", "b", "d", "a", "c")) {
            segments.add(io.add(List.of(object(k))));
        }
        KWayMerge<Integer> firstAttempt = new KWayMerge<>(cmp, 2, io, DuplicateHook.NO_OP, SortMetrics.NO_OP);
        firstAttempt.merge(segments);   // cascades at least once; "crashes" — the cursor is never drained

        // Redo: every original the checkpoint named must still be openable.
        assertThat(segments).allMatch(io::isLive);
        KWayMerge<Integer> redo = new KWayMerge<>(cmp, 2, io, DuplicateHook.NO_OP, SortMetrics.NO_OP);
        List<ListEntry> out = drain(redo.merge(segments));
        assertThat(out).containsExactly(object("a"), object("b"), object("c"), object("d"), object("e"));
    }
}
