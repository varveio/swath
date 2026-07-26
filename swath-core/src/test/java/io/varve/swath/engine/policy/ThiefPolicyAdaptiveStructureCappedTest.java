/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.WorkerState;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Characterization test for {@code ADAPTIVE_STRUCTURE_CAPPED} — the parent-empty sliver's
 * coarse→fine structure back-out (algorithms.md §3, {@code adaptiveStructurePivot}) winning on a
 * probe whose page was truncated. Called out explicitly (independent reviewer, decision-trace-
 * goldens campaign) as the thinnest-pinned pivot mechanism: exactly one golden event in one
 * fixture, sitting at the narrow intersection of TWO conditions (the adaptive back-out engaging AT
 * ALL, and the winning probe specifically being capped) — so it is the branch most likely to suffer
 * unnoticed extraction drift. This test drives it directly and in isolation.
 *
 * <p>Shape: {@code cursor = "2022/03/05/abc"}, {@code hi = "2022/04/"} — cursor and {@code hi}
 * diverge at ADJACENT code points ({@code '3'}/{@code '4'}, the day boundary), the same real-bucket
 * pattern algorithms.md's own {@code adaptiveStructurePivot} javadoc uses (a {@code
 * YYYY/MM/DD/uuid} keyspace). The placed pivot is therefore the degenerate cursor-adjacent MIN_SAFE
 * sliver, and it HITS — the parent-empty case. The coarsest level ({@code "2022/"}) yields nothing;
 * the next-finer level ({@code "2022/03/"}) yields a TRUNCATED sample of two boundaries, so the
 * cascade takes the FURTHEST one rather than the (unaffordable) true median.
 */
class ThiefPolicyAdaptiveStructureCappedTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void adaptiveStructureBackOutTakesTheFurthestBoundaryOfATruncatedSampleAtTheSecondLevel() {
        byte[] lo = b("2022/");
        byte[] cursor = b("2022/03/05/abc");
        byte[] hi = b("2022/04/");
        var digest = new WorkerState(1, lo, cursor, hi).alphabetDigest().snapshot();
        StealAttemptView view = new StealAttemptView(1, lo, cursor, hi, 0, 0.5, digest, false, 0, 0);

        // consecutiveZeroFanoutStructureProbes/consecutiveTimedOutStructureProbes are both 0 above,
        // so structureProbingEnabled's escape hatch is never reached -- this stub is never drawn from.
        ThiefPolicy policy = new ThiefPolicy(EngineToggles.DEFAULT, new byte[0], bound -> 0);
        StealAttempt attempt = policy.beginAttempt(view);

        // 1. The placed pivot (the cursor-adjacent sliver "2022/03/05/abc ") hits.
        StealAction afterPlacement = attempt.start();
        assertThat(afterPlacement).isInstanceOf(RequestKeyProbe.class);
        StealAction afterCoarsestLevel = attempt.onProbeResult(new KeyProbeOutcome(true));

        // 2. The coarsest level ("2022/") yields no boundary -- descend one level finer.
        assertThat(afterCoarsestLevel).isInstanceOf(RequestStructureProbe.class);
        assertThat(((RequestStructureProbe) afterCoarsestLevel).probePrefix()).isEqualTo(b("2022/"));
        StealAction afterSecondLevel = attempt.onProbeResult(new StructureProbeOutcome(List.of(), false));

        // 3. The next-finer level ("2022/03/") answers with a TRUNCATED two-boundary sample.
        assertThat(afterSecondLevel).isInstanceOf(RequestStructureProbe.class);
        assertThat(((RequestStructureProbe) afterSecondLevel).probePrefix()).isEqualTo(b("2022/03/"));
        StealAction result = attempt.onProbeResult(new StructureProbeOutcome(
                List.of(b("2022/03/06/"), b("2022/03/9")), true));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.ADAPTIVE_STRUCTURE_CAPPED);
        // The furthest boundary of the truncated sample -- NOT the median ("2022/03/06/") a complete
        // sample would have taken; the sample is only a PREFIX of the directory's true children, so
        // its median would sit near the cursor rather than the middle of the range.
        assertThat(commit.pivot()).isEqualTo(b("2022/03/9"));
        assertThat(commit.engagements()).contains(new Engagement("STRUCTURE", "fanout_capped"));
    }
}
