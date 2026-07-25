/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * <b>The maintained best-first frontier reaches a late-discovered wide subtree before a
 * probe-hungry, early-discovered narrow one drains the wallet.</b> Regression guard for the
 * commoncrawl shape the old one-shot pre-pass reorder could not fix: a wide, genuinely heavy
 * subtree that only becomes visible several probes INTO the descent (because reaching it needs its
 * own parent probed first) sat behind, in plain insertion order, a moderate, flat-leaf-dense
 * sibling's children — all enqueued in one shot as soon as THEIR parent was probed — so a probe
 * wallet sized for the whole tree still drained itself entirely on the narrow siblings, and the wide
 * subtree was never reached (s3://commoncrawl: {@code projects/headers-testing/}'s ~120 flat
 * children vs. {@code contrib/datacomp/DCLM-pool/jsonl/}, a ~5M-object subtree several probes
 * deeper).
 *
 * <p>The fixture: two top-level siblings, {@code a/} (fans out immediately into {@value
 * #A_CHILDREN} single-object leaf directories — narrow span, individually) and {@code y/} (fans out
 * into exactly ONE child, {@link #B_CHILD}, a dense flat region spanning a much wider keyspace gap
 * to the top-level {@link #SENTINEL}). Byte spacing is chosen so {@code a/} scores higher than
 * {@code y/} at the top level (matching the real shape where the narrow sibling's own chain happened
 * to be reached first) and {@code y/}'s own score clearly beats every one of {@code a/}'s (uniformly
 * scored) children — so {@code a/} is probed first, its children flood the frontier, but {@code y/}
 * still gets probed ahead of them, discovering {@link #B_CHILD} before the wallet drains. {@code
 * maxProbes} is sized to exhaust well before all of {@code a/}'s children are probed (the FIFO-order
 * failure mode this regresses), but comfortably covers the handful of probes ({@code a/}, {@code
 * y/}, {@link #B_CHILD}) the maintained-priority frontier actually needs.
 */
final class SeedDescentMaintainedPriorityReachesLateDiscoveredWideSubtreeTest {

    private static final byte[] NO_PREFIX = new byte[0];
    private static final int WORKERS = 4;   // targetSeeds = maxProbes = 4*4 = 16
    private static final int A_CHILDREN = 20;
    private static final int B_CHILD_OBJECTS = 1200;   // > PROBE_PAGE (1000): B_CHILD truncates flat

    // y/'s single child is given a long, distinctive name so its span score (the byte-gap to the
    // trailing SENTINEL) is dominated by its OWN length, not by y/'s short 2-byte top-level name —
    // decoupling "y/ itself stays cheap enough to poll after a/ at the top level" from "y/'s child,
    // once discovered, unmistakably outranks every one of a/'s narrow children".
    private static final String B_CHILD = "y/" + "deep".repeat(25) + "/";
    // Starts with '~' (0x7E, the highest printable-ASCII byte) so y/'s own top-level span score
    // (the gap from "y/" to this sentinel) lands strictly between a/'s children's uniform internal
    // score and a/'s own top-level score — see the class javadoc.
    private static final String SENTINEL = "~" + "~".repeat(100) + "/";
    private static final byte[] B_CHILD_LO = B_CHILD.getBytes(StandardCharsets.UTF_8);
    private static final byte[] B_CHILD_HI = prefixCeil(B_CHILD_LO);

    /**
     * {@code a/} — {@value #A_CHILDREN} single-object leaf directories named by a single trailing
     * letter ({@code a/ca/} .. {@code a/ct/}), each an identical single-byte step from its neighbor
     * so every internal gap scores uniformly (no digit-carry artifact inflating one child's score).
     * {@link #B_CHILD} — a single dense flat region ({@value #B_CHILD_OBJECTS} direct objects, no
     * sub-directories), discovered only after {@code y/} itself is probed. {@link #SENTINEL} is a
     * trailing top-level entry so {@code y/}'s (and {@link #B_CHILD}'s) span score measures a real
     * keyspace gap instead of the degenerate open-tail case (the frontier's rightmost cut always
     * scores 0).
     */
    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < A_CHILDREN; i++) {
            char letter = (char) ('a' + i);
            keys.add(("a/c%c/o".formatted(letter)).getBytes(StandardCharsets.UTF_8));
        }
        for (int o = 0; o < B_CHILD_OBJECTS; o++) {
            keys.add((B_CHILD + "obj%05d".formatted(o)).getBytes(StandardCharsets.UTF_8));
        }
        keys.add((SENTINEL + "o").getBytes(StandardCharsets.UTF_8));
        return keys;
    }

    @Test
    void wideSubtreeEnqueuedMidDescentIsProbedBeforeEarlyNarrowFanoutDrainsTheWallet() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace()).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, WORKERS, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);

        // Still an exact I2/I3 tiling regardless of how the frontier chose to spend its probes.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        // The discriminating property: B_CHILD (discovered mid-descent, several frontier entries
        // behind a/'s flood of narrow children) got its OWN interior cut-points — it was probed and
        // pre-cut into radix bands — instead of being stranded, unprobed, as one giant serial range
        // because the probe wallet drained itself on a/'s children first.
        long interiorCuts = specs.stream()
                .map(NodeSpec::rangeStart)
                .filter(start -> start != null
                        && Arrays.compareUnsigned(start, B_CHILD_LO) > 0
                        && Arrays.compareUnsigned(start, B_CHILD_HI) < 0)
                .count();
        assertThat(interiorCuts)
                .as("y/'s dense child must be probed and pre-cut into radix bands — the maintained "
                        + "priority frontier must reach it before a/'s early narrow fanout exhausts "
                        + "maxProbes")
                .isPositive();

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED.dense_root_radix_banded", 0L))
                .as("the dense flat region under y/'s child was actually radix-banded (structure "
                        + "tiled), not left as one un-split range")
                .isPositive();
    }

    /** Increment the last byte of a prefix to form its exclusive ceiling (all prefix keys sort below it). */
    private static byte[] prefixCeil(byte[] prefix) {
        byte[] c = Arrays.copyOf(prefix, prefix.length);
        c[c.length - 1]++;
        return c;
    }
}
