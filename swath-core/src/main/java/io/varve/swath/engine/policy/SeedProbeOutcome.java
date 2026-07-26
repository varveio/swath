/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * The distilled, policy-domain result of one bounded {@code delimiter=/} probe the descent requested
 * via {@link RequestSeedProbe} — derived executor-side from whatever the store's real page response
 * was ({@code io.varve.swath.store.ListPage}), which never itself crosses into this package
 * (seam-notes.md's source-agnostic constraint: no S3/protocol type in policy views, decisions, or
 * probe outcomes).
 *
 * <p>Worked out from every classifier the descent runs (algorithms.md §8: {@code isFlatWide},
 * {@code isPartitionFanout}, {@code sampleProvesHeavy}, {@code bandHeavyCut}, the cut-weight sampler),
 * not guessed:
 * <ul>
 *   <li>{@code commonPrefixes} — every {@code CommonPrefix} the page returned, raw and unsorted (the
 *       store returns them sorted, but a mock/other store may not — the descent sorts defensively
 *       itself). Needed both as cut-point candidates and as the raw name bytes the partition-fan-out
 *       ({@code key=value/} in the final path segment) and heavy-cut-banding classifiers inspect
 *       directly — a count alone cannot answer either.</li>
 *   <li>{@code truncated} — whether this page is a strict prefix of the directory's true children.
 *       The flat-wide / tiny-leaf-explosion / heavy-cut-vs-1:1 disambiguation, and whether the descent
 *       even continues past this level, all start from this one flag.</li>
 *   <li>{@code objectCount} — {@code entries().size()}: every classifier that touches direct objects
 *       only ever tests emptiness ({@code isFlatWide}) or compares the count against a density floor
 *       ({@code sampleProvesHeavy}'s dense-child threshold, the cut-weight sampler's weight formula) —
 *       never an individual object's key.</li>
 *   <li>{@code lastKey} — the greatest sort key on the page (the max of its entries and common
 *       prefixes), or {@code null} for an empty page. The one field here that is not a classification
 *       input but a mechanical pagination cursor, carried only because computing it needs the store's
 *       real per-entry key bytes — page-decoding work the executor still owns — and is consulted only
 *       when the descent decides to page a truncated top level ({@code mass_aware_seed}'s bounded
 *       +1-page pass).</li>
 * </ul>
 *
 * @param commonPrefixes the raw common-prefix boundary bytes, in the order the store returned them
 * @param truncated      {@code true} iff the page is a strict prefix of the directory's true children
 * @param objectCount    the number of direct objects ({@code Contents}) this page returned
 * @param lastKey        the greatest sort key on the page (entries and common prefixes both
 *                       considered), or {@code null} for an empty page
 */
public record SeedProbeOutcome(List<byte[]> commonPrefixes, boolean truncated, int objectCount, byte[] lastKey) {
}
