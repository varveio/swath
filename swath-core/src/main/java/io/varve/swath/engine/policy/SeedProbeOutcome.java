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
 * (contracts.md §2.1's source-agnostic constraint: no S3/protocol type in policy views, decisions, or
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
 *   <li>{@code pageCapped} — the underlying store fact (the current code's {@code ListPage#truncated}):
 *       this page hit the store's own page cap, so it is a strict prefix of the directory's true
 *       children, not the whole listing. Deliberately named for the fact, not for either interpretation
 *       the descent draws from it — this one bit does double duty as two distinct policy signals at
 *       different call sites (mirroring, and generalizing, why {@code StructureProbeOutcome} renamed
 *       the same underlying protocol flag to {@code fanoutSampleCapped} for the thief's single-meaning
 *       use of it):
 *       <ul>
 *         <li><b>fan-out-sample-capped</b> — at the top-level probe and every plain descent-frontier
 *             probe (the current code's {@code collectCutPoints}, e.g. around the initial top probe,
 *             the optional top-pagination probe, and the frontier poll loop): a capped page means the
 *             directory's fan-out is at least a page wide and the classifiers cannot see all of it yet
 *             — this is what {@code isFlatWide}/{@code isPartitionFanout} branch on, and what decides
 *             whether the descent even continues past this level.</li>
 *         <li><b>heavy-mass signal</b> — at a disambiguation SAMPLE probe (the current code's
 *             {@code sampleProvesHeavy}, which treats a sampled child's own capped page as evidence the
 *             child itself holds at least a page of mass — i.e. "dense", not "1:1") and at a
 *             cut-weight-sampling probe (the current code's cut-weight sampler, which assigns a capped
 *             cut the store's page-size weight as its estimated mass rather than its literal observed
 *             count): here the SAME bit is read as a mass estimate, not a fan-out-visibility flag.</li>
 *       </ul>
 *       Genuinely one bit, not two: both readings are downstream inferences the descent draws from the
 *       identical underlying fact ("this page hit the store's cap"), so splitting it into two fields
 *       would duplicate the same information under two names.</li>
 *   <li>{@code objectCount} — {@code entries().size()}: every classifier that touches direct objects
 *       only ever tests emptiness ({@code isFlatWide}) or compares the count against a density floor
 *       ({@code sampleProvesHeavy}'s dense-child threshold, the cut-weight sampler's weight formula) —
 *       never an individual object's key.</li>
 *   <li>{@code lastKey} — the greatest sort key on the page (the max of its entries and common
 *       prefixes), or {@code null} for an empty page. The one field here that is not a classification
 *       input but a mechanical pagination cursor, carried only because computing it needs the store's
 *       real per-entry key bytes — page-decoding work the executor still owns — and is consulted only
 *       when the descent decides to page a capped top level ({@code mass_aware_seed}'s bounded +1-page
 *       pass).</li>
 * </ul>
 *
 * @param commonPrefixes the raw common-prefix boundary bytes, in the order the store returned them
 * @param pageCapped     {@code true} iff this page hit the store's own page cap — see the type-level
 *                       javadoc for the two distinct policy meanings this one fact carries depending
 *                       on which probe it answers
 * @param objectCount    the number of direct objects ({@code Contents}) this page returned
 * @param lastKey        the greatest sort key on the page (entries and common prefixes both
 *                       considered), or {@code null} for an empty page
 */
public record SeedProbeOutcome(List<byte[]> commonPrefixes, boolean pageCapped, int objectCount, byte[] lastKey) {
}
