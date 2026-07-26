/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * The result of one bounded {@code delimiter=/} structure probe (algorithms.md §3): every
 * {@code CommonPrefix} the store's page returned, raw and <b>unfiltered</b> (the cascade filters
 * to those strictly in {@code (cursor, hi)} itself, so this outcome's {@code commonPrefixes.size()}
 * is the true fan-out the zero-fan-out suppression streak needs — filtering first would corrupt
 * that count), plus whether the fan-out <b>sample</b> is capped/incomplete (the
 * median-vs-furthest-proved-boundary choice, and the {@code STRUCTURE.fanout_capped} engagement
 * mark).
 *
 * <p>Deliberately named for what the cascade does with it, not for the protocol flag it is derived
 * from ({@code ListPage#truncated()}) — {@code IsTruncated} reads as a proof-of-in-range-work
 * signal, which it is not here: {@code max_keys} is shared between {@code Contents} and
 * {@code CommonPrefixes}, so a low fan-out does not imply a complete directory, and this flag means
 * only "the sample of common prefixes this probe returned may be a strict prefix of the
 * directory's true children, so their median would be the SAMPLE's median, not the range's."
 *
 * @param commonPrefixes    the raw common-prefix boundary bytes, in the order the store returned them
 * @param fanoutSampleCapped {@code true} iff the probe did not observe the directory's full
 *                           fan-out (the sample is only a prefix of the true children)
 */
public record StructureProbeOutcome(List<byte[]> commonPrefixes, boolean fanoutSampleCapped)
        implements ProbeOutcome {
}
