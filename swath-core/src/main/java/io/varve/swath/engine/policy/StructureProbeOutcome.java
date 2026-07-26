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
 * that count), plus whether the page was truncated (the median-vs-furthest-proved-boundary choice,
 * and the fan-out-capped engagement mark).
 *
 * @param commonPrefixes the raw common-prefix boundary bytes, in the order the store returned them
 * @param truncated      {@code true} iff the page did not reach the end of the directory
 */
public record StructureProbeOutcome(List<byte[]> commonPrefixes, boolean truncated) implements ProbeOutcome {
}
