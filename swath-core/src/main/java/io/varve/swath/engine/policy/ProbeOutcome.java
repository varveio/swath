/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The distilled, policy-domain result of a probe {@link StealAttempt} requested via
 * {@link StealAction} — derived executor-side from whatever the store's real page response was
 * (e.g. {@code io.varve.swath.store.ListPage}), which never itself crosses into this package
 * (seam-notes.md's source-agnostic constraint: no S3/protocol types in policy views, decisions, or
 * events).
 *
 * <p>Each variant carries exactly the facts the cascade branches on for that probe kind — worked
 * out from every branch of the current cascade, not guessed:
 * <ul>
 *   <li>{@link KeyProbeOutcome} — the single-key speculative probe ({@code probeNonEmpty}): the
 *       cascade only ever tests whether a key {@code <= H} came back, never the key itself.</li>
 *   <li>{@link StructureProbeOutcome} — the {@code delimiter=/} structure probe: the cascade needs
 *       the raw common-prefix boundaries (to filter/sort/pick median-or-furthest itself) and
 *       whether the page was truncated (median vs. furthest-proved-boundary, and the fan-out
 *       suppression streak) — never the page's continuation tokens, HTTP status, or latency.</li>
 *   <li>{@link FloorProbeOutcome} — the flat-leaf bootstrap floor probe: the cascade needs only the
 *       first key at/after the leaf prefix, if any.</li>
 * </ul>
 *
 * @see KeyProbeOutcome
 * @see StructureProbeOutcome
 * @see FloorProbeOutcome
 */
public sealed interface ProbeOutcome permits KeyProbeOutcome, StructureProbeOutcome, FloorProbeOutcome {
}
