/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * One steal attempt's pivot cascade in progress (algorithms.md §3), holding whatever per-attempt
 * state the cascade needs between probes (bisect budget spent so far, whether the far-ahead
 * step-back already fired, whether structure discovery already ran this attempt). A fresh instance
 * is created per attempt by {@link StealPolicy#beginAttempt}; never reused across attempts.
 *
 * <p>The executor drives it to a terminal {@link StealAction} ({@link Commit}, {@link Retry}, or
 * {@link MarkUnsplittable}): call {@link #start()}, and for every {@link RequestKeyProbe}/
 * {@link RequestStructureProbe}/{@link RequestFloorProbe} it returns, issue the described RPC and
 * feed the distilled {@link ProbeOutcome} back via {@link #onProbeResult}, repeating until a
 * terminal action comes back.
 */
public interface StealAttempt {

    /** The first decision for this attempt: place the initial pivot, or refuse outright. */
    StealAction start();

    /** Continue the cascade with the result of the most recently requested probe. */
    StealAction onProbeResult(ProbeOutcome outcome);
}
