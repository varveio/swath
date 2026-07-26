/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * <b>The policy seam</b> (swath-notes' 2026-07-26 simulator campaign): the thief's decision logic —
 * victim selection plus the pivot cascade (algorithms.md §3, §3.1, §3.2, §3.3) — as a deterministic
 * state machine over observed events, with every clock read, thread, socket, and SQLite touch left
 * to the executor ({@code Thief}). The engine and a future discrete-event simulator share this;
 * each supplies its own execution side.
 *
 * <p><b>Source-agnostic (seam-notes.md, binding since before this interface's dispatch):</b> no
 * S3/protocol type (no SDK response class, no XML/wire type, no {@code io.varve.swath.store.ListPage})
 * crosses into this package's views, decisions, or probe outcomes — keys as raw bytes, counts,
 * streaks, and policy-domain enums only.
 *
 * <p>Two calls per steal attempt: {@link #selectVictim} picks (or refuses to pick) a victim from
 * the speculatively-read pool; {@link #beginAttempt} — called only once a victim is chosen and the
 * executor has taken the lock-guarded coherent {@code (cursor, hi)} snapshot — starts the
 * per-attempt cascade, driven to a terminal action via {@link StealAttempt}.
 *
 * <p>A policy instance may hold internal state (e.g. cascade-wide tunables), but the cascade's
 * own per-attempt state (bisect budget spent, step-back already tried, structure-probe suppression
 * already consulted this attempt) lives on the {@link StealAttempt} returned by
 * {@link #beginAttempt}, not here — this interface's implementation should be safe to reuse across
 * concurrent attempts against different victims.
 */
public interface StealPolicy {

    /**
     * Choose a victim from the curated, steal-eligible pool: {@code argmax estRemaining}
     * (algorithms.md §3.2), skipping candidates cached unsplittable, in a futility cooldown, or
     * with no remaining span. {@code pool} is read in the caller's order (selection is a linear
     * scan, not order-independent — pool order is an executor input, not a policy concern).
     */
    Selection selectVictim(List<VictimView> pool);

    /**
     * Begin one steal attempt's pivot cascade against the victim {@code view} describes, under its
     * coherent snapshot. Returns a fresh {@link StealAttempt} that carries this one attempt's
     * internal cascade state; call {@link StealAttempt#start()} to get the first {@link StealAction}.
     */
    StealAttempt beginAttempt(StealAttemptView view);
}
