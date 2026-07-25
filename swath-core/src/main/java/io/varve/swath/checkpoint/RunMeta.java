/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import io.varve.swath.model.ListingMode;

/**
 * The opened run ({@code run_meta}). {@code resumed} is true when
 * {@code openRun} matched an existing incomplete run rather than creating a fresh
 * one. The stored {@code filterSpec}/{@code outputFormat} let the caller enforce
 * the §5 resume-safety check (a changed filter/format ⇒ refuse in v1.0).
 *
 * <p>{@code context} bundles the checkpointed run's non-{@code args_hash}
 * soft-restore fields (see {@link SoftRestoreContext}) — on {@code resumed=true} the
 * caller restores whichever of these it was not explicitly (re-)given, so a bare
 * {@code --resume} reconstructs the original run's auth/output context instead of
 * silently defaulting.
 *
 * <p>{@code status} is the {@code run_meta.status} row {@code openRun} matched
 * ({@code RunStatus.RUNNING} for a fresh/{@code --restart} run — a freshly inserted
 * row always starts {@code RUNNING}). {@code fatalError} is the nullable {@code
 * run_meta.fatal_error} flag (surfaces as {@code false} for NULL — a row written
 * before the column existed (backfilled NULL), or a row nothing has flagged fatal yet): {@code true} only when the
 * CLI's fatal-error guard ({@code ListCommand#runEngineGuarded}/the seed-probe catch)
 * marked this FAILED row itself, as opposed to the broken-pipe path ({@link
 * io.varve.swath.runtime.ListRunner}) marking FAILED directly WITHOUT this flag so a
 * truncated-stdout run stays normally resumable (INT-12). On {@code resumed=true} the
 * caller refuses a blind {@code --resume} only when {@code status==FAILED &&
 * fatalError} (a deterministic in-process failure would just re-fail the same way);
 * a broken-pipe {@code FAILED} (flag unset) and the normal SIGKILL/interrupt-left
 * {@code RUNNING} both resume through unchanged.
 *
 * <p>{@code identitySpec} is the readable label-tagged canonical identity string the CLI persisted
 * at run creation (as {@code run_meta.identity_spec}); on {@code resumed=true} the caller
 * recomputes it from the post-restore invocation and refuses when an IDENTITY option changed,
 * naming the changed column. {@code null} for a run created without one (a core/test path, or an
 * older checkpoint predating this column), in which case the caller applies no registry-driven identity gate.
 */
public record RunMeta(
        long id,
        boolean resumed,
        String argsHash,
        String strategy,
        ListingMode mode,
        String storedFilterSpec,
        String storedOutputFormat,
        SoftRestoreContext context,
        boolean sortEnabled,
        RunStatus status,
        boolean fatalError,
        String identitySpec) {
}
