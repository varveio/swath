/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/**
 * The single home for a run's non-{@code args_hash} <b>context</b> — the fields that
 * are checkpointed and <i>soft-restored on a bare {@code --resume}</i> but never enter
 * {@code args_hash} because they don't change <i>what is listed</i>.
 *
 * <p>{@code noSignRequest}/{@code profile}/{@code region}/{@code fetchOwner}/
 * {@code rawOutput}/{@code outputPath} are exactly this shape — like
 * {@code filterSpec}/{@code outputFormat} they stay out of the hash, but a bare
 * {@code --resume} that omits them must not silently fall back to CLI defaults (e.g.
 * losing {@code --no-sign-request} → auth failure), so they are persisted into
 * {@code run_meta} and restored on resume. {@code requestPayer}
 * ({@code --request-payer requester}) is the same shape (it changes billing, never
 * <i>what is listed</i>) and is soft-restored the same way.
 *
 * <p>{@code destinationKind} (the resolved {@code STDOUT}/{@code FILE}/{@code DIRECTORY}
 * enum name) and {@code outputType} (the raw {@code --output-type file|dir} override, or
 * {@code null}) are the same shape — they describe how output is <i>written</i>, never
 * <i>what is listed</i>, so they stay out of {@code args_hash} and are persisted/restored
 * here alongside {@code outputPath}. They are held as {@code String}s because the
 * destination-kind enum lives in the CLI module, out of reach of swath-core.
 *
 * <p>This record is the <b>one place</b> a future access-mode flag of this shape gets
 * added: bundling these fields here keeps {@link RunKey}/{@link RunMeta} from growing a
 * loose field and another back-compat constructor overload every time.
 */
public record SoftRestoreContext(
        boolean noSignRequest, String profile, String region,
        boolean fetchOwner, boolean rawOutput, String outputPath, boolean requestPayer,
        String destinationKind, String outputType) {

    /** The "nothing set" context (all flags off, no profile/region/output) — the default for a fresh run. */
    public static final SoftRestoreContext NONE =
            new SoftRestoreContext(false, null, null, false, false, null, false, null, null);
}
