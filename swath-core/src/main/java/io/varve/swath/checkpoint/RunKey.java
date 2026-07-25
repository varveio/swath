/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import io.varve.swath.model.ListingMode;

/**
 * The identity + resume-eligibility key for a run ({@code run_meta}, §5).
 *
 * <p>{@code argsHash} is the SHA-256 over exactly the listing-relevant fields
 * ({@code ArgsHashFields}); it gates whether a {@code --resume} is allowed.
 * {@code filterSpec}/{@code outputFormat} are stored but <b>not</b> part of
 * {@code argsHash} (§5): a v1.0 resume with a changed filter/format is refused at
 * the CLI (the already-written output reflects the old filter), but they don't
 * change <i>what is listed</i>, so they stay out of the hash.
 *
 * <p>{@code context} bundles the run's non-{@code args_hash}
 * soft-restored-on-resume fields (see {@link SoftRestoreContext}) — like
 * {@code filterSpec}/{@code outputFormat} they stay out of the hash, but a bare
 * {@code --resume} that omits them must not silently fall back to CLI defaults.
 * {@link SqliteCheckpointStore} persists them into {@code run_meta} so the caller can
 * restore them on resume.
 */
public record RunKey(
        String scheme,
        String endpoint,
        String bucket,
        byte[] prefix,
        String argsHash,
        String strategy,
        ListingMode mode,
        String filterSpec,
        String outputFormat,
        SoftRestoreContext context,
        boolean sortEnabled,
        String identitySpec) {

    /**
     * Identity-only convenience constructor: no soft-restore context
     * ({@link SoftRestoreContext#NONE}) and {@code sortEnabled} off. Fine for tests that
     * don't exercise the soft-restore path or {@code --sort}.
     */
    public RunKey(String scheme, String endpoint, String bucket, byte[] prefix, String argsHash,
                  String strategy, ListingMode mode, String filterSpec, String outputFormat) {
        this(scheme, endpoint, bucket, prefix, argsHash, strategy, mode, filterSpec, outputFormat,
                SoftRestoreContext.NONE, false, null);
    }

    /**
     * Full soft-restore constructor without an {@code identitySpec} — the readable label-tagged
     * identity string ({@code run_meta.identity_spec}) is computed by the CLI's
     * {@code ResumeRegistry} and is absent on the core/test paths that do not classify options.
     * Passing {@code null} stores no identity_spec, so those runs simply carry no registry-driven
     * identity gate.
     */
    public RunKey(String scheme, String endpoint, String bucket, byte[] prefix, String argsHash,
                  String strategy, ListingMode mode, String filterSpec, String outputFormat,
                  SoftRestoreContext context, boolean sortEnabled) {
        this(scheme, endpoint, bucket, prefix, argsHash, strategy, mode, filterSpec, outputFormat,
                context, sortEnabled, null);
    }
}
