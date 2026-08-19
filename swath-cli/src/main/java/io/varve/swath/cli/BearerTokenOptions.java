/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import picocli.CommandLine.Option;

/**
 * The bearer-token auth flags, declared once and attached to both {@code swath list} (nested in
 * {@link ConnectionOptions}, so they render under its {@code Connection:} heading) and {@code swath
 * resume} (attached directly).
 *
 * <p>They are the one piece of connection config a resume cannot restore from its checkpoint. Every
 * other auth flag ({@code --profile}, {@code --region}, {@code --no-sign-request}) is {@link
 * ResumeClass#STICKY} and soft-restored; these are {@link ResumeClass#FREE} and never persisted,
 * for two reasons that both cut the same way:
 *
 * <ul>
 *   <li><b>A stored command is a stored secret.</b> Nothing obliges the command to <i>mint</i> a
 *       token — {@code --bearer-token-command 'echo eyJhbGci…'} is a plausible shortcut for someone
 *       who already has one in hand. A STICKY row would put that literal token in {@code run_meta},
 *       at rest on disk, having been typed as a command rather than a credential. The token itself
 *       otherwise never leaves memory ({@code ProcessBearerTokenSupplier}'s cache), and this is the
 *       one path that would change that.</li>
 *   <li><b>A stored command is executed later.</b> Restoring it means a checkpoint file decides what
 *       a subsequent {@code swath resume} runs, so whoever can write that file chooses the command.
 *       A checkpoint is data, not a trusted script.</li>
 * </ul>
 *
 * <p>That leaves re-passing them on {@code swath resume} as the only way a resumed run against a
 * bearer-auth endpoint (e.g. GCS's XML API) can authenticate at all.
 *
 * <p>Shared rather than declared per-command because the two copies must not drift: the
 * classification above is a security property, and {@code list} and {@code resume} disagreeing
 * about it — or about the help text that tells operators to re-pass the flag — is exactly the bug
 * this shape prevents. Contrast {@code --stats}/{@code --progress}, which are duplicated between
 * {@link OutputOptions} and {@link ResumeCommand}.
 */
final class BearerTokenOptions {

    // One description for both commands, so it must hold on both: the GCS worked example that used
    // to live here named --endpoint-url/--force-path-style, which `swath resume` does not accept
    // (they are restored from the checkpoint). It lives in docs/operating.md instead.
    @Resume(ResumeClass.FREE)
    @Option(names = "--bearer-token-command", paramLabel = "CMD",
            description = "Shell command whose stdout is a fresh OAuth bearer token (e.g. 'gcloud "
                    + "auth print-access-token' for GCS's XML API), used instead of AWS SigV4 "
                    + "signing for every request. Never stored in the checkpoint, so a resumed run "
                    + "against a bearer-auth endpoint must re-pass it.")
    String command;

    @Resume(ResumeClass.FREE)
    @Option(names = "--bearer-token-refresh-interval", paramLabel = "DURATION",
            description = "How often to re-run --bearer-token-command for a fresh token (default: 45m). "
                    + "Size it comfortably under the token source's real expiry.")
    String refreshInterval;

    /**
     * Copy what the operator passed to {@code swath resume} onto the delegated {@link ListCommand}'s
     * own instance. A method rather than field-by-field assignment at the call site so a third flag
     * added above is forwarded without touching {@link ResumeCommand}.
     */
    void copyFrom(BearerTokenOptions other) {
        this.command = other.command;
        this.refreshInterval = other.refreshInterval;
    }
}
