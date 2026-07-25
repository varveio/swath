/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

/**
 * How an option (or a {@code --tune} key) relates to checkpoint resume: {@link #IDENTITY} changes
 * <i>what is listed</i> or <i>where it is
 * written</i> and is refused when it differs from the checkpoint; {@link #STICKY} is soft-restored
 * from the checkpoint unless re-passed (profile/auth/region); {@link #FREE} may change freely on a
 * resume. The one typed answer shared by visible {@code @Option}s ({@link Resume}) and {@code
 * --tune} keys ({@link TuneOptions.KeySpec}).
 */
enum ResumeClass {
    IDENTITY,
    STICKY,
    FREE
}
