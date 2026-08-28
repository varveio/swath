/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

import io.varve.swath.output.DiskFull;

/**
 * Sink write / Parquet failure. The wrapped cause chain classifies a full disk as exit 74;
 * otherwise this exits 1. {@link PublicationPendingException} identifies the narrower terminal
 * dataset-publication failure whose already-durable parts permit a publication-only retry.
 */
public sealed class OutputException extends SwathException
        permits MergePendingException, PublicationPendingException {

    /**
     * {@code EX_IOERR} (sysexits.h) for the out-of-space case. It is repeated literally because
     * core must not depend on the CLI module; the CLI names the same published value as
     * {@code io.varve.swath.cli.ExitCodes#DISK_FULL}.
     */
    private static final int DISK_FULL = 74;

    public OutputException(String message) {
        super(message);
    }

    public OutputException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * {@link DiskFull#isIn} searches the whole cause chain rather than this exception's immediate
     * cause: the {@code IOException} the OS raised is thrown inside the Parquet writer and arrives
     * here wrapped, sometimes more than once.
     */
    @Override
    public int exitCode() {
        return DiskFull.isIn(this) ? DISK_FULL : 1;
    }
}
