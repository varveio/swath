/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

import io.varve.swath.output.DiskFull;

/** Sink write / Parquet failure. Exit 1 — or 74 when the cause is a full disk. */
public final class OutputException extends SwathException {

    /**
     * {@code EX_IOERR} (sysexits.h) for the out-of-space case specifically, kept apart from the
     * generic {@code 1} because an external runner can act on this and on nothing else here: a
     * full workspace is fixed by giving the next attempt more space, which is the wrong response
     * to every other sink failure. Named at the CLI boundary as
     * {@code io.varve.swath.cli.ExitCodes#DISK_FULL} so the published table keeps one source —
     * the same split {@code InvalidArgsException} and {@code ExitCodes#USAGE} already use.
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
