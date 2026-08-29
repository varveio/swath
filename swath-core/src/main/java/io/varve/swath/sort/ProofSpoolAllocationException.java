/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;

/** Checked, stably classified failure to allocate or map the parallel proof spool. */
public final class ProofSpoolAllocationException extends IOException {

    private static final long serialVersionUID = 1L;

    public static final String ERROR_CLASS = "proof_spool_allocation_failed";

    public ProofSpoolAllocationException(Path path, Throwable cause) {
        super("sort proof spool " + path + ": error_class=" + ERROR_CLASS
                + ": allocation or mapping failed", cause);
    }

    /** The greppable fingerprint written to the terminal run summary. */
    public String errorClass() {
        return ERROR_CLASS;
    }
}
