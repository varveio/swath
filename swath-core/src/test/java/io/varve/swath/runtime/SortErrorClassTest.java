/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sort.ProofSpoolAllocationException;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class SortErrorClassTest {

    @Test
    void proofSpoolAllocationClassificationSurvivesTheMergeWrapper() {
        Throwable wrapped = new RuntimeException(new ProofSpoolAllocationException(
                Path.of("proof.tmp"), new IOException("ENOSPC")));

        assertThat(ListRunner.sortErrorClass(wrapped))
                .isEqualTo(ProofSpoolAllocationException.ERROR_CLASS);
    }
}
