/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import java.io.IOException;

/**
 * Deterministic crash-injection seam for the replacement-publication tail.
 *
 * <p>{@code ordinal} is the zero-based part ordinal for {@link
 * PublicationStep#AFTER_PART_RENAME}; it is {@code -1} at every other step.
 */
@FunctionalInterface
public interface PublicationStepHook {

    PublicationStepHook NO_OP = (step, ordinal) -> { };

    void reached(PublicationStep step, int ordinal) throws IOException;
}
