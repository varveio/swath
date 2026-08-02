/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

/** Why a run ended. Only {@link #QUIESCED} completes; the other values are ceilings. */
public enum SimStopReason {

    /** The schedule emptied. */
    QUIESCED,
    /** The next event lay beyond the maximum duration and was not dispatched. */
    MAX_DURATION,
    /** The event cap was reached. */
    EVENT_CAP
}
