/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

/** Why a run ended. Only {@link #QUIESCED} is a finished run; the other two are ceilings hit. */
public enum SimStopReason {

    /** The schedule emptied — every actor ran out of future events, which is what "done" means. */
    QUIESCED,
    /** The next event lay beyond the scenario's declared max duration, so it was never dispatched. */
    MAX_DURATION,
    /** The event cap was reached — a runaway-scenario guard, never an expected outcome. */
    EVENT_CAP
}
