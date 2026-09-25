/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import java.time.Duration;
import java.util.function.LongConsumer;

/** A request-local page; rendering runs after the shared read permit is released. */
public interface PreparedPage {
    RenderedResponse render(BudgetedOutput output);

    default Duration injectedLatency() { return Duration.ZERO; }

    default LongConsumer injectionOverrunRecorder() { return ignored -> { }; }
}
