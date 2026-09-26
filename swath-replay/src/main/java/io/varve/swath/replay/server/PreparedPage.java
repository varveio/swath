/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.metrics.ListingObservation;
import java.time.Duration;
import java.util.function.LongConsumer;

/** A request-local page; rendering runs after the shared read permit is released. */
public interface PreparedPage {
    RenderedResponse render(BudgetedOutput output);

    /** Cheap post-page first-allocation hint; zero keeps the parsed-request estimate. */
    default int initialOutputBytesHint() { return 0; }

    default ListingObservation observation() { return ListingObservation.UNKNOWN; }

    default Duration injectedLatency() { return Duration.ZERO; }

    default LongConsumer injectionOverrunRecorder() { return ignored -> { }; }
}
