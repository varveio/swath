/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.InvalidConfigException;
import java.time.Duration;

/** OTLP metrics export controls: collector endpoint and an explicit environment override. */
final class MetricsOptions {

    String metricsEndpoint;

    boolean noMetrics;

    String metricsInterval;

    String resolvedExportMode() {
        if (noMetrics) {
            return "none";
        }
        return metricsEndpoint == null ? null : "otlp";
    }

    /** Resolve the internal metrics interval; null defers to SWATH_OTLP_INTERVAL/default. */
    Duration resolveMetricsInterval() throws InvalidConfigException {
        return metricsInterval == null ? null : DurationParser.metricsInterval(metricsInterval);
    }
}
