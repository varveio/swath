/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine.Option;

/** Composite holders let related option mixins share one picocli help heading. */
final class ListOptionGroups {

    private ListOptionGroups() {
    }

    static final class RunControl {
        final SortOptions sorting;

        final LivenessOptions liveness;

        RunControl() {
            this(new SortOptions(), new LivenessOptions());
        }

        RunControl(SortOptions sorting, LivenessOptions liveness) {
            this.sorting = sorting;
            this.liveness = liveness;
        }

        @Resume(value = ResumeClass.IDENTITY, restored = true)
        @Option(names = "--sort", negatable = true,
                description = "Globally sort a Parquet directory dataset by key.")
        void sort(boolean value) {
            sorting.sort = value;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--progress-interval", paramLabel = "DURATION",
                description = "Set the progress reporting interval (default: 1s while the "
                        + "progress line redraws on a terminal, 30s for appended records; "
                        + "floor 1s).")
        void progressInterval(String value) {
            liveness.progressInterval = value;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--max-duration", paramLabel = "DURATION",
                description = "Stop after DURATION, leaving durable work resumable.")
        void maxDuration(String value) {
            liveness.maxDuration = value;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--idle-timeout", paramLabel = "DURATION",
                description = "Abort after no activity (default: 120s; 0/none/off disables).")
        void idleTimeout(String value) {
            liveness.stallTimeout = value;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--no-progress-timeout", paramLabel = "DURATION",
                description = "Abort after no committed progress (default: 10m; 0/none/off disables).")
        void noProgressTimeout(String value) {
            liveness.noProgressTimeout = value;
        }
    }

    static final class Diagnostics {
        final EngineOptions engine;

        final MetricsOptions metrics;

        final TuneOptions tune;

        final GlobalOptions global;

        Diagnostics() {
            this(new EngineOptions(), new MetricsOptions(), new TuneOptions(), new GlobalOptions());
        }

        Diagnostics(EngineOptions engine, MetricsOptions metrics, TuneOptions tune,
                GlobalOptions global) {
            this.engine = engine;
            this.metrics = metrics;
            this.tune = tune;
            this.global = global;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--color", paramLabel = "MODE",
                description = "Color the progress line and end-of-run summary: auto, "
                        + "always, or never (default: auto).")
        void color(AnsiPalette.Mode value) {
            global.color = value;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--engine-toggle", paramLabel = "NAME=VALUE", hidden = true,
                description = "Set a diagnostic engine ablation (repeatable; see docs/configuration.md).")
        void engineToggle(String[] values) {
            engine.engineToggle = new ArrayList<>(List.of(values));
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--trace", paramLabel = "PATH",
                description = "Write a diagnostic JSONL run trace to PATH.")
        void trace(String value) {
            engine.trace = value;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--metrics-endpoint", paramLabel = "URL",
                description = "Export OTLP metrics to URL (overrides SWATH_OTLP_ENDPOINT).")
        void metricsEndpoint(String value) {
            metrics.metricsEndpoint = value;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--no-metrics",
                description = "Disable metrics export, including environment configuration.")
        void noMetrics(boolean value) {
            metrics.noMetrics = value;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "--tune", paramLabel = "KEY=VALUE",
                description = "Set a typed expert option (repeatable; use --tune help; see Tuning "
                        + "in docs/configuration.md).")
        void tune(String[] values) {
            tune.entries = new ArrayList<>(List.of(values));
        }

        @Resume(ResumeClass.FREE)
        @Option(names = "-v", description = "Increase verbosity (-v INFO, -vv DEBUG, -vvv TRACE).")
        void verbose(boolean[] values) {
            global.verbosity = values;
        }

        @Resume(ResumeClass.FREE)
        @Option(names = {"-q", "--quiet"},
                description = "Decrease verbosity (-q ERROR, -qq silent); suppresses the startup "
                        + "destination echo.")
        void quiet(boolean[] values) {
            global.quiet = values;
        }

    }
}
