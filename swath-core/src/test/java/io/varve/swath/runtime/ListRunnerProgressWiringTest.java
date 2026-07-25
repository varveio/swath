/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.observability.RunProgressReporter;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;

/**
 * {@link ListRunner#startProgress} is the engine's end of the ONE progress lifecycle: it starts the
 * reporter when nothing else has (a core-only caller, or the merge-only resume path, where the
 * merge/publish tail would otherwise report nothing on any external channel), and JOINS the
 * session reporter the CLI already started around the whole run — including the seed step — rather
 * than running a second ticker with its own clock and its own windowed-rate baseline.
 *
 * <p>Pins that resolution directly; the reporter's own semantics are covered by {@code
 * LiveProgressContractTest}.
 */
final class ListRunnerProgressWiringTest {

    @Test
    void unsetIntervalStillStartsAnOwningReporter() throws Exception {
        RunContext ctx = RunContext.create();

        try (RunProgressReporter reporter = ListRunner.startProgress(ctx, null)) {
            assertThat(executorOf(reporter))
                    .as("no session reporter is running, so this one owns the ticker")
                    .isNotNull();
        }
    }

    @Test
    void engineStartJoinsTheSessionReporterTheCliAlreadyOwns() throws Exception {
        RunContext ctx = RunContext.create();

        try (RunProgressReporter session =
                     RunProgressReporter.start(ctx.metrics(), Duration.ofMillis(20))) {
            try (RunProgressReporter engine = ListRunner.startProgress(ctx, Duration.ofMillis(20))) {
                assertThat(executorOf(engine))
                        .as("a nested engine-phase start must not schedule a second ticker")
                        .isNull();
            }
            assertThat(executorOf(session))
                    .as("closing the engine's joined handle must not stop the session ticker")
                    .isNotNull();
        }
    }

    private static ScheduledExecutorService executorOf(RunProgressReporter reporter) throws Exception {
        Field f = RunProgressReporter.class.getDeclaredField("executor");
        f.setAccessible(true);
        return (ScheduledExecutorService) f.get(reporter);
    }
}
