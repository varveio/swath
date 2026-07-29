/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.varve.swath.engine.EngineToggles;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Characterization guard for the {@code engine_toggles_effective} startup line's <em>logger
 * identity</em>. Production {@code logback.xml} renders the category with {@code %logger{20}}, so the
 * token printed for this line is load-bearing: it must stay {@code io.varve.swath.cli.ListCommand}
 * (the historical emitter) even after the resolve logic moved into {@link EngineOptions}. This pins
 * both the logger name AND the raw message template + argument array so a future relocation that
 * emits under a different category (e.g. {@code EngineOptions}) fails here instead of silently
 * regressing the operator-visible token.
 */
class EngineTogglesEffectiveLogIdentityTest {

    private static final String EXPECTED_TEMPLATE =
            "engine_toggles_effective owner_split={} density_ewma={} radix_bands={} "
                    + "structure_probes={} far_ahead={} alphabet_pivots={} reflect={} confetti_feedback={} "
                    + "reflect_lift={} fanout_tiling={} mass_aware_seed={} readahead={} "
                    + "rate_anchored_sensing={} tail_floor={} "
                    + "(non-default engine configuration — the rate_anchored_sensing/tail_floor pair is a "
                    + "documented, supported rollback; every other toggle is an EXPERIMENTAL/DIAGNOSTIC "
                    + "ablation surface, measurement only)";

    @Test
    void engineTogglesEffectiveEmitsUnderTheListCommandLoggerWithUnchangedMessageAndArgs() throws Exception {
        ListCommand cmd = new ListCommand();
        // owner_split=off is the canonical non-default toggle; everything else stays at its default
        // (readahead off; mass_aware_seed and rate_anchored_sensing on; tail_floor reach_floored)
        // so this exercises the non-silent branch.
        cmd.engine.toggles = EngineToggles.parse(List.of("owner_split=off"), false);
        assertThat(cmd.engine.toggles.isDefault()).isFalse();

        ILoggingEvent event = captureEngineTogglesLine(cmd);

        assertThat(event.getLoggerName()).isEqualTo("io.varve.swath.cli.ListCommand");
        assertThat(event.getLevel()).isEqualTo(Level.INFO);
        assertThat(event.getMessage()).isEqualTo(EXPECTED_TEMPLATE);
        assertThat(event.getArgumentArray()).containsExactly(
                false, true, true, true, true, true, true, true, true, true, true, false, true, "reach_floored");
    }

    /**
     * The line's own promise: {@link EngineToggles#isDefault()} fires it on ANY of the fourteen
     * components deviating, so a run that deviates in ONE of them must print that one's real state.
     * A toggle omitted from the format string would fire the line and then print every field at its
     * default, which is the failure this pins — read off the FORMATTED message, since a missing
     * field is exactly what a template-only assertion cannot see.
     */
    @Test
    void aRunDeviatingOnlyInRateAnchoredSensingPrintsThatToggleAsDisabled() throws Exception {
        ListCommand cmd = new ListCommand();
        // Since the 0.2.0 default flip the sensor is ON by default, so the deviating run is the
        // OFF one — i.e. the documented rollback arm. The property under test is unchanged: a run
        // deviating in exactly one component must print that component's real state.
        cmd.engine.toggles = EngineToggles.parse(List.of("rate_anchored_sensing=off"), false);
        assertThat(cmd.engine.toggles.isDefault()).isFalse();

        // The line renders each toggle's effective boolean, so an opted-OUT mechanism reads
        // `=false`; a toggle missing from the format string prints nothing at all, which is what
        // this catches.
        assertThat(captureEngineTogglesLine(cmd).getFormattedMessage())
                .contains("rate_anchored_sensing=false");
    }

    /** The same promise for the one value-taking toggle: it prints its selected MODE, not a boolean. */
    @Test
    void aRunDeviatingOnlyInTailFloorPrintsTheSelectedMode() throws Exception {
        ListCommand cmd = new ListCommand();
        // `current` is the deviating mode since 0.2.0 (it is the rollback arm); `reach_floored` is
        // now the default and would not fire the line at all.
        cmd.engine.toggles = EngineToggles.parse(List.of("tail_floor=current"), false);
        assertThat(cmd.engine.toggles.isDefault()).isFalse();

        assertThat(captureEngineTogglesLine(cmd).getFormattedMessage())
                .contains("tail_floor=current");
    }

    /** Run {@code cmd}'s startup echo and return the {@code engine_toggles_effective} event it emitted. */
    private static ILoggingEvent captureEngineTogglesLine(ListCommand cmd) {
        Logger logger =
                (Logger) LoggerFactory.getLogger(ListCommand.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);
        try {
            cmd.logEngineTogglesIfNonDefault();
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        return appender.list.stream()
                .filter(e -> e.getMessage().startsWith("engine_toggles_effective"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no engine_toggles_effective event captured on the io.varve.swath.cli.ListCommand logger"));
    }
}
