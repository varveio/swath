/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Characterization: the SLF4J logger names of the observability package.
 *
 * <p>A logger name is derived from the DECLARING CLASS, so extracting a class into a new package —
 * or moving a log line onto a newly-extracted helper — silently re-homes the logger and breaks every
 * operator log filter, alert rule and log-based metric keyed on it. That re-homing is invisible to
 * an assertion about log CONTENT, so the names are pinned here as literals.
 *
 * <p>FROZEN in the same sense as the meter-series snapshot: updated deliberately alongside an
 * intended change, never re-captured to turn a red test green.
 *
 * <p><b>Known gap:</b> this pins the loggers of the classes that declare one TODAY. A brand-new
 * class introduced by an extraction, logging under a brand-new name, is not caught here — only the
 * re-homing or disappearance of an existing name is.
 */
final class ObservabilityLoggerNameCharacterizationTest {

    /** The observability classes that declare a logger, and the name that logger must carry. */
    private static final Map<Class<?>, String> EXPECTED_LOGGER_NAMES = Map.of(
            JsonRunSummaryWriter.class, "io.varve.swath.observability.JsonRunSummaryWriter",
            JsonlTraceSink.class, "io.varve.swath.observability.JsonlTraceSink",
            MeterRegistries.class, "io.varve.swath.observability.MeterRegistries",
            RunProgressReporter.class, "io.varve.swath.observability.RunProgressReporter",
            SortMergeHeartbeat.class, "io.varve.swath.observability.SortMergeHeartbeat");

    /**
     * Classes that deliberately declare NO logger of their own. {@link DaemonSchedulers} logs under
     * the CALLER's logger by design (it takes one as a parameter); {@link RunMetrics} does not log
     * at all — every signal it carries is a meter or a summary field.
     */
    private static final List<Class<?>> EXPECTED_LOGGERLESS =
            List.of(RunMetrics.class, DaemonSchedulers.class, ResourceMetrics.class, RunSummary.class);

    @Test
    void observabilityLoggerNamesAreFrozen() {
        EXPECTED_LOGGER_NAMES.forEach((type, expectedName) -> {
            List<Field> loggers = declaredLoggerFields(type);
            assertThat(loggers)
                    .describedAs("%s must declare exactly one logger field", type.getName())
                    .hasSize(1);
            // The name the class ACTUALLY logs under, read off its own field — not merely what
            // LoggerFactory would derive from the class, so a hand-renamed logger is caught too.
            assertThat(declaredLoggerName(loggers.get(0)))
                    .describedAs("logger name declared by %s", type.getName())
                    .isEqualTo(expectedName);
            assertThat(LoggerFactory.getLogger(type).getName()).isEqualTo(expectedName);
        });
    }

    @Test
    void classesThatDeliberatelyDoNotLogStillDoNot() {
        for (Class<?> type : EXPECTED_LOGGERLESS) {
            assertThat(declaredLoggerFields(type))
                    .describedAs("%s must not declare a logger", type.getName())
                    .isEmpty();
        }
    }

    /**
     * The meter names {@link RunMetrics} registers are string literals, never derived from a class
     * name — so unlike the loggers above, no meter identity can be re-homed by moving a class. This
     * pins that property directly: nothing in the registered set carries a package-qualified name.
     *
     * <p>Driven through the full {@link RunMetricsCharacterizationWorkload} — not a bare {@code new
     * RunMetrics(registry)} — so the lazily-registered api/error/steal/steal-reason/call-class/
     * Parquet/output meters are exercised too, not just the eagerly-registered gauges. The claim is
     * about the WHOLE registered set, so the whole set must be present when it is checked.
     */
    @Test
    void noMeterNameIsDerivedFromAClassName(@TempDir Path scratchDir) {
        SimpleMeterRegistry registry =
                new SimpleMeterRegistry();
        RunMetricsCharacterizationWorkload.drive(registry, scratchDir);

        assertThat(registry.getMeters()).isNotEmpty();
        assertThat(registry.getMeters().stream().map(meter -> meter.getId().getName()).toList())
                .allSatisfy(name -> assertThat(name)
                        .startsWith("swath.")
                        .doesNotContain("io.varve")
                        .doesNotContain("RunMetrics"));
    }

    private static String declaredLoggerName(Field field) {
        try {
            field.setAccessible(true);
            return ((Logger) field.get(null)).getName();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not read " + field, e);
        }
    }

    private static List<Field> declaredLoggerFields(Class<?> type) {
        List<Field> found = new ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            if (Logger.class.isAssignableFrom(field.getType())) {
                found.add(field);
            }
        }
        return found;
    }
}
