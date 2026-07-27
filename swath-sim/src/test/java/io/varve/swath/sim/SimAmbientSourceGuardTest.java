/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sim.kernel.SimKernel;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * No source in this module may read the host's clock or the host's randomness.
 *
 * <p>This is the rule that makes every other claim in the module true. A simulated run's wall time
 * is a modelled quantity computed from declared inputs; one call to a real clock anywhere in it, and
 * the number becomes partly a measurement of the machine that produced it, silently and without
 * failing anything. The same goes for randomness: a run is reproducible because every draw descends
 * from the run's seed, and a single ambient draw breaks that without breaking any test that is not
 * this one. Both failures are invisible in a passing suite, which is exactly why they get a
 * mechanical check rather than a convention.
 *
 * <p><b>What is checked, and how.</b> Every {@code .java} file under this module's {@code src/main}
 * is scanned for the literal call shapes below, after comments and string literals are stripped —
 * stripped first because this module's own documentation names these APIs while explaining why it
 * does not call them, and an unstripped scan would flag the very file documenting the rule. The
 * stripping mirrors the equivalent guard on the engine's decision path.
 *
 * <p><b>What is not checked</b> (state it, do not read a green run as more): reflection; a clock
 * reached through an interface this module is handed from outside; and an ambient read inside a
 * dependency this module calls. The first two are unclosable statically. The third is bounded by
 * what this module depends on — a store answers from a fixture and takes no part in the timeline —
 * but it is a real edge of the check, not a gap the scan covers.
 */
class SimAmbientSourceGuardTest {

    /** Directory-classpath marker, which is how this module's own classes resolve under its tests. */
    private static final String MAIN_CLASSES_DIR_MARKER = "/build/classes/java/main/";

    /** One forbidden shape: a human label and the pattern that finds it in stripped source. */
    private record AmbientShape(String label, Pattern pattern) {
        static AmbientShape literal(String text) {
            return new AmbientShape(text, Pattern.compile(Pattern.quote(text)));
        }

        /** Matched as a whole word, so a bare {@code ThreadLocal} cannot double-report inside
         *  {@code ThreadLocalRandom} (there is no word boundary between the two halves). */
        static AmbientShape wholeWord(String identifier) {
            return new AmbientShape(identifier, Pattern.compile("\\b" + Pattern.quote(identifier) + "\\b"));
        }
    }

    private static final List<AmbientShape> FORBIDDEN = List.of(
            AmbientShape.literal("System.nanoTime("),
            AmbientShape.literal("System.currentTimeMillis("),
            AmbientShape.literal("Instant.now("),
            AmbientShape.literal("LocalDate.now("),
            AmbientShape.literal("LocalDateTime.now("),
            AmbientShape.literal("ZonedDateTime.now("),
            AmbientShape.literal("OffsetDateTime.now("),
            AmbientShape.literal("Clock.system"),
            AmbientShape.literal("new Date("),
            AmbientShape.literal("Math.random("),
            AmbientShape.literal("ThreadLocalRandom"),
            AmbientShape.wholeWord("ThreadLocal"),
            AmbientShape.literal("new Random("),
            AmbientShape.literal("new SecureRandom("),
            AmbientShape.literal("RandomGenerator.getDefault("));

    @Test
    void noSimulatorSourceReadsAnAmbientClockOrAnAmbientRandomSource() throws Exception {
        List<Path> sources = mainSources();
        assertThat(sources).as("must resolve this module's real sources, or this test checks nothing")
                .hasSizeGreaterThan(10);
        assertThat(sources).anyMatch(p -> p.endsWith("SimKernel.java"));

        List<String> violations = new ArrayList<>();
        for (Path source : sources) {
            String stripped = stripCommentsAndLiterals(Files.readString(source));
            for (AmbientShape shape : FORBIDDEN) {
                if (shape.pattern().matcher(stripped).find()) {
                    violations.add(source.getFileName() + " uses `" + shape.label() + "`");
                }
            }
        }

        assertThat(violations)
                .as("a simulated run's timings must come from its declared inputs and its seed, never "
                        + "from the machine it happens to run on")
                .isEmpty();
    }

    /**
     * The scan must be able to fail. A source containing a forbidden shape is synthesised in memory
     * and put through the same stripping and matching, so a green run above means the shapes were
     * absent rather than that the matcher never matches anything.
     */
    @Test
    void theScanDetectsAPlantedAmbientRead() {
        String planted = """
                class Leaky {
                    // System.nanoTime() named in a comment is fine
                    long now() { return System.nanoTime(); }
                }
                """;
        String documented = """
                class Clean {
                    /** Deliberately does NOT call System.nanoTime() or ThreadLocalRandom.current(). */
                    long now() { return 0; }
                }
                """;

        assertThat(matches(planted)).containsExactly("System.nanoTime(");
        assertThat(matches(documented))
                .as("prose naming a forbidden API to explain its absence must not be flagged").isEmpty();
    }

    private static List<String> matches(String source) {
        String stripped = stripCommentsAndLiterals(source);
        List<String> found = new ArrayList<>();
        for (AmbientShape shape : FORBIDDEN) {
            if (shape.pattern().matcher(stripped).find()) {
                found.add(shape.label());
            }
        }
        return found;
    }

    /** Every {@code .java} file under this module's {@code src/main/java}, in a stable order. */
    private static List<Path> mainSources() throws IOException, URISyntaxException {
        try (Stream<Path> files = Files.walk(mainSourceRoot())) {
            return List.copyOf(files.filter(p -> p.toString().endsWith(".java"))
                    .collect(Collectors.toCollection(TreeSet::new)));
        }
    }

    /**
     * This module's source root, derived from where its own compiled classes live, so the test does
     * not depend on the process's working directory.
     */
    private static Path mainSourceRoot() throws URISyntaxException {
        URL classes = SimKernel.class.getResource("/" + SimKernel.class.getName().replace('.', '/') + ".class");
        if (classes == null || !"file".equals(classes.getProtocol())) {
            throw new IllegalStateException("this module's classes must resolve to a directory for the "
                    + "source scan to find their sources, got " + classes);
        }
        String path = Path.of(classes.toURI()).toString();
        int marker = path.indexOf(MAIN_CLASSES_DIR_MARKER);
        if (marker < 0) {
            throw new IllegalStateException("expected a main-classes output directory in " + path);
        }
        return Path.of(path.substring(0, marker), "src", "main", "java");
    }

    /**
     * Strips comments and string/char literals, mirroring the engine-side decision-path guard: this
     * codebase's javadoc routinely names a forbidden API while explaining why it is not called.
     */
    private static String stripCommentsAndLiterals(String text) {
        StringBuilder out = new StringBuilder(text.length());
        int n = text.length();
        int i = 0;
        while (i < n) {
            char c = text.charAt(i);
            if (c == '/' && i + 1 < n && text.charAt(i + 1) == '/') {
                while (i < n && text.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && text.charAt(i + 1) == '*') {
                i += 2;
                while (i + 1 < n && !(text.charAt(i) == '*' && text.charAt(i + 1) == '/')) {
                    i++;
                }
                i = Math.min(i + 2, n);
                continue;
            }
            if (c == '"' || c == '\'') {
                char quote = c;
                out.append(' ');
                i++;
                while (i < n && text.charAt(i) != quote) {
                    i += (text.charAt(i) == '\\' && i + 1 < n) ? 2 : 1;
                }
                i = Math.min(i + 1, n);
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }
}
