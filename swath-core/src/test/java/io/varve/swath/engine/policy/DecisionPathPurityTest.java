/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * The determinism audit's enforcement test (issue #19's closing slice): every {@code decide()}
 * path — {@code io.varve.swath.engine.policy} plus the transitive closure of every type any
 * policy-package class or record holds as a field (recursing into {@code io.varve.swath.*} field
 * types only; a JDK/library type is a leaf) — must hold no ambient collaborator, per contracts.md
 * §2.1.
 *
 * <h3>Why this shape, not a grep of the policy package alone</h3>
 * The audit's ORIGINAL brief ("grep the policy package for time/RNG APIs") would have caught only
 * issue #20 (ambient {@code ThreadLocalRandom} inside {@code ThiefPolicy}) — never issue #19 ({@code
 * AlphabetDigest}'s {@code RunMetrics} field, in {@code io.varve.swath.engine}, reached only via
 * {@code StealAttemptView.alphabetDigest()}) or issue #22 ({@code OwnerSplitGovernor}'s {@code
 * ConfettiFeedbackGate} field — a policy-package field, but not matching any time/RNG API name
 * either). Both were found by human review. This test's two halves are shaped after those two
 * distinct failure modes instead:
 *
 * <ul>
 *   <li>{@link #decisionPathHoldsNoAmbientCollaboratorState()} — a FIELD-TYPE closure walk (not a
 *       text scan): catches a class anywhere in the closure HOLDING a {@code RunMetrics}/{@code
 *       TraceSink} reference, or any {@code java.util.concurrent.atomic} type, as a field —
 *       precisely the shape both #19 and #22 took. The closure recursion is what makes it
 *       PACKAGE-INDEPENDENT: {@code AlphabetDigest} is reached (and its field checked) purely
 *       because {@code StealAttemptView} carries it, exactly the mechanism #19 needed and the
 *       original grep-shaped brief did not have.</li>
 *   <li>{@link #decisionPathReadsNoAmbientClockOrRandomness()} — a comment/string-stripped SOURCE
 *       scan (reflection cannot see a method body's API calls) for the literal ambient-time/RNG
 *       call shapes, scoped to the SAME closure's source files — the mechanism issue #20 needed.
 *       Comments are stripped first because this codebase's own javadoc routinely NAMES these APIs
 *       while explaining why a class does NOT call them (e.g. {@code DecisionRng}'s own javadoc
 *       says {@code ThreadLocalRandom.current()} in prose) — an unstripped scan would false-positive
 *       on the very file documenting the fix.</li>
 * </ul>
 *
 * <p>Both tests assert the closure/source-set they scan is non-trivially populated FIRST — an
 * empty scan is an empty (vacuous) pass, not a clean one (AGENTS.md's "mutate your own, report the
 * evidence" standard: this repo's own campaign found a barrier test and a conservation test that
 * passed only because they checked nothing real).
 *
 * <h3>The executor-side exception this test must NOT flag</h3>
 * {@code Thief}'s {@code ThreadLocalRandom} default for {@link DecisionRng} and {@code
 * IdleStealBackoff}'s {@code System::nanoTime} default for {@link DecisionClock} live in {@code
 * io.varve.swath.engine} classes that are never carried as a FIELD of any policy-package type —
 * {@code Thief}/{@code IdleStealBackoff} are the executor, consulted the other direction (they
 * construct/drive a policy, a policy never references them back) — so the closure never reaches
 * them and this test never scans their source. Likewise {@code EngineToggles#recordOffMarks}
 * legitimately takes a {@code RunMetrics} PARAMETER (called only from executor code — {@code
 * Thief}/{@code SeedStep}/{@code WorkStealingScan} — never from a {@code decide()} path): this
 * test checks field TYPES, not parameter types, precisely so an explicit, caller-supplied,
 * pass-through parameter (the same shape every {@code Engagement} collector already uses) stays
 * legal while an ambient FIELD reference does not.
 *
 * <p><b>Verified against both historical leaks</b> (see this test's own commit message): temporarily
 * reintroducing #19's {@code RunMetrics} field on {@code AlphabetDigest} and #22's {@code
 * ConfettiFeedbackGate} field on {@code OwnerSplitGovernor} each independently turned {@link
 * #decisionPathHoldsNoAmbientCollaboratorState()} red; reintroducing #20's direct {@code
 * ThreadLocalRandom.current()} call in {@code ThiefPolicy} turned {@link
 * #decisionPathReadsNoAmbientClockOrRandomness()} red. All three reverted clean.
 */
final class DecisionPathPurityTest {

    private static final String POLICY_PACKAGE = "io.varve.swath.engine.policy";
    private static final String PROJECT_PACKAGE_PREFIX = "io.varve.swath";
    /** Directory-classpath marker (a local, non-jarred build). */
    private static final String MAIN_CLASSES_DIR_MARKER = "/build/classes/java/main/";
    /** Jar-classpath marker (this module's own jar on another module's/the test task's classpath). */
    private static final String MAIN_JAR_DIR_MARKER = "/build/libs/";

    private static final Set<String> FORBIDDEN_AMBIENT_COLLABORATORS = Set.of(
            "io.varve.swath.observability.RunMetrics",
            "io.varve.swath.observability.TraceSink");
    private static final String FORBIDDEN_ATOMIC_PACKAGE = "java.util.concurrent.atomic";

    /** Ambient clock/randomness API call shapes (issue #20's own shape), matched on stripped source. */
    private static final List<String> AMBIENT_TIME_RNG_CALL_SHAPES = List.of(
            "System.nanoTime(", "System.currentTimeMillis(", "Math.random(",
            "ThreadLocalRandom", "new Random(", "Instant.now(", "System.getenv(");

    @Test
    void decisionPathHoldsNoAmbientCollaboratorState() throws Exception {
        Set<Class<?>> closure = decisionPathClosure();
        assertThat(closure).as("the closure must resolve real classes, or this test checks nothing")
                .hasSizeGreaterThan(10);
        assertThat(closure).as("the closure must reach outside the policy package itself (e.g. "
                        + "AlphabetDigest, carried via StealAttemptView) -- otherwise this is only the "
                        + "original grep-shaped check the RE-SCOPED audit found insufficient")
                .anyMatch(c -> !c.getPackageName().equals(POLICY_PACKAGE));
        List<String> violations = new ArrayList<>();
        for (Class<?> type : closure) {
            for (Field f : type.getDeclaredFields()) {
                if (f.isSynthetic()) {
                    continue;   // e.g. ThiefPolicy.Attempt's implicit outer-instance reference
                }
                for (Class<?> candidate : fieldRelevantTypes(f)) {
                    String reason = forbiddenReason(candidate);
                    if (reason != null) {
                        violations.add(type.getName() + "#" + f.getName() + " : "
                                + candidate.getName() + " (" + reason + ")");
                    }
                }
            }
        }
        assertThat(violations)
                .as("every decide()-reachable type (the policy package plus every field-reachable "
                        + "io.varve.swath.* type -- e.g. AlphabetDigest via StealAttemptView) must hold "
                        + "no RunMetrics/TraceSink reference and mutate no java.util.concurrent.atomic "
                        + "state -- issues #19 (AlphabetDigest's RunMetrics field) and #22 "
                        + "(OwnerSplitGovernor's AtomicLong-backed ConfettiFeedbackGate field) were both "
                        + "exactly this shape")
                .isEmpty();
    }

    @Test
    void decisionPathReadsNoAmbientClockOrRandomness() throws Exception {
        Set<Path> sourceFiles = new TreeSet<>();
        for (Class<?> type : decisionPathClosure()) {
            Path src = sourceFileOf(type);
            if (src != null) {
                sourceFiles.add(src);
            }
        }
        assertThat(sourceFiles).as("must resolve real .java sources, or this test checks nothing")
                .hasSizeGreaterThan(10);
        List<String> violations = new ArrayList<>();
        for (Path src : sourceFiles) {
            String stripped = stripCommentsAndLiterals(Files.readString(src));
            for (String shape : AMBIENT_TIME_RNG_CALL_SHAPES) {
                if (stripped.contains(shape)) {
                    violations.add(src + " calls `" + shape + "` directly");
                }
            }
        }
        assertThat(violations)
                .as("no decide()-reachable source may read an ambient clock or randomness directly -- "
                        + "issue #20 was exactly this shape (ThiefPolicy's structure-probe suppression "
                        + "recovery reaching for ThreadLocalRandom.current() instead of the injected "
                        + "DecisionRng); DecisionRng/DecisionClock's injected-default suppliers "
                        + "(Thief/IdleStealBackoff) are executor-side and never enter this closure")
                .isEmpty();
    }

    // -------------------------------------------------------------------------
    // Closure discovery: the policy package, plus every field-reachable io.varve.swath.* type.
    // -------------------------------------------------------------------------

    private static Set<Class<?>> decisionPathClosure()
            throws IOException, URISyntaxException, ClassNotFoundException {
        Set<Class<?>> visited = new HashSet<>();
        Deque<Class<?>> frontier = new ArrayDeque<>(classesInPackage(POLICY_PACKAGE));
        while (!frontier.isEmpty()) {
            Class<?> c = frontier.poll();
            if (c == null || !visited.add(c)) {
                continue;
            }
            for (Field f : c.getDeclaredFields()) {
                if (f.isSynthetic()) {
                    continue;
                }
                for (Class<?> candidate : fieldRelevantTypes(f)) {
                    Class<?> leaf = candidate.isArray() ? candidate.getComponentType() : candidate;
                    if (leaf.getPackageName().startsWith(PROJECT_PACKAGE_PREFIX) && !visited.contains(leaf)) {
                        frontier.add(leaf);
                    }
                }
            }
        }
        return visited;
    }

    /** The field's own declared type, plus (for a parameterized type) each {@code Class} type argument. */
    private static List<Class<?>> fieldRelevantTypes(Field f) {
        List<Class<?>> out = new ArrayList<>();
        out.add(f.getType());
        if (f.getGenericType() instanceof ParameterizedType pt) {
            for (Type arg : pt.getActualTypeArguments()) {
                if (arg instanceof Class<?> cls) {
                    out.add(cls);
                }
            }
        }
        return out;
    }

    private static String forbiddenReason(Class<?> type) {
        Class<?> leaf = type.isArray() ? type.getComponentType() : type;
        if (FORBIDDEN_AMBIENT_COLLABORATORS.contains(leaf.getName())) {
            return "ambient collaborator";
        }
        if (FORBIDDEN_ATOMIC_PACKAGE.equals(leaf.getPackageName())) {
            return "java.util.concurrent.atomic mutation";
        }
        return null;
    }

    /**
     * Every top-level/nested class file directly in {@code pkg}'s MAIN (never test) output —
     * handling both a raw {@code build/classes/java/main} directory (a solo module build) and this
     * module's own packaged jar (how a Gradle multi-module test classpath actually resolves an
     * intra-repo dependency here — verified by running this exact scan under {@code :swath-core:test}
     * and confirming a raw directory URL never appears for this package on that classpath).
     */
    private static List<Class<?>> classesInPackage(String pkg)
            throws IOException, URISyntaxException, ClassNotFoundException {
        String path = pkg.replace('.', '/');
        ClassLoader cl = DecisionPathPurityTest.class.getClassLoader();
        List<Class<?>> out = new ArrayList<>();
        Enumeration<URL> resources = cl.getResources(path);
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            if ("file".equals(url.getProtocol())) {
                if (!url.getPath().contains(MAIN_CLASSES_DIR_MARKER)) {
                    continue;   // the test/testFixtures classes output for this same package name
                }
                Path dir = Path.of(url.toURI());
                try (Stream<Path> files = Files.list(dir)) {
                    for (Path p : files.filter(f -> f.toString().endsWith(".class")).toList()) {
                        String simple = p.getFileName().toString();
                        String className = pkg + "." + simple.substring(0, simple.length() - ".class".length());
                        out.add(Class.forName(className, false, cl));
                    }
                }
            } else if ("jar".equals(url.getProtocol())) {
                Path jarFile = jarFilePathOf(url);
                if (jarFile == null || !jarFile.toString().contains(MAIN_JAR_DIR_MARKER)) {
                    continue;   // a dependency jar that merely happens to share this package name
                }
                String prefix = path + "/";
                try (JarFile jar = new JarFile(jarFile.toFile())) {
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        String name = entries.nextElement().getName();
                        if (!name.startsWith(prefix) || !name.endsWith(".class")) {
                            continue;
                        }
                        String rest = name.substring(prefix.length());
                        if (rest.contains("/")) {
                            continue;   // a sub-package -- none expected here, skip defensively
                        }
                        String className = pkg + "." + rest.substring(0, rest.length() - ".class".length());
                        out.add(Class.forName(className, false, cl));
                    }
                }
            }
        }
        return out;
    }

    /** The {@code .java} source file backing {@code type}'s TOP-LEVEL enclosing class, or {@code null}
     *  if it does not resolve to one under this build's {@code src/main/java} (a JDK/library type). */
    private static Path sourceFileOf(Class<?> type) throws URISyntaxException {
        String name = type.getName();
        int dollar = name.indexOf('$');
        String topLevelName = (dollar >= 0) ? name.substring(0, dollar) : name;
        String resourceName = topLevelName.replace('.', '/') + ".class";
        URL url = type.getClassLoader().getResource(resourceName);
        if (url == null) {
            return null;
        }
        String relativeJavaPath = resourceName.substring(0, resourceName.length() - ".class".length()) + ".java";
        if ("file".equals(url.getProtocol())) {
            String classPath = Path.of(url.toURI()).toString();
            int idx = classPath.indexOf(MAIN_CLASSES_DIR_MARKER);
            if (idx < 0) {
                return null;
            }
            Path src = Path.of(classPath.substring(0, idx), "src", "main", "java", relativeJavaPath);
            return Files.isRegularFile(src) ? src : null;
        }
        if ("jar".equals(url.getProtocol())) {
            Path jarFile = jarFilePathOf(url);
            if (jarFile == null) {
                return null;
            }
            String jarPath = jarFile.toString();
            int idx = jarPath.indexOf(MAIN_JAR_DIR_MARKER);
            if (idx < 0) {
                return null;
            }
            Path src = Path.of(jarPath.substring(0, idx), "src", "main", "java", relativeJavaPath);
            return Files.isRegularFile(src) ? src : null;
        }
        return null;
    }

    /** The underlying jar FILE path of a {@code jar:file:/path/to/x.jar!/entry} resource URL. */
    private static Path jarFilePathOf(URL jarEntryUrl) {
        String s = jarEntryUrl.toString();
        int bang = s.indexOf("!/");
        if (!s.startsWith("jar:") || bang < 0) {
            return null;
        }
        return Path.of(URI.create(s.substring("jar:".length(), bang)));
    }

    // -------------------------------------------------------------------------
    // Comment/string-literal stripping (mirrors scripts/ci/check-instrumentation-drift.py's
    // `scrub()`), so a javadoc/comment naming a forbidden API to explain why it is NOT called never
    // false-positives this scan.
    // -------------------------------------------------------------------------

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
