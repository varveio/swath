/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Option;

/**
 * The registry/annotation drift guard, walking the full {@code App.commandLine()} command tree. It is
 * what a hand-written registry cannot give: classification lives on the same element as
 * {@code @Option} (so it can never name a wrong option), and the guard catches a missing
 * classification or a missing/creeping registry row as a red build.
 *
 * <p>Only the assertions checkable at this stage are active. Further structural assertions are
 * deferred to the units that build their subject:
 * <ul>
 *   <li>union of persisted {@code column} values == the {@code run_meta} columns the store
 *       writes, once {@code identity_spec} is persisted.</li>
 *   <li>{@code SoftRestoreContext}'s components == STICKY ∪ IDENTITY(restored=true).</li>
 *   <li>docs table == registry, in the {@code TuneOptionsTest} shape.</li>
 * </ul>
 */
class ResumeRegistryDriftTest {

    @Test
    void everyVisibleOptionCarriesResume() {
        for (CommandSpec spec : allSpecs()) {
            for (OptionSpec option : spec.options()) {
                if (!isSwathOption(option)) {
                    continue;
                }
                AnnotatedElement element = (AnnotatedElement) option.userObject();
                assertThat(element.getAnnotation(Resume.class))
                        .as("option %s declared by %s must carry @Resume",
                                option.longestName(), declaringClass(option.userObject()).getName())
                        .isNotNull();
            }
        }
    }

    @Test
    void reflectiveDiscoveryEqualsCommandSpecOptions() {
        for (CommandSpec spec : allSpecs()) {
            List<String> declared = new ArrayList<>();
            List<String> discovered = new ArrayList<>();
            for (OptionSpec option : spec.options()) {
                if (!isSwathOption(option)) {
                    continue;
                }
                declared.add(option.longestName());
                if (option.userObject() instanceof AnnotatedElement element
                        && element.isAnnotationPresent(Option.class)) {
                    discovered.add(option.longestName());
                }
            }
            assertThat(discovered).as("reflective userObject discovery for %s", spec.name())
                    .containsExactlyInAnyOrderElementsOf(declared);
        }
    }

    @Test
    void everyPersistedClassOptionHasRegistryRow() {
        for (OptionSpec option : allSwathOptions()) {
            ResumeClass resumeClass = resumeOf(option).value();
            if (resumeClass == ResumeClass.IDENTITY || resumeClass == ResumeClass.STICKY) {
                assertThat(ResumeRegistry.hasPersistedRow(option.longestName()))
                        .as("%s option %s needs a ResumeRegistry row", resumeClass, option.longestName())
                        .isTrue();
            }
        }
    }

    @Test
    void everyFreeOptionHasNoRegistryRow() {
        for (OptionSpec option : allSwathOptions()) {
            if (resumeOf(option).value() == ResumeClass.FREE) {
                assertThat(ResumeRegistry.hasPersistedRow(option.longestName()))
                        .as("FREE option %s must not have a ResumeRegistry row", option.longestName())
                        .isFalse();
            }
        }
    }

    @Test
    void resumeHasZeroIdentityOptions() {
        CommandSpec resume = App.commandLine().getSubcommands().get("resume").getCommandSpec();
        for (OptionSpec option : resume.options()) {
            if (!isSwathOption(option)) {
                continue;
            }
            assertThat(resumeOf(option).value())
                    .as("resume must expose no IDENTITY option, but %s is one", option.longestName())
                    .isNotEqualTo(ResumeClass.IDENTITY);
        }
    }

    private static List<OptionSpec> allSwathOptions() {
        List<OptionSpec> swathOptions = new ArrayList<>();
        for (CommandSpec spec : allSpecs()) {
            for (OptionSpec option : spec.options()) {
                if (isSwathOption(option)) {
                    swathOptions.add(option);
                }
            }
        }
        return swathOptions;
    }

    private static Resume resumeOf(OptionSpec option) {
        return ((AnnotatedElement) option.userObject()).getAnnotation(Resume.class);
    }

    private static List<CommandSpec> allSpecs() {
        List<CommandSpec> specs = new ArrayList<>();
        collect(App.commandLine().getCommandSpec(), specs);
        return specs;
    }

    private static void collect(CommandSpec spec, List<CommandSpec> out) {
        out.add(spec);
        for (CommandLine sub : spec.subcommands().values()) {
            collect(sub.getCommandSpec(), out);
        }
    }

    private static boolean isSwathOption(OptionSpec option) {
        Class<?> declaring = declaringClass(option.userObject());
        return declaring != null && declaring.getPackageName().startsWith("io.varve.swath");
    }

    private static Class<?> declaringClass(Object userObject) {
        if (userObject instanceof Field field) {
            return field.getDeclaringClass();
        }
        if (userObject instanceof Method method) {
            return method.getDeclaringClass();
        }
        return null;
    }
}
