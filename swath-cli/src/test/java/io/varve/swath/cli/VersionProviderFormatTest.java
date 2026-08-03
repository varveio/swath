/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * {@code --version} rendering. The commit value comes from a manifest attribute that
 * only exists in a packaged jar, so the formatting is tested directly rather than through a
 * built artifact — {@link AppCliTest} already covers the end-to-end invocation.
 *
 * <p>The absent/blank/"unknown" cases are the ones that matter: they are what a development
 * build, a source-archive build with no git, and a test run actually produce, and each must
 * degrade to an explicit unavailable marker rather than rendering an empty or bogus commit.
 */
class VersionProviderFormatTest {

    @Test
    void rendersStableVersionLineAndAttribution() {
        assertThat(App.VersionProvider.format("0.1.0", "50933dc2b9bf085df6fc0945874fa8071673e37f", "25.0.3"))
                .containsExactly(
                        "swath 0.1.0",
                        "Built by Varve: https://varve.io",
                        "Source: https://github.com/varveio/swath",
                        "Commit: 50933dc2b9bf",
                        "Runtime: 25.0.3");
    }

    @Test
    void rendersUnavailableCommitWhenUnavailable() {
        assertThat(App.VersionProvider.format("0.1.0", null, "25.0.3"))
                .containsExactly(
                        "swath 0.1.0",
                        "Built by Varve: https://varve.io",
                        "Source: https://github.com/varveio/swath",
                        "Commit: unavailable",
                        "Runtime: 25.0.3");
        assertThat(App.VersionProvider.format("0.1.0", "   ", "25.0.3")[3])
                .isEqualTo("Commit: unavailable");
    }

    @Test
    void rendersUnavailableForTheUnknownCommitSentinel() {
        // The build writes "unknown" when neither GITHUB_SHA nor git is available; it is a
        // build-side placeholder and must never surface to a user as if it were a commit.
        assertThat(App.VersionProvider.format("0.1.0", "unknown", "25.0.3")[3])
                .isEqualTo("Commit: unavailable");
    }

    @Test
    void fallsBackToDevelopmentWithoutAManifestVersion() {
        assertThat(App.VersionProvider.format(null, null, "25.0.3")[0]).isEqualTo("swath development");
        assertThat(App.VersionProvider.format(null, "50933dc2b9bf085df6fc0945874fa8071673e37f", "25.0.3")[3])
                .isEqualTo("Commit: 50933dc2b9bf");
    }

    @Test
    void doesNotOverrunAShorterCommitString() {
        assertThat(App.VersionProvider.format("0.1.0", "abc123", "25.0.3")[3])
                .isEqualTo("Commit: abc123");
    }
}
