/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Guards the historical Java release documentation's wording. */
final class DocumentationEntryPointTest {

    private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

    @Test
    void javaReleaseEntryPointsKeepTheHistoricalWorkflow() throws Exception {
        String gettingStarted = read("docs/getting-started.md");
        String fullScale = read("docs/full-scale-demo.md");
        String usage = read("docs/usage.md");
        String roadmap = read("ROADMAP.md");
        String agentGuide = read("AGENTS.md");

        assertThat(gettingStarted)
                .contains("small queryable Parquet inventory")
                .contains("stofs_2d_glo.20230113/")
                .contains("Use a directory path")
                .contains("not a point-in-time snapshot");
        assertThat(fullScale)
                .contains("39,585,029")
                .contains("swath v0.2.1")
                .contains("full public bucket");
        assertThat(usage)
                .contains("The listing-scope `args_hash`, filter specification, and output/run identity")
                .contains("Avoid `-o inventory.parquet`")
                .doesNotContain("complete snapshot");
        assertThat(roadmap)
                .doesNotContain("Parallel sort-merge promotion")
                .doesNotContain("ships off-by-default");
        assertThat(agentGuide).doesNotContain("No AI attribution");
    }

    private static String read(String relative) throws IOException {
        return Files.readString(ROOT.resolve(relative));
    }
}
