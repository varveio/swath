/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** Guards the newcomer-facing documentation paths and release-critical wording. */
final class DocumentationEntryPointTest {

    private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    private static final List<String> ENTRY_POINTS = List.of(
            "README.md",
            "ROADMAP.md",
            "AGENTS.md",
            "docs/README.md",
            "docs/getting-started.md",
            "docs/full-scale-demo.md",
            "docs/install.md",
            "docs/usage.md",
            "docs/operating.md",
            "docs/faq.md",
            "docs/style.md");

    private static final Pattern MARKDOWN_LINK =
            Pattern.compile("!?\\[[^]\\n]*]\\(([^)\\s]+)(?:\\s+[^)]*)?\\)");
    private static final Pattern CAPITALIZED_PROJECT_NAME = Pattern.compile("\\bSwath\\b");

    @Test
    void entryPointLocalLinkTargetsExist() throws Exception {
        List<String> failures = new ArrayList<>();
        for (String relative : ENTRY_POINTS) {
            Path source = ROOT.resolve(relative);
            assertThat(source).as(relative).isRegularFile();
            checkTargets(source, failures);
        }
        assertThat(failures).isEmpty();
    }

    @Test
    void entryPointsUseTheLowercaseProjectName() throws Exception {
        for (String relative : ENTRY_POINTS) {
            assertThat(CAPITALIZED_PROJECT_NAME.matcher(read(relative)).find())
                    .as(relative + " contains capitalized project name")
                    .isFalse();
        }
    }

    @Test
    void releaseEntryPointsUseTheNewcomerFirstStory() throws Exception {
        String readme = read("README.md");
        String gettingStarted = read("docs/getting-started.md");
        String fullScale = read("docs/full-scale-demo.md");
        String usage = read("docs/usage.md");
        String roadmap = read("ROADMAP.md");
        String agentGuide = read("AGENTS.md");

        assertThat(readme)
                .contains("Parallel, resumable S3 listing for very large buckets")
                .contains("stofs_2d_glo.20230113/")
                .contains("## See it at full scale")
                .contains("not a point-in-time snapshot")
                .doesNotContain("-o inventory.parquet");
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

    private static void checkTargets(Path source, List<String> failures) throws IOException {
        Matcher matcher = MARKDOWN_LINK.matcher(Files.readString(source));
        while (matcher.find()) {
            String raw = matcher.group(1);
            if (raw.startsWith("http://") || raw.startsWith("https://")
                    || raw.startsWith("mailto:")) {
                continue;
            }
            int hash = raw.indexOf('#');
            String pathPart = hash >= 0 ? raw.substring(0, hash) : raw;
            int query = pathPart.indexOf('?');
            if (query >= 0) {
                pathPart = pathPart.substring(0, query);
            }
            Path target = pathPart.isEmpty()
                    ? source
                    : source.getParent().resolve(decode(pathPart)).normalize();
            String origin = ROOT.relativize(source) + " -> " + raw;
            if (!target.startsWith(ROOT)) {
                failures.add(origin + " escapes the repository");
            } else if (!Files.exists(target)) {
                failures.add(origin + " does not exist at " + ROOT.relativize(target));
            }
        }
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
