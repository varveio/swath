// Aggregator root — no production src/ here. Every module applies the
// `swath.java-conventions` convention plugin from `build-logic/` individually; this
// file intentionally carries no build logic of its own beyond the plugin declarations
// needed for `./gradlew build`/`clean` etc. to fan out across all subprojects.

import com.github.jk1.license.LicenseReportExtension
import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.render.InventoryMarkdownReportRenderer
import com.github.jk1.license.render.JsonReportRenderer
import com.github.jk1.license.render.ReportRenderer
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.tasks.Exec

plugins {
    // On-demand dependency audit: `./gradlew buildHealth` aggregates every module's report.
    // Deliberately NOT wired into `check` — see the note in swath.java-conventions.
    alias(libs.plugins.dependency.analysis)
    // Dependency-license compliance gate (jk1). Kept off the root's own classpath
    // (`apply false`) and applied per-subproject below: jk1's root-aggregation mode
    // resolves each subproject's `runtimeClasspath` from a root task, which Gradle 9
    // forbids ("resolution without an exclusive lock"). Applying it in each module means
    // each `checkLicense` resolves only its OWN configuration — the supported pattern.
    alias(libs.plugins.license.report) apply false
}

val verifyReleaseVersion by tasks.registering {
    group = "verification"
    description = "Verifies a required vX.Y.Z or vX.Y.Z-rc.N release tag matches the canonical version."
    val releaseTag = providers.gradleProperty("releaseTag")
    inputs.property("releaseTag", releaseTag.orNull ?: "")
    doLast {
        val tag = checkNotNull(releaseTag.orNull) { "Release builds require -PreleaseTag=vX.Y.Z" }
        // Stable X.Y.Z, or an -rc.N pre-release. Deliberately NOT the full SemVer
        // pre-release grammar: `-rc.N` is the only pre-release form this project ships, and
        // an exhaustive pattern would accept `-alpha`, `-beta.2+build`, and other identifiers
        // whose publish semantics nothing here implements. Build metadata (`+…`) stays
        // unsupported — it does not affect precedence, so it cannot mean anything useful on
        // a release tag.
        val releaseVersion = Regex("^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-rc\\.[1-9][0-9]*)?$")
        check(releaseVersion.matches(project.version.toString())) {
            "Release versions must be X.Y.Z or X.Y.Z-rc.N; found ${project.version}"
        }
        check(Regex("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-rc\\.[1-9][0-9]*)?$").matches(tag)) {
            "Release tag must be vX.Y.Z or vX.Y.Z-rc.N; found $tag"
        }
        val expected = "v${project.version}"
        check(tag == expected) {
            "Release tag $tag does not match canonical version $expected"
        }
    }
}

// License compliance (see THIRD_PARTY_NOTICES.md + config/license/). Per module:
//   ./gradlew checkLicense            — fails if any distributed dep's license is not on
//                                       config/license/allowed-licenses.json (the CI gate).
//   ./gradlew generateLicenseReport   — writes build/reports/licenses/ inventory + JSON.
// swath-cli's runtimeClasspath is the full shipping closure (the fat jar), so
// :swath-cli:checkLicense is the authoritative gate; the others cover their own dists.
subprojects {
    apply(plugin = "com.github.jk1.dependency-license-report")

    configure<LicenseReportExtension> {
        // Only the runtime closure ships (fat jar / replay dist). Test-only, JMH,
        // and annotation-processor deps are never distributed — out of scope.
        configurations = arrayOf("runtimeClasspath")

        outputDir = layout.buildDirectory.dir("reports/licenses").get().asFile.absolutePath

        // POM license spellings are wildly inconsistent ("Apache 2", "The Apache Software
        // License, Version 2.0", "ASL 2.0", …). Normalize to canonical names so the
        // allow-list matches a stable string. Built-in rules + swath-specific additions.
        filters = arrayOf(
            LicenseBundleNormalizer(
                rootProject.file("config/license/license-normalizer-bundle.json").path,
                /* createDefaultTransformationRules = */ true,
            ),
        )

        renderers = arrayOf<ReportRenderer>(
            InventoryMarkdownReportRenderer("third-party-report.md", "swath third-party dependencies"),
            JsonReportRenderer("licenses.json", false),
        )

        allowedLicensesFile = rootProject.file("config/license/allowed-licenses.json")
    }

    if (path in setOf(":swath-core", ":swath-cli", ":swath-replay")) {
        plugins.withId("java") {
            val runtime = configurations.getByName("runtimeClasspath")
            val withoutHadoop = configurations.create("parquetBaselineWithoutHadoop") {
                isCanBeConsumed = false
                isCanBeResolved = true
                extendsFrom(
                    configurations.getByName("implementation"),
                    configurations.getByName("runtimeOnly"),
                )
                exclude(group = "org.apache.hadoop")
                exclude(group = "org.apache.hadoop.thirdparty")
            }
            tasks.register("writeParquetDependencyBaseline") {
                group = "reporting"
                description = "Records this module's PR 7 runtime closure and Hadoop-attributable delta."
                val report = layout.buildDirectory.file(
                    "reports/parquet-linkability/dependency-baseline.jsonl")
                inputs.files(runtime, withoutHadoop)
                outputs.file(report)
                doLast {
                    fun externalArtifacts(configuration: org.gradle.api.artifacts.Configuration) =
                        configuration.incoming.artifacts.artifacts.mapNotNull { artifact ->
                            val component = artifact.id.componentIdentifier as? ModuleComponentIdentifier
                                ?: return@mapNotNull null
                            "${component.group}:${component.module}:${component.version}" to artifact.file
                        }.toMap()
                    fun json(fields: Map<String, String>): String = fields.entries.joinToString(
                        prefix = "{", postfix = "}") { (key, value) ->
                        val escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
                        "\"$key\":\"$escaped\""
                    }

                    val full = externalArtifacts(runtime)
                    val slim = externalArtifacts(withoutHadoop)
                    val attributable = full.keys - slim.keys
                    val lines = mutableListOf(json(linkedMapOf(
                        "format" to "swath-parquet-dependency-baseline-v1",
                        "record" to "closure_summary",
                        "project" to project.path,
                        "artifacts" to full.size.toString(),
                        "bytes" to full.values.sumOf { it.length() }.toString(),
                        "hadoop_attributable_artifacts" to attributable.size.toString(),
                        "hadoop_attributable_bytes" to
                            attributable.sumOf { full.getValue(it).length() }.toString(),
                    )))
                    full.toSortedMap().forEach { (coordinate, file) ->
                        lines += json(linkedMapOf(
                            "format" to "swath-parquet-dependency-baseline-v1",
                            "record" to "artifact",
                            "project" to project.path,
                            "coordinate" to coordinate,
                            "file" to file.name,
                            "bytes" to file.length().toString(),
                            "hadoop_attributable" to (coordinate in attributable).toString(),
                        ))
                    }
                    val output = report.get().asFile
                    output.parentFile.mkdirs()
                    output.writeText(lines.joinToString("\n", postfix = "\n"))
                }
            }
        }
    }
}

val cliLicenseReportDir = project(":swath-cli").layout.buildDirectory.dir("reports/licenses")
val thirdPartyNotices = layout.projectDirectory.file("THIRD_PARTY_NOTICES.md")
val cliRuntimeArtifacts = layout.buildDirectory.file("generated/legal/cli-runtime-artifacts.txt")

val writeCliRuntimeArtifactCoordinates by tasks.registering {
    group = "reporting"
    description = "Writes the exact external artifacts shaded into the CLI runtime."
    val runtimeClasspath = project(":swath-cli").configurations.named("runtimeClasspath")
    inputs.files(runtimeClasspath)
    outputs.file(cliRuntimeArtifacts)
    doLast {
        val output = cliRuntimeArtifacts.get().asFile
        output.parentFile.mkdirs()
        val artifacts = runtimeClasspath.get().resolvedConfiguration.resolvedArtifacts
            .map {
                "${it.moduleVersion.id.group}:${it.name}:${it.moduleVersion.id.version}\t${it.file.name}"
            }
            .distinct()
            .sorted()
        output.writeText(artifacts.joinToString("\n", postfix = "\n"))
    }
}

val generateThirdPartyNotices by tasks.registering(Exec::class) {
    group = "reporting"
    description = "Generates THIRD_PARTY_NOTICES.md from the shaded CLI runtime graph."
    dependsOn(":swath-cli:generateLicenseReport", writeCliRuntimeArtifactCoordinates)
    inputs.file(cliLicenseReportDir.map { it.file("licenses.json") })
    inputs.dir(cliLicenseReportDir)
    inputs.file(cliRuntimeArtifacts)
    inputs.file(layout.projectDirectory.file("scripts/legal/render-third-party-notices.py"))
    outputs.file(thirdPartyNotices)
    commandLine("python3", "scripts/legal/render-third-party-notices.py", "--licenses-json",
        cliLicenseReportDir.get().file("licenses.json").asFile, "--runtime-artifacts", cliRuntimeArtifacts.get().asFile,
        "--report-dir", cliLicenseReportDir.get().asFile, "--output", thirdPartyNotices.asFile)
}

val verifyThirdPartyNotices by tasks.registering {
    group = "verification"
    description = "Fails when checked-in THIRD_PARTY_NOTICES.md differs from the CLI runtime graph."
    dependsOn(":swath-cli:generateLicenseReport", writeCliRuntimeArtifactCoordinates)
    mustRunAfter(generateThirdPartyNotices)
    val generated = layout.buildDirectory.file("generated/legal/THIRD_PARTY_NOTICES.md")
    inputs.file(cliLicenseReportDir.map { it.file("licenses.json") })
    inputs.dir(cliLicenseReportDir)
    inputs.file(cliRuntimeArtifacts)
    inputs.file(thirdPartyNotices)
    inputs.file(layout.projectDirectory.file("scripts/legal/render-third-party-notices.py"))
    outputs.file(generated)
    doLast {
        val output = generated.get().asFile
        output.parentFile.mkdirs()
        providers.exec {
            commandLine("python3", "scripts/legal/render-third-party-notices.py", "--licenses-json",
                cliLicenseReportDir.get().file("licenses.json").asFile, "--runtime-artifacts", cliRuntimeArtifacts.get().asFile,
                "--report-dir", cliLicenseReportDir.get().asFile, "--output", output)
        }.result.get().assertNormalExitValue()
        if (!output.readBytes().contentEquals(thirdPartyNotices.asFile.readBytes())) {
            throw GradleException("THIRD_PARTY_NOTICES.md is stale; run ./gradlew generateThirdPartyNotices")
        }
    }
}

val replayLicenseReportDir = project(":swath-replay").layout.buildDirectory.dir("reports/licenses")
val replayThirdPartyNotices = layout.projectDirectory.file("swath-replay/THIRD_PARTY_NOTICES.md")
val replayRuntimeArtifacts = layout.buildDirectory.file("generated/legal/replay-runtime-artifacts.txt")

val generateReplayThirdPartyNotices by tasks.registering(Exec::class) {
    group = "reporting"
    description = "Generates swath-replay/THIRD_PARTY_NOTICES.md from the replay runtime graph."
    dependsOn(":swath-replay:generateLicenseReport", ":swath-replay:writeReplayRuntimeArtifactCoordinates")
    inputs.file(replayLicenseReportDir.map { it.file("licenses.json") })
    inputs.dir(replayLicenseReportDir)
    inputs.file(replayRuntimeArtifacts)
    inputs.file(layout.projectDirectory.file("scripts/legal/render-third-party-notices.py"))
    outputs.file(replayThirdPartyNotices)
    commandLine("python3", "scripts/legal/render-third-party-notices.py", "--licenses-json",
        replayLicenseReportDir.get().file("licenses.json").asFile,
        "--runtime-artifacts", replayRuntimeArtifacts.get().asFile,
        "--report-dir", replayLicenseReportDir.get().asFile,
        "--output", replayThirdPartyNotices.asFile,
        "--distribution", "replay")
}

val verifyReplayThirdPartyNotices by tasks.registering {
    group = "verification"
    description = "Fails when swath-replay/THIRD_PARTY_NOTICES.md differs from the replay runtime graph."
    dependsOn(":swath-replay:generateLicenseReport", ":swath-replay:writeReplayRuntimeArtifactCoordinates")
    mustRunAfter(generateReplayThirdPartyNotices)
    val generated = layout.buildDirectory.file("generated/legal/SWATH_REPLAY_THIRD_PARTY_NOTICES.md")
    inputs.file(replayLicenseReportDir.map { it.file("licenses.json") })
    inputs.dir(replayLicenseReportDir)
    inputs.file(replayRuntimeArtifacts)
    inputs.file(replayThirdPartyNotices)
    inputs.file(layout.projectDirectory.file("scripts/legal/render-third-party-notices.py"))
    outputs.file(generated)
    doLast {
        val output = generated.get().asFile
        output.parentFile.mkdirs()
        providers.exec {
            commandLine("python3", "scripts/legal/render-third-party-notices.py", "--licenses-json",
                replayLicenseReportDir.get().file("licenses.json").asFile,
                "--runtime-artifacts", replayRuntimeArtifacts.get().asFile,
                "--report-dir", replayLicenseReportDir.get().asFile,
                "--output", output,
                "--distribution", "replay")
        }.result.get().assertNormalExitValue()
        if (!output.readBytes().contentEquals(replayThirdPartyNotices.asFile.readBytes())) {
            throw GradleException(
                "swath-replay/THIRD_PARTY_NOTICES.md is stale; run ./gradlew generateReplayThirdPartyNotices",
            )
        }
    }
}

val parquetDependencyBaseline by tasks.registering {
    group = "reporting"
    description = "Records PR 7 runtime closures and distribution sizes with Hadoop-attributable deltas."
    dependsOn(
        ":swath-core:jar",
        ":swath-core:writeParquetDependencyBaseline",
        ":swath-cli:shadowJar",
        ":swath-cli:installDist",
        ":swath-cli:writeParquetDependencyBaseline",
        ":swath-replay:installDist",
        ":swath-replay:writeParquetDependencyBaseline",
    )
    val report = layout.buildDirectory.file("reports/parquet-linkability/dependency-baseline.jsonl")
    val moduleReports = listOf(":swath-core", ":swath-cli", ":swath-replay").map { projectPath ->
        project(projectPath).layout.buildDirectory.file(
            "reports/parquet-linkability/dependency-baseline.jsonl")
    }
    inputs.files(moduleReports)
    inputs.file(project(":swath-cli").layout.buildDirectory.file("libs/swath.jar"))
    inputs.dir(project(":swath-cli").layout.buildDirectory.dir("install/swath"))
    inputs.dir(project(":swath-replay").layout.buildDirectory.dir("install/swath-replay"))
    outputs.file(report)
    doLast {
        fun json(fields: Map<String, String>): String = fields.entries.joinToString(
            prefix = "{",
            postfix = "}",
        ) { (key, value) ->
            val escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
            "\"$key\":\"$escaped\""
        }

        val lines = mutableListOf<String>()
        moduleReports.forEach { moduleReport ->
            val input = moduleReport.get().asFile
            lines += input.readLines()
        }

        val packages = linkedMapOf(
            "cli_fat_jar" to project(":swath-cli").layout.buildDirectory.file("libs/swath.jar").get().asFile,
            "cli_install" to project(":swath-cli").layout.buildDirectory.dir("install/swath").get().asFile,
            "replay_install" to project(":swath-replay").layout.buildDirectory.dir("install/swath-replay").get().asFile,
        )
        packages.forEach { (name, path) ->
            val files = if (path.isDirectory) path.walkTopDown().filter { it.isFile }.toList() else listOf(path)
            lines += json(linkedMapOf(
                "format" to "swath-parquet-dependency-baseline-v1",
                "record" to "package",
                "package" to name,
                "files" to files.size.toString(),
                "jars" to files.count { it.extension == "jar" }.toString(),
                "bytes" to files.sumOf { it.length() }.toString(),
            ))
        }
        val output = report.get().asFile
        output.parentFile.mkdirs()
        output.writeText(lines.joinToString("\n", postfix = "\n"))
    }
}

tasks.register("parquetBaseline") {
    group = "verification"
    description = "Runs the complete PR 7 linkability and current-runtime baseline suite."
    dependsOn(
        ":swath-core:parquetLinkability",
        ":swath-cli:parquetStartupBaseline",
        parquetDependencyBaseline,
    )
}
