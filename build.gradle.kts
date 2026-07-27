// Aggregator root — no production src/ here. Every module applies the
// `swath.java-conventions` convention plugin from `build-logic/` individually; this
// file intentionally carries no build logic of its own beyond the plugin declarations
// needed for `./gradlew build`/`clean` etc. to fan out across all subprojects.

import com.github.jk1.license.LicenseReportExtension
import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.render.InventoryMarkdownReportRenderer
import com.github.jk1.license.render.JsonReportRenderer
import com.github.jk1.license.render.ReportRenderer
import org.gradle.api.tasks.Exec

plugins {
    // Dependency-license compliance gate (jk1). Kept off the root's own classpath
    // (`apply false`) and applied per-subproject below: jk1's root-aggregation mode
    // resolves each subproject's `runtimeClasspath` from a root task, which Gradle 9
    // forbids ("resolution without an exclusive lock"). Applying it in each module means
    // each `checkLicense` resolves only its OWN configuration — the supported pattern.
    alias(libs.plugins.license.report) apply false
    // Spotless itself is applied per-subproject by the `swath.java-conventions` convention
    // plugin (build-logic/), never here. Declaring it `apply false` at the root just makes
    // Gradle resolve ONE copy of the plugin for the whole build instead of a separate copy
    // per subproject classloader -- spotless-plugin-gradle 8.x's shared `SpotlessTaskService`
    // build service otherwise fails with "loaded with InstrumentingVisitableURLClassLoader(...)
    // using a provider ... loaded with InstrumentingVisitableURLClassLoader(...)" the moment a
    // second subproject applies it. See diffplug/spotless#747.
    alias(libs.plugins.spotless) apply false
}

val verifyReleaseVersion by tasks.registering {
    group = "verification"
    description = "Verifies a required stable vX.Y.Z release tag matches the canonical version."
    val releaseTag = providers.gradleProperty("releaseTag")
    inputs.property("releaseTag", releaseTag.orNull ?: "")
    doLast {
        val tag = checkNotNull(releaseTag.orNull) { "Release builds require -PreleaseTag=vX.Y.Z" }
        val stableSemVer = Regex("^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$")
        check(stableSemVer.matches(project.version.toString())) {
            "Release versions must be stable SemVer X.Y.Z; pre-release and build metadata are not supported"
        }
        check(Regex("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$").matches(tag)) {
            "Release tag must be stable SemVer vX.Y.Z; found $tag"
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
        // Only the runtime closure ships (fat jar / replay-server dist). Test-only, JMH,
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
