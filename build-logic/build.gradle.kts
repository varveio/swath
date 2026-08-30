plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    // Make the Spotless plugin available to the precompiled `swath.java-conventions`
    // script plugin (which applies it by id to stamp/verify the SPDX license header).
    // Kept in step with `spotless` in gradle/libs.versions.toml.
    implementation("com.diffplug.spotless:spotless-plugin-gradle:7.0.2")
    // Same arrangement for the dependency-analysis plugin, which `swath.java-conventions`
    // applies by id so every module reports into the root's aggregated `buildHealth`.
    // Kept in step with `dependencyanalysis` in gradle/libs.versions.toml.
    implementation("com.autonomousapps:dependency-analysis-gradle-plugin:3.19.1")
}
