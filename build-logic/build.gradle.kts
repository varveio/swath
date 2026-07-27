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
    implementation("com.diffplug.spotless:spotless-plugin-gradle:8.8.0")
}
