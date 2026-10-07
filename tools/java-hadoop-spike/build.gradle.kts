/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
plugins { java }
layout.buildDirectory.set(file(providers.gradleProperty("probeBuildDir").orElse("/tmp/swath-java-hadoop-build")))
repositories { mavenCentral() }
dependencies {
 implementation("org.apache.parquet:parquet-hadoop:1.18.1") { exclude(group = "org.apache.hadoop") }
 implementation("com.github.luben:zstd-jni:1.5.7-15")
 compileOnly("org.apache.hadoop:hadoop-common:3.4.1")
 compileOnly("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1")
}
tasks.register("classpath") { doLast { println(configurations.runtimeClasspath.get().asPath) } }
tasks.register("compileClasspath") { doLast { println(configurations.compileClasspath.get().asPath) } }
