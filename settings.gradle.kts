pluginManagement {
    includeBuild("build-logic")
}

rootProject.name = "swath"

include(":swath-model", ":swath-core", ":swath-s3", ":swath-cli", ":swath-replay-server")
