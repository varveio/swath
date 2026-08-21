#!/usr/bin/env python3
"""Render a shipped runtime's legal bundle from jk1's resolved-runtime report."""

import argparse
import json
from pathlib import Path


ZSTD_JNI_PREFIX = "com.github.luben:zstd-jni:"

ZSTD_JNI_WRAPPER_LICENSE = """Zstd-jni: JNI bindings to Zstd Library

Copyright (c) 2015-present, Luben Karavelov/ All rights reserved.

BSD License

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this
  list of conditions and the following disclaimer in the documentation and/or
  other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."""

ZSTANDARD_NATIVE_BSD_LICENSE = """BSD License

For Zstandard software

Copyright (c) 2016-present, Facebook, Inc. All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

 * Neither the name Facebook nor the names of its contributors may be used to
   endorse or promote products derived from this software without specific
   prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."""


def markdown_indented_block(text):
    """Render literal notice text using the document's indented-code-block style."""
    return "\n".join(f"    {line}" if line else "" for line in text.splitlines())


def embedded_legal_files(report_dir: Path, runtime_artifact_files: set[str]):
    for artifact_dir in sorted(
        path for path in report_dir.iterdir()
        if path.is_dir() and path.name in runtime_artifact_files
    ):
        files = sorted(
            path for path in artifact_dir.rglob("*")
            if path.is_file() and path.name.upper().startswith("NOTICE")
        )
        if files:
            yield artifact_dir.name, files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--licenses-json", type=Path, required=True)
    parser.add_argument("--runtime-artifacts", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--distribution", choices=("cli", "replay"), default="cli")
    args = parser.parse_args()

    dependencies = json.loads(args.licenses_json.read_text(encoding="utf-8"))["dependencies"]
    runtime_coordinates = set()
    runtime_artifact_files = set()
    for line_number, line in enumerate(
        args.runtime_artifacts.read_text(encoding="utf-8").splitlines(), start=1
    ):
        try:
            coordinate, artifact_file = line.split("\t", maxsplit=1)
        except ValueError as error:
            raise ValueError(
                f"{args.runtime_artifacts}:{line_number}: expected coordinate<TAB>artifact-file"
            ) from error
        if not coordinate or not artifact_file:
            raise ValueError(
                f"{args.runtime_artifacts}:{line_number}: coordinate and artifact-file are required"
            )
        runtime_coordinates.add(coordinate)
        runtime_artifact_files.add(artifact_file)
    dependencies = [
        dependency for dependency in dependencies
        if f"{dependency['moduleName']}:{dependency['moduleVersion']}" in runtime_coordinates
    ]
    dependencies.sort(key=lambda dependency: (dependency["moduleName"], dependency["moduleVersion"]))
    if args.distribution == "cli":
        ownership = [
            "This file is generated from the resolved `:swath-cli` `runtimeClasspath`, the exact",
            "dependency closure shaded into `swath.jar`. Do not edit it by hand.",
        ]
        command = "./gradlew generateThirdPartyNotices verifyThirdPartyNotices"
        packaging = [
            "The shaded jar separately retains merged `META-INF/LICENSE*` and `META-INF/NOTICE*`",
            "resources.",
        ]
    else:
        ownership = [
            "This file is generated from the resolved `:swath-replay` `runtimeClasspath`, the exact",
            "dependency closure packaged in the replay application distribution. Do not edit it by hand.",
        ]
        command = "./gradlew generateReplayThirdPartyNotices verifyReplayThirdPartyNotices"
        packaging = [
            "The replay distribution separately retains each dependency jar's embedded legal",
            "resources.",
        ]

    lines = [
        "# Third-Party Notices", "",
        *ownership, "",
        "Regenerate and verify it with:", "",
        f"    {command}", "",
        "The inventory is derived from the dependency-license-report JSON. Embedded upstream",
        "notices are copied from matching `META-INF/NOTICE*` resources; the pinned Zstandard",
        "wrapper and native-library terms are rendered explicitly because zstd-jni's binary jar",
        "does not carry those source-tree license files.",
        *packaging, "", "## Runtime dependency inventory", "",
    ]
    for dependency in dependencies:
        licenses = "; ".join(
            license_["moduleLicense"] or "No license declared in resolved metadata"
            for license_ in dependency["moduleLicenses"]
        ) or "No license declared in resolved metadata"
        lines.append(f"- `{dependency['moduleName']}:{dependency['moduleVersion']}` — {licenses}")
    zstd_jni = next(
        (coordinate for coordinate in runtime_coordinates if coordinate.startswith(ZSTD_JNI_PREFIX)),
        None,
    )
    if zstd_jni:
        lines.extend([
            "", "## Bundled Zstandard legal notices", "",
            f"The `{zstd_jni}` runtime contains both the zstd-jni wrapper and bundled native",
            "Zstandard code. The wrapper's BSD 2-Clause terms and the native library's BSD",
            "3-Clause terms are reproduced separately below.", "",
            "### zstd-jni wrapper — BSD 2-Clause", "",
            markdown_indented_block(ZSTD_JNI_WRAPPER_LICENSE), "",
            "### Native Zstandard library — BSD 3-Clause", "",
            markdown_indented_block(ZSTANDARD_NATIVE_BSD_LICENSE),
        ])
    lines.extend(["", "## Embedded upstream notice resources", ""])
    for artifact, files in embedded_legal_files(args.report_dir, runtime_artifact_files):
        lines.extend([f"### {artifact}", ""])
        for resource in files:
            relative = resource.relative_to(args.report_dir / artifact)
            text = "\n".join(line.rstrip() for line in resource.read_text(
                encoding="utf-8", errors="replace").splitlines()).rstrip()
            lines.extend([f"#### {relative}", "", markdown_indented_block(text), ""])
    args.output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
