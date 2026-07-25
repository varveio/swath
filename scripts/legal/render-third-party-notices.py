#!/usr/bin/env python3
"""Render the CLI runtime legal bundle from jk1's resolved-runtime report."""

import argparse
import json
from pathlib import Path


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
    lines = [
        "# Third-Party Notices", "",
        "This file is generated from the resolved `:swath-cli` `runtimeClasspath`, the exact",
        "dependency closure shaded into `swath.jar`. Do not edit it by hand.", "",
        "Regenerate and verify it with:", "",
        "    ./gradlew generateThirdPartyNotices verifyThirdPartyNotices", "",
        "The inventory is derived from the dependency-license-report JSON and the legal text below",
        "is copied from the matching artifacts' embedded `META-INF/NOTICE*` resources.",
        "The shaded jar separately retains merged `META-INF/LICENSE*` and `META-INF/NOTICE*`",
        "resources.", "", "## Runtime dependency inventory", "",
    ]
    for dependency in dependencies:
        licenses = "; ".join(
            license_["moduleLicense"] or "No license declared in resolved metadata"
            for license_ in dependency["moduleLicenses"]
        ) or "No license declared in resolved metadata"
        lines.append(f"- `{dependency['moduleName']}:{dependency['moduleVersion']}` — {licenses}")
    lines.extend(["", "## Embedded upstream notice resources", ""])
    for artifact, files in embedded_legal_files(args.report_dir, runtime_artifact_files):
        lines.extend([f"### {artifact}", ""])
        for resource in files:
            relative = resource.relative_to(args.report_dir / artifact)
            text = "\n".join(line.rstrip() for line in resource.read_text(
                encoding="utf-8", errors="replace").splitlines()).rstrip()
            lines.extend([f"#### {relative}", "", "```text", text, "```", ""])
    args.output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
