#!/usr/bin/env python3
"""report-test-counts — report what a test tier actually executed, and fail an empty run.

The opt-in tiers select tests by tag (``-Pdeep``, ``-PonlyPerf``, the
``-Dswath.it.sigkill=true`` ITs). A mis-typed property, a renamed tag, or a cached task
result therefore produces an *empty* run, and an empty run reports green -- the failure
mode the nightly deep job spent weeks in. This script turns "the tier executed nothing"
into a red step, and prints the per-module counts the nightly log was missing.

Self-contained (stdlib only) so the public CI has no external package dependency.
The `.sh` wrapper CI invokes execs this file directly.

Usage:
    scripts/ci/report-test-counts.py --repo-root PATH <label> [module...]
    scripts/ci/report-test-counts.sh <label> [module...]  (CI entry point)

Exit codes: 0 = tests executed, 1 = nothing executed, 2 = script error.
"""

from __future__ import annotations

import argparse
import os
import sys
import xml.etree.ElementTree as ElementTree
from pathlib import Path

# Where Gradle's XML test reports land, relative to a module directory. Gradle recreates
# this directory when the module's `test` task executes, so its contents describe the most
# recent execution rather than an accumulation across tiers.
RESULTS_GLOB = "build/test-results/test/TEST-*.xml"


class ModuleCounts:
    """Per-module totals across every test suite the module reported."""

    def __init__(self, module: str) -> None:
        self.module = module
        self.tests = 0
        self.skipped = 0
        self.failures = 0
        self.errors = 0

    @property
    def executed(self) -> int:
        """Tests that actually ran: JUnit counts a skipped test in ``tests`` too."""
        return self.tests - self.skipped

    def add(self, suite: ElementTree.Element) -> None:
        self.tests += int(suite.get("tests", 0))
        self.skipped += int(suite.get("skipped", 0))
        self.failures += int(suite.get("failures", 0))
        self.errors += int(suite.get("errors", 0))


def collect(repo_root: Path, modules: list[str]) -> list[ModuleCounts]:
    """Read every module's JUnit XML, in the module order given (or sorted, if none)."""
    candidates = (
        [repo_root / module for module in modules]
        if modules
        else sorted(path for path in repo_root.iterdir() if (path / "build/test-results/test").is_dir())
    )
    collected = []
    for directory in candidates:
        reports = sorted(directory.glob(RESULTS_GLOB))
        if not reports:
            continue
        counts = ModuleCounts(directory.name)
        for report in reports:
            try:
                counts.add(ElementTree.parse(report).getroot())
            except ElementTree.ParseError as error:
                raise SystemExit(f"error: unreadable test report {report}: {error}") from error
        collected.append(counts)
    return collected


def report(label: str, collected: list[ModuleCounts]) -> str:
    """Render the GitHub-flavoured summary table (also readable in a plain log)."""
    lines = [f"### {label} — executed tests", "", "| module | executed | skipped | failed |", "| --- | --: | --: | --: |"]
    for counts in collected:
        failed = counts.failures + counts.errors
        lines.append(f"| {counts.module} | {counts.executed} | {counts.skipped} | {failed} |")
    total = sum(counts.executed for counts in collected)
    lines.append(f"| **total** | **{total}** | | |")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("label", help='what ran, e.g. "deep tier"')
    parser.add_argument("modules", nargs="*", help="restrict to these modules (default: all with results)")
    args = parser.parse_args()

    collected = collect(args.repo_root, args.modules)
    summary = report(args.label, collected)
    print(summary)

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a", encoding="utf-8") as handle:
            handle.write(summary + "\n")

    if sum(counts.executed for counts in collected) == 0:
        print(
            f"::error::{args.label} executed no tests — the tag filter matched nothing, "
            "or the task result was reused. A tier that runs nothing is not a passing tier.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
