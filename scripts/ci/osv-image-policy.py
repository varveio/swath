#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Apply swath's fail-closed policy to an OSV-Scanner image JSON report.

The scanner's numeric CVSS score is deliberately not the Ubuntu gate. Canonical's
Ubuntu priority includes distro-specific reachability and exposure, so Ubuntu findings
are gated on the ``Ubuntu`` severity carried by the member ``UBUNTU-CVE`` records.
"""

from __future__ import annotations

import argparse
from collections import Counter
import datetime
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
import re
import sys
import tempfile
import tomllib
import unittest


PRIORITY_ORDER = {
    "negligible": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}
UBUNTU_ECOSYSTEM = re.compile(
    r"^Ubuntu(?::Pro)?:([0-9]+\.[0-9]+)(?::LTS)?$"
)


class SchemaError(ValueError):
    """The scanner report cannot be interpreted without guessing."""


@dataclass
class Occurrence:
    key: tuple[str, ...]
    display_id: str
    disposition: str
    priority: str
    package: str
    fixed_versions: set[str] = field(default_factory=set)


def require_dict(value: object, where: str) -> dict:
    if not isinstance(value, dict):
        raise SchemaError(f"{where} must be an object")
    return value


def require_list(value: object, where: str) -> list:
    if not isinstance(value, list):
        raise SchemaError(f"{where} must be an array")
    return value


def string_list(value: object, where: str) -> list[str]:
    values = require_list(value, where)
    if not all(isinstance(item, str) and item for item in values):
        raise SchemaError(f"{where} must contain non-empty strings")
    return values


def ubuntu_release(ecosystem: str) -> str | None:
    match = UBUNTU_ECOSYSTEM.fullmatch(ecosystem)
    return match.group(1) if match else None


def ubuntu_priority(vulnerability: dict, where: str) -> str:
    severities = require_list(vulnerability.get("severity", []), f"{where}.severity")
    priorities: list[str] = []
    for index, severity_value in enumerate(severities):
        severity = require_dict(severity_value, f"{where}.severity[{index}]")
        if severity.get("type") == "Ubuntu":
            score = severity.get("score")
            if not isinstance(score, str) or not score:
                raise SchemaError(f"{where}.severity[{index}].score must be a string")
            priorities.append(score.lower())
    if not priorities:
        return "unknown"
    if len(set(priorities)) != 1:
        raise SchemaError(f"{where} has conflicting Ubuntu priorities: {priorities}")
    priority = priorities[0]
    return priority if priority in PRIORITY_ORDER else "unknown"


def fixed_versions(vulnerability: dict, package: dict, where: str) -> set[str]:
    release = ubuntu_release(package["ecosystem"])
    fixes: set[str] = set()
    affected_values = require_list(vulnerability.get("affected", []), f"{where}.affected")
    for affected_index, affected_value in enumerate(affected_values):
        affected = require_dict(
            affected_value, f"{where}.affected[{affected_index}]"
        )
        affected_package = require_dict(
            affected.get("package", {}),
            f"{where}.affected[{affected_index}].package",
        )
        affected_ecosystem = affected_package.get("ecosystem")
        if not isinstance(affected_ecosystem, str):
            raise SchemaError(
                f"{where}.affected[{affected_index}].package.ecosystem must be a string"
            )
        if affected_package.get("name") != package["name"]:
            continue
        if ubuntu_release(affected_ecosystem) != release:
            continue
        ranges = require_list(
            affected.get("ranges", []), f"{where}.affected[{affected_index}].ranges"
        )
        for range_index, range_value in enumerate(ranges):
            affected_range = require_dict(
                range_value,
                f"{where}.affected[{affected_index}].ranges[{range_index}]",
            )
            events = require_list(
                affected_range.get("events", []),
                f"{where}.affected[{affected_index}].ranges[{range_index}].events",
            )
            for event_index, event_value in enumerate(events):
                event = require_dict(
                    event_value,
                    f"{where}.affected[{affected_index}].ranges[{range_index}]"
                    f".events[{event_index}]",
                )
                if "fixed" in event:
                    fixed = event["fixed"]
                    if not isinstance(fixed, str) or not fixed:
                        raise SchemaError(
                            f"{where} contains a non-string fixed version"
                        )
                    fixes.add(fixed)
    return fixes


def group_member_ids(group: dict, vulnerabilities: dict[str, dict]) -> set[str]:
    # `aliases` can name every CVE covered by a broad USN even when only a subset affects
    # this binary/version. `ids` is OSV-Scanner's exact set for this occurrence.
    identifiers = set(group["ids"])
    members = {value for value in identifiers if value.startswith("UBUNTU-CVE-")}
    # The partition check assigns every vulnerability document to exactly one group. A USN
    # can relate to CVEs owned by another group, so relation traversal must stay inside this
    # group's IDs rather than importing every document named by `related` or `upstream`.
    for vulnerability_id in identifiers:
        vulnerability = vulnerabilities[vulnerability_id]
        for field_name in ("aliases", "related", "upstream"):
            if field_name not in vulnerability:
                continue
            related = string_list(
                vulnerability[field_name],
                f"vulnerability {vulnerability_id}.{field_name}",
            )
            members.update(
                value
                for value in related
                if value in identifiers and value.startswith("UBUNTU-CVE-")
            )
    return members


def parse_report(
    document: object, java_exceptions: set[str] | None = None
) -> list[Occurrence]:
    java_exceptions = java_exceptions or set()
    root = require_dict(document, "report")
    results = require_list(root.get("results"), "report.results")
    occurrences: list[Occurrence] = []
    for result_index, result_value in enumerate(results):
        result = require_dict(result_value, f"results[{result_index}]")
        packages = require_list(
            result.get("packages"), f"results[{result_index}].packages"
        )
        source = require_dict(result.get("source", {}), f"results[{result_index}].source")
        source_path = source.get("path", "<image>")
        if not isinstance(source_path, str):
            raise SchemaError(f"results[{result_index}].source.path must be a string")
        for package_index, package_value in enumerate(packages):
            where = f"results[{result_index}].packages[{package_index}]"
            entry = require_dict(package_value, where)
            package = require_dict(entry.get("package"), f"{where}.package")
            for field_name in ("name", "version", "ecosystem"):
                if not isinstance(package.get(field_name), str) or not package[field_name]:
                    raise SchemaError(f"{where}.package.{field_name} must be a string")
            groups = require_list(entry.get("groups"), f"{where}.groups")
            vulnerability_values = require_list(
                entry.get("vulnerabilities"), f"{where}.vulnerabilities"
            )
            vulnerabilities: dict[str, dict] = {}
            for vulnerability_index, vulnerability_value in enumerate(
                vulnerability_values
            ):
                vulnerability = require_dict(
                    vulnerability_value,
                    f"{where}.vulnerabilities[{vulnerability_index}]",
                )
                vulnerability_id = vulnerability.get("id")
                if not isinstance(vulnerability_id, str) or not vulnerability_id:
                    raise SchemaError(
                        f"{where}.vulnerabilities[{vulnerability_index}].id"
                        " must be a string"
                    )
                if vulnerability_id in vulnerabilities:
                    raise SchemaError(f"{where} repeats vulnerability {vulnerability_id}")
                vulnerabilities[vulnerability_id] = vulnerability

            normalized_groups: list[tuple[dict, list[str], str]] = []
            grouped_ids: Counter[str] = Counter()
            for group_index, group_value in enumerate(groups):
                group_where = f"{where}.groups[{group_index}]"
                group = require_dict(group_value, group_where)
                ids = string_list(group.get("ids"), f"{group_where}.ids")
                if not ids:
                    raise SchemaError(f"{group_where}.ids must not be empty")
                group["ids"] = ids
                group["aliases"] = string_list(
                    group.get("aliases", []), f"{group_where}.aliases"
                )
                normalized_groups.append((group, ids, group_where))
                grouped_ids.update(ids)
            missing_documents = sorted(grouped_ids.keys() - vulnerabilities.keys())
            if missing_documents:
                raise SchemaError(
                    f"{where} groups reference missing vulnerability documents: "
                    f"{missing_documents}"
                )
            ungrouped = sorted(vulnerabilities.keys() - grouped_ids.keys())
            if ungrouped:
                raise SchemaError(
                    f"{where} has ungrouped vulnerability documents: {ungrouped}"
                )
            multiply_grouped = sorted(
                vulnerability_id
                for vulnerability_id, count in grouped_ids.items()
                if count != 1
            )
            if multiply_grouped:
                raise SchemaError(
                    f"{where} groups vulnerability documents more than once: "
                    f"{multiply_grouped}"
                )

            package_label = (
                f"{package['name']}@{package['version']} ({package['ecosystem']}, "
                f"{source_path})"
            )
            for group, ids, group_where in normalized_groups:
                key = tuple(sorted(set(ids)))
                display_id = next(
                    (value for value in ids if value.startswith("USN-")), ids[0]
                )
                if ubuntu_release(package["ecosystem"]) is None:
                    excepted = set(ids).issubset(java_exceptions)
                    occurrences.append(
                        Occurrence(
                            key=key,
                            display_id=display_id,
                            disposition="EXCEPT" if excepted else "FAIL",
                            priority="Java-exception" if excepted else "non-Ubuntu",
                            package=package_label,
                        )
                    )
                    continue

                members = group_member_ids(group, vulnerabilities)
                if not members:
                    occurrences.append(
                        Occurrence(
                            key=key,
                            display_id=display_id,
                            disposition="FAIL",
                            priority="unknown",
                            package=package_label,
                        )
                    )
                    continue
                missing = sorted(members - vulnerabilities.keys())
                if missing:
                    raise SchemaError(
                        f"{group_where} references missing Ubuntu members: {missing}"
                    )
                priorities: list[str] = []
                fixes: set[str] = set()
                for member in sorted(members):
                    vulnerability = vulnerabilities[member]
                    priorities.append(
                        ubuntu_priority(vulnerability, f"vulnerability {member}")
                    )
                    fixes.update(
                        fixed_versions(
                            vulnerability, package, f"vulnerability {member}"
                        )
                    )
                priority = (
                    "unknown"
                    if "unknown" in priorities
                    else max(priorities, key=PRIORITY_ORDER.__getitem__)
                )
                disposition = (
                    "REPORT"
                    if priority in {"negligible", "low", "medium"}
                    else "FAIL"
                )
                occurrences.append(
                    Occurrence(
                        key=key,
                        display_id=display_id,
                        disposition=disposition,
                        priority=priority,
                        package=package_label,
                        fixed_versions=fixes,
                    )
                )
    return occurrences


def render(occurrences: list[Occurrence], output: io.TextIOBase) -> int:
    grouped: dict[tuple[str, ...], list[Occurrence]] = {}
    for occurrence in occurrences:
        grouped.setdefault(occurrence.key, []).append(occurrence)
    sorted_groups = sorted(grouped.values(), key=lambda items: items[0].display_id)
    for values in sorted_groups:
        dispositions = {value.disposition for value in values}
        priorities = {value.priority for value in values}
        if len(dispositions) != 1 or len(priorities) != 1:
            raise SchemaError(
                f"advisory group {values[0].display_id} has inconsistent classifications"
            )

    print(
        f"OSV image policy: {len(grouped)} advisory group(s), "
        f"{len(occurrences)} package occurrence(s)",
        file=output,
    )
    rollup = {priority: [0, 0] for priority in (*PRIORITY_ORDER, "unknown")}
    for values in sorted_groups:
        priority = values[0].priority
        if priority in rollup:
            rollup[priority][0] += 1
            rollup[priority][1] += len(values)
    print(
        "Ubuntu priority summary: "
        + ", ".join(
            f"{priority}={counts[0]} group(s)/{counts[1]} occurrence(s)"
            for priority, counts in rollup.items()
        ),
        file=output,
    )

    failures = 0
    for values in sorted_groups:
        disposition = values[0].disposition
        if disposition == "FAIL":
            failures += 1
        fixes = sorted(
            {fixed for value in values for fixed in value.fixed_versions}
        )
        fix_summary = (
            f"fixed version(s) listed: {', '.join(fixes)}"
            if fixes
            else "no fixed version listed"
        )
        packages = sorted({value.package for value in values})
        print(
            f"{disposition} {values[0].priority} {values[0].display_id}: "
            f"{len(values)} occurrence(s); {fix_summary}; "
            f"packages: {'; '.join(packages)}",
            file=output,
        )
    if failures:
        print(
            f"OSV image policy rejected {failures} advisory group(s).", file=output
        )
        return 1
    return 0


def load_java_exceptions(path: Path, today: datetime.date | None = None) -> set[str]:
    today = today or datetime.date.today()
    try:
        with path.open("rb") as config_file:
            config = tomllib.load(config_file)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise SchemaError(f"cannot read Java exception config: {error}") from error
    entries = require_list(config.get("IgnoredVulns", []), "IgnoredVulns")
    active: set[str] = set()
    seen: set[str] = set()
    for index, entry_value in enumerate(entries):
        entry = require_dict(entry_value, f"IgnoredVulns[{index}]")
        vulnerability_id = entry.get("id")
        expiry = entry.get("ignoreUntil")
        reason = entry.get("reason")
        if not isinstance(vulnerability_id, str) or not vulnerability_id:
            raise SchemaError(f"IgnoredVulns[{index}].id must be a string")
        if vulnerability_id in seen:
            raise SchemaError(f"duplicate Java exception {vulnerability_id}")
        seen.add(vulnerability_id)
        # datetime.datetime subclasses datetime.date; require a bare TOML local date so
        # comparison cannot raise TypeError and silently turn a policy error into rc=1.
        if type(expiry) is not datetime.date:
            raise SchemaError(f"IgnoredVulns[{index}].ignoreUntil must be a TOML date")
        if not isinstance(reason, str) or not reason.strip():
            raise SchemaError(f"IgnoredVulns[{index}].reason must be a non-empty string")
        # Match OSV-Scanner 2.5.1: ignoreUntil is active only while it is after today.
        if expiry > today:
            active.add(vulnerability_id)
    return active


def evaluate(
    path: Path,
    scanner_exit_code: int,
    output: io.TextIOBase,
    java_exceptions: set[str] | None = None,
) -> int:
    if scanner_exit_code not in (0, 1):
        print(
            f"OSV-Scanner failed with exit code {scanner_exit_code}; policy not evaluated.",
            file=output,
        )
        return 2
    try:
        with path.open(encoding="utf-8") as report_file:
            document = json.load(report_file)
        occurrences = parse_report(document, java_exceptions)
        if scanner_exit_code == 0 and occurrences:
            raise SchemaError("scanner returned 0 but the report contains findings")
        if scanner_exit_code == 1 and not occurrences:
            raise SchemaError("scanner returned 1 but the report contains no findings")
        return render(occurrences, output)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, SchemaError) as error:
        print(f"OSV image policy could not safely interpret the report: {error}", file=output)
        return 2


def vulnerability(
    vulnerability_id: str,
    priority: str | None,
    *,
    related: list[str] | None = None,
    fixed: str | None = None,
) -> dict:
    severity = (
        [{"type": "Ubuntu", "score": priority}] if priority is not None else []
    )
    events = [{"introduced": "0"}]
    if fixed is not None:
        events.append({"fixed": fixed})
    value = {
        "id": vulnerability_id,
        "severity": severity,
        "affected": [
            {
                "package": {"name": "pkg", "ecosystem": "Ubuntu:24.04:LTS"},
                "ranges": [{"type": "ECOSYSTEM", "events": events}],
            }
        ],
    }
    if related is not None:
        value["related"] = related
    return value


def report(
    groups: list[dict],
    vulnerabilities: list[dict],
    *,
    ecosystem: str = "Ubuntu:24.04",
    duplicate: bool = False,
) -> dict:
    package_entry = {
        "package": {"name": "pkg", "version": "1", "ecosystem": ecosystem},
        "groups": groups,
        "vulnerabilities": vulnerabilities,
    }
    packages = [package_entry, package_entry] if duplicate else [package_entry]
    return {
        "results": [
            {"source": {"path": "/image"}, "packages": packages}
        ]
    }


class PolicySelfTest(unittest.TestCase):
    def evaluate_document(
        self,
        document: object,
        scanner_rc: int = 1,
        java_exceptions: set[str] | None = None,
    ) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            output = io.StringIO()
            return (
                evaluate(path, scanner_rc, output, java_exceptions),
                output.getvalue(),
            )

    def test_cvss_high_but_ubuntu_medium_reports_and_passes(self) -> None:
        member = vulnerability("UBUNTU-CVE-1", "medium", fixed="2")
        member["severity"].insert(0, {"type": "CVSS_V3", "score": "9.8"})
        code, output = self.evaluate_document(
            report(
                [{"ids": ["UBUNTU-CVE-1"], "aliases": [], "max_severity": "9.8"}],
                [member],
            )
        )
        self.assertEqual(0, code)
        self.assertIn("REPORT medium", output)
        self.assertIn("fixed version(s) listed: 2", output)

    def test_ubuntu_high_fails(self) -> None:
        code, output = self.evaluate_document(
            report(
                [{"ids": ["UBUNTU-CVE-1"], "aliases": []}],
                [vulnerability("UBUNTU-CVE-1", "high")],
            )
        )
        self.assertEqual(1, code)
        self.assertIn("FAIL high", output)

    def test_missing_or_unknown_priority_fails(self) -> None:
        for priority in (None, "unknown"):
            with self.subTest(priority=priority):
                code, output = self.evaluate_document(
                    report(
                        [{"ids": ["UBUNTU-CVE-1"], "aliases": []}],
                        [vulnerability("UBUNTU-CVE-1", priority)],
                    )
                )
                self.assertEqual(1, code)
                self.assertIn("FAIL unknown", output)

    def test_usn_uses_worst_member_priority(self) -> None:
        code, output = self.evaluate_document(
            report(
                [
                    {
                        "ids": ["USN-1", "UBUNTU-CVE-1", "UBUNTU-CVE-2"],
                        "aliases": [],
                    }
                ],
                [
                    vulnerability(
                        "USN-1",
                        None,
                        related=["UBUNTU-CVE-1", "UBUNTU-CVE-2"],
                    ),
                    vulnerability("UBUNTU-CVE-1", "low"),
                    vulnerability("UBUNTU-CVE-2", "high"),
                ],
            )
        )
        self.assertEqual(1, code)
        self.assertIn("FAIL high USN-1", output)

    def test_cross_group_relations_do_not_contaminate_priority(self) -> None:
        code, output = self.evaluate_document(
            report(
                [
                    {
                        "ids": ["USN-1", "UBUNTU-CVE-1"],
                        "aliases": [],
                    },
                    {
                        "ids": ["USN-2", "UBUNTU-CVE-2"],
                        "aliases": [],
                    },
                ],
                [
                    vulnerability(
                        "USN-1",
                        None,
                        related=["UBUNTU-CVE-1", "UBUNTU-CVE-2"],
                    ),
                    vulnerability("UBUNTU-CVE-1", "low"),
                    vulnerability(
                        "USN-2", None, related=["UBUNTU-CVE-2"]
                    ),
                    vulnerability("UBUNTU-CVE-2", "high"),
                ],
            )
        )
        self.assertEqual(1, code)
        self.assertIn("REPORT low USN-1", output)
        self.assertIn("FAIL high USN-2", output)
        self.assertNotIn("FAIL high USN-1", output)

    def test_non_ubuntu_fails(self) -> None:
        code, output = self.evaluate_document(
            report(
                [{"ids": ["GHSA-1"], "aliases": []}],
                [{"id": "GHSA-1", "affected": [], "severity": []}],
                ecosystem="Maven",
            )
        )
        self.assertEqual(1, code)
        self.assertIn("FAIL non-Ubuntu GHSA-1", output)

    def test_active_java_exception_is_the_only_non_ubuntu_allowance(self) -> None:
        document = report(
            [{"ids": ["GHSA-1"], "aliases": []}],
            [{"id": "GHSA-1", "affected": [], "severity": []}],
            ecosystem="Maven",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            output = io.StringIO()
            code = evaluate(path, 1, output, {"GHSA-1"})
        self.assertEqual(0, code)
        self.assertIn("EXCEPT Java-exception GHSA-1", output.getvalue())

        ubuntu_document = report(
            [{"ids": ["GHSA-1"], "aliases": []}],
            [{"id": "GHSA-1", "affected": [], "severity": []}],
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text(json.dumps(ubuntu_document), encoding="utf-8")
            output = io.StringIO()
            code = evaluate(path, 1, output, {"GHSA-1"})
        self.assertEqual(1, code)
        self.assertNotIn("EXCEPT", output.getvalue())

    def test_expired_java_exception_is_inactive(self) -> None:
        config = """
[[IgnoredVulns]]
id = "GHSA-active"
ignoreUntil = 2026-09-01
reason = "active fixture"

[[IgnoredVulns]]
id = "GHSA-expired"
ignoreUntil = 2026-08-30
reason = "expired fixture"

[[IgnoredVulns]]
id = "GHSA-today"
ignoreUntil = 2026-08-31
reason = "equal-date fixture"
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "osv-scanner.toml"
            path.write_text(config, encoding="utf-8")
            exceptions = load_java_exceptions(
                path, today=datetime.date(2026, 8, 31)
            )
        self.assertEqual({"GHSA-active"}, exceptions)
        document = report(
            [{"ids": ["GHSA-expired"], "aliases": []}],
            [{"id": "GHSA-expired", "affected": [], "severity": []}],
            ecosystem="Maven",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            output = io.StringIO()
            code = evaluate(path, 1, output, exceptions)
        self.assertEqual(1, code)
        self.assertIn("FAIL non-Ubuntu GHSA-expired", output.getvalue())

    def test_datetime_exception_expiry_fails_closed(self) -> None:
        config = """
[[IgnoredVulns]]
id = "GHSA-datetime"
ignoreUntil = 2026-09-01T00:00:00Z
reason = "datetime fixture"
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "osv-scanner.toml"
            path.write_text(config, encoding="utf-8")
            with self.assertRaisesRegex(SchemaError, "must be a TOML date"):
                load_java_exceptions(path, today=datetime.date(2026, 8, 31))

    def test_missing_ungrouped_and_duplicate_documents_fail_closed(self) -> None:
        missing_group_document = report(
            [{"ids": ["GHSA-1"], "aliases": []}],
            [],
            ecosystem="Maven",
        )
        code, output = self.evaluate_document(
            missing_group_document, java_exceptions={"GHSA-1"}
        )
        self.assertEqual(2, code)
        self.assertIn("missing vulnerability documents", output)

        extra_ungrouped_document = report(
            [{"ids": ["GHSA-1"], "aliases": []}],
            [
                {"id": "GHSA-1", "affected": [], "severity": []},
                {"id": "GHSA-2", "affected": [], "severity": []},
            ],
            ecosystem="Maven",
        )
        code, output = self.evaluate_document(extra_ungrouped_document)
        self.assertEqual(2, code)
        self.assertIn("ungrouped vulnerability documents", output)

        duplicate_grouping = report(
            [
                {"ids": ["GHSA-1"], "aliases": []},
                {"ids": ["GHSA-1"], "aliases": []},
            ],
            [{"id": "GHSA-1", "affected": [], "severity": []}],
            ecosystem="Maven",
        )
        code, output = self.evaluate_document(duplicate_grouping)
        self.assertEqual(2, code)
        self.assertIn("more than once", output)

    def test_malformed_and_scanner_failure_return_script_error(self) -> None:
        code, output = self.evaluate_document({"not_results": []})
        self.assertEqual(2, code)
        self.assertIn("could not safely interpret", output)
        code, output = self.evaluate_document({}, scanner_rc=7)
        self.assertEqual(2, code)
        self.assertIn("failed with exit code 7", output)

    def test_invalid_utf8_report_returns_script_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_bytes(b'{"results": ["\xff"]}')
            output = io.StringIO()
            code = evaluate(path, 1, output)
        self.assertEqual(2, code)
        self.assertIn("could not safely interpret", output.getvalue())

    def test_duplicate_occurrences_are_summarized(self) -> None:
        code, output = self.evaluate_document(
            report(
                [{"ids": ["UBUNTU-CVE-1"], "aliases": []}],
                [vulnerability("UBUNTU-CVE-1", "low")],
                duplicate=True,
            )
        )
        self.assertEqual(0, code)
        self.assertIn("1 advisory group(s), 2 package occurrence(s)", output)
        self.assertIn("low=1 group(s)/2 occurrence(s)", output)
        self.assertIn("medium=0 group(s)/0 occurrence(s)", output)
        self.assertEqual(1, output.count("REPORT low UBUNTU-CVE-1"))
        self.assertIn("2 occurrence(s)", output)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", nargs="?", type=Path)
    parser.add_argument("--scanner-exit-code", type=int)
    parser.add_argument("--java-exceptions", type=Path)
    parser.add_argument("--self-test", action="store_true")
    arguments = parser.parse_args()
    if arguments.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(PolicySelfTest)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if arguments.report is None or arguments.scanner_exit_code is None:
        parser.error("report and --scanner-exit-code are required unless --self-test is used")
    try:
        java_exceptions = (
            load_java_exceptions(arguments.java_exceptions)
            if arguments.java_exceptions is not None
            else set()
        )
    except SchemaError as error:
        print(f"OSV image policy could not safely interpret the config: {error}")
        return 2
    return evaluate(
        arguments.report,
        arguments.scanner_exit_code,
        sys.stdout,
        java_exceptions,
    )


if __name__ == "__main__":
    sys.exit(main())
