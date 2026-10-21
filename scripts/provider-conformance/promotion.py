#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Keep the evidence ledger in step with the live case matrix and promote proven rows.

The case matrix in each provider adapter owns row IDs and behavior text. `rows` rewrites
the ledger rows and the matrix table from it, keeping any existing evidence. `promote`
attaches sanitized captures (OBSERVED) and replay receipts (REPLAY_MATCH) from an
evidence directory, then runs the fail-closed validator; nothing is promoted on failure.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import json
import re
from pathlib import Path
from xml.etree import ElementTree

import azure_live
import evidence
import gcs_live
import s3_live
from campaign import safe_file_id

PLACEHOLDER_RUN = "conformance/r00000000t000000z-00000000/"
# Promises carried from the bundled rows each case family was split from.
FAMILY_PROMISES = {
    "gcs-01": "Provisional bounded list", "gcs-02": "Provisional bounded list",
    "gcs-03": "Provisional; fast path gated", "gcs-04": "Provisional unsigned UTF-8 order",
    "gcs-05": "Strict native parser; duplicate policy gated", "gcs-06": "Zero refused pending evidence",
    "gcs-07": "Complete native-token walk required", "gcs-08": "Strict UTF-8 subset",
    "gcs-09": "Must run unmodified with endpoint override", "gcs-10": "Separate request counts",
    "gcs-11": "Named defaults and transport forms only; other options refused",
    "azure-01": "Explicit version only", "azure-02": "Version individually gated",
    "azure-03": "Provisional flat list", "azure-04": "**Unsupported until native evidence**",
    "azure-05": "Matching `startFrom` retained on continuation; other combinations refused pending evidence",
    "azure-06": "Provisional unsigned UTF-8 order",
    "azure-07": "Explicit encoding; raw-wire assertion required",
    "azure-08": "Zero refused pending evidence", "azure-09": "Complete native-marker walk required",
    "azure-10": "Must run unmodified with endpoint override", "azure-11": "Separate request counts",
    "azure-12": "Provisional 1,024 UTF-16 units and 254 segments; native creation probes required",
    "azure-13": "Echo explicit client request ID; XML accept only",
    "azure-14": "Empty or omitted options only; expansions refused",
    "s3-01": "Exclusive `start-after`, intersected with prefix", "s3-02": "Byte prefix match",
    "s3-03": "CommonPrefixes rollup; start-after suppression per existing S3 goldens",
    "s3-04": "Unsigned UTF-8 order", "s3-05": "Lenient S3 query decoding preserved from existing replay",
    "s3-06": "max-keys 0 to 1,000 with cap; malformed values refused",
    "s3-07": "Complete native-token walk; replay-owned continuation tokens",
    "s3-08": "encoding-type=url encodes Key, Prefix, Delimiter, StartAfter and CommonPrefixes",
    "s3-09": "Must run unmodified with endpoint override", "s3-10": "Separate request counts",
    "s3-11": "Synthetic owner when requested; optional attributes refused",
    "s3-12": "ListObjectsV2 only; V1 recorded for discrimination",
}
# Rows without a live case: their native behavior cannot be produced by this campaign.
UNPROBED_ROWS = [
    {"id": "gcs-10", "provider": "gcs", "feature": "Retry-disabled one-attempt errors; normal SDK retries",
     "profile_promise": FAMILY_PROMISES["gcs-10"], "api_versions": ["json-v1"], "evidence_kind": "retry_run"},
    {"id": "azure-10d", "provider": "azure", "feature": "Java SDK auto-pagination at 2026-10-06",
     "profile_promise": "Requires a pinned SDK that names the version", "api_versions": ["2026-10-06"],
     "evidence_kind": "sdk_run"},
    {"id": "azure-11", "provider": "azure", "feature": "Retry-disabled one-attempt errors; normal SDK retries",
     "profile_promise": FAMILY_PROMISES["azure-11"], "api_versions": list(azure_live.VERSIONS),
     "evidence_kind": "retry_run"},
    {"id": "s3-10", "provider": "s3", "feature": "Retry-disabled one-attempt errors; normal SDK retries",
     "profile_promise": FAMILY_PROMISES["s3-10"], "api_versions": [s3_live.API_VERSION],
     "evidence_kind": "retry_run"},
]
TABLE_HEADER = "| ID | Provider | Feature or probe | Profile promise pending evidence | Capture SHA-256 | Replay test | State |"
REPLAY_TEST_PATH = ("swath-replay/src/test/java/io/varve/swath/replay/conformance/provider/"
                    "NativeEvidenceReplayTest.java")


def placeholder_providers():
    return [gcs_live.GcsProvider("example-bucket-x9", "example-project-x9", "us-east1", None),
            azure_live.AzureProvider("exampleaccountx9", "conformance", "sv=1&sig=x9x9", "eastus"),
            s3_live.S3Provider("example-bucket-x9", "us-east-1",
                               {"AWS_ACCESS_KEY_ID": "AKIDEXAMPLEX9", "AWS_SECRET_ACCESS_KEY": "x9"})]


def family(row_id: str) -> str:
    return re.sub(r"[a-z]+$", "", row_id)


def generated_rows(ledger: dict) -> list[dict]:
    existing = {row["id"]: row for row in ledger["rows"]}
    rows = []
    for provider in placeholder_providers():
        for case in provider.cases(PLACEHOLDER_RUN, "full"):
            versions = list(case.versions) or list(provider.api_versions)
            row = {"id": case.id, "provider": provider.name, "feature": case.title,
                   "profile_promise": FAMILY_PROMISES[family(case.id)], "status": "UNMEASURED",
                   "api_versions": versions,
                   "evidence_kind": "exchange" if case.kind == "token" else case.kind}
            previous = existing.get(case.id)
            if previous and previous["status"] != "UNMEASURED":
                row.update({key: previous[key] for key in ("status", "evidence", "evidence_kind")})
            rows.append(row)
    for template in UNPROBED_ROWS:
        rows.append(existing.get(template["id"], {**template, "status": "UNMEASURED"}))
    order = {"gcs": 0, "azure": 1, "s3": 2}
    return sorted(rows, key=lambda row: (order[row["provider"]], natural(row["id"])))


def natural(row_id: str) -> tuple:
    return tuple(int(part) if part.isdigit() else part for part in re.split(r"(\d+)", row_id))


def provider_display(row: dict) -> str:
    if row["provider"] == "gcs":
        return "GCS JSON v1"
    if row["provider"] == "s3":
        return "Amazon S3 ListObjectsV2"
    if len(row["api_versions"]) == 1:
        return f"Azure Blob XML `{row['api_versions'][0]}`"
    return "Azure Blob XML"


def matrix_lines(rows: list[dict]) -> list[str]:
    lines = [TABLE_HEADER, "| --- | --- | --- | --- | --- | --- | --- |"]
    for row in rows:
        checksum = test = "—"
        if row["status"] != "UNMEASURED":
            checksum = "; ".join(f"{v}:{row['evidence'][v]['capture_sha256']}" for v in row["api_versions"])
        if row["status"] == "REPLAY_MATCH":
            test = "`" + "; ".join(f"{v}:{row['evidence'][v]['replay_test']}" for v in row["api_versions"]) + "`"
        lines.append(f"| {row['id']} | {provider_display(row)} | {row['feature']} | "
                     f"{row['profile_promise']} | {checksum} | {test} | {row['status']} |")
    return lines


def rewrite_matrix(text: str, rows: list[dict]) -> str:
    lines = text.splitlines()
    start = lines.index(TABLE_HEADER)
    end = start
    while end < len(lines) and lines[end].startswith("|"):
        end += 1
    return "\n".join(lines[:start] + matrix_lines(rows) + lines[end:]) + "\n"


# Observation summaries ------------------------------------------------------------


def relative(name: str, run_prefix: str) -> str:
    return name[len(run_prefix):] if name.startswith(run_prefix) else name


def summarize_exchange(provider: str, exchange: dict, run_prefix: str) -> dict:
    status = exchange["response"]["status"]
    body = base64.b64decode(exchange["response"]["body_base64"])
    if status != 200:
        if provider == "gcs":
            reason = evidence.strict_json(body.decode())["error"]["errors"][0].get("reason")
        elif provider == "s3":
            match = re.search(r"<Code>([^<]*)</Code>", body.decode("utf-8", "replace"))
            reason = match.group(1) if match else "no error body"
        else:
            reason = ElementTree.fromstring(body).findtext("Code") if body else \
                next(h["value"] for h in exchange["response"]["headers"] if h["name"] == "x-ms-error-code")
        return {"status": status, "error": reason}
    if provider == "gcs":
        page = evidence.strict_json(body.decode())
        entries = [("item", i["name"]) for i in page.get("items", [])] + \
                  [("prefix", p) for p in page.get("prefixes", [])]
        token = bool(page.get("nextPageToken"))
    elif provider == "s3":
        # Regular expressions, not a parser: S3 can return keys XML 1.0 cannot carry.
        text = body.decode("utf-8", "replace")
        entries = [("item", html.unescape(k)) for k in re.findall(r"<Contents>.*?<Key>(.*?)</Key>", text, re.S)]
        entries += [("prefix", html.unescape(k)) for k in
                    re.findall(r"<CommonPrefixes>\s*<Prefix>(.*?)</Prefix>", text, re.S)]
        token = "<NextContinuationToken>" in text or "<NextMarker>" in text
    else:
        root = ElementTree.fromstring(body)
        entries = []
        for node in root.find("Blobs") if root.find("Blobs") is not None else []:
            name = node.find("Name")
            label = ("prefix" if node.tag == "BlobPrefix" else "item") + \
                ("-encoded" if name.attrib.get("Encoded") == "true" else "")
            entries.append((label, name.text or ""))
        token = bool(root.findtext("NextMarker"))
    return {"status": status, "entries": [(kind, relative(name, run_prefix)) for kind, name in entries],
            "continues": token}


def expected_result(provider: str, capture: dict, run_prefix: str) -> str:
    exchanges = [capture] if capture["schema_version"] == "provider-capture-v1" else \
        [step["exchange"] for step in capture["steps"]]
    pages = [summarize_exchange(provider, exchange, run_prefix) for exchange in exchanges]
    if len(pages) == 1 and "error" in pages[0]:
        return f"HTTP {pages[0]['status']} {pages[0]['error']}"
    parts = []
    for index, page in enumerate(pages):
        entries = page["entries"]
        shown = entries if len(entries) <= 24 else entries[:3] + [("…", f"{len(entries) - 6} more")] + entries[-3:]
        listed = ", ".join(f"{kind}:{name!a}" for kind, name in shown)
        parts.append(f"page {index + 1}: HTTP {page['status']}, {len(entries)} entries [{listed}]"
                     + (", continues" if page["continues"] else ", terminal"))
    return "; ".join(parts)


# Promotion ------------------------------------------------------------------------


def promote(ledger: dict, evidence_root: Path, receipts: dict[str, dict]) -> dict:
    indexes = {}
    for provider in ("gcs", "azure", "s3"):
        path = evidence_root / provider / "index.json"
        if path.exists():
            indexes[provider] = evidence.strict_json(path.read_text(encoding="utf-8"))
    for row in ledger["rows"]:
        index = indexes.get(row["provider"])
        if index is None or row["id"] in {u["id"] for u in UNPROBED_ROWS}:
            continue
        proofs = {}
        kinds = set()
        for version in row["api_versions"]:
            key = safe_file_id(row["id"], version if row["provider"] == "azure" else None)
            entry = index["captures"].get(key)
            if entry is None:
                break
            capture_path = evidence_root / row["provider"] / entry["file"]
            payload = evidence.read_evidence(capture_path)
            if hashlib.sha256(payload).hexdigest() != entry["sha256"]:
                raise ValueError(f"{key} capture differs from its index checksum")
            capture = evidence.strict_json(payload.decode("utf-8"))
            first = capture if capture["schema_version"] == "provider-capture-v1" else capture["steps"][0]["exchange"]
            request = first["request"]
            proof = {"capture_file": f"{row['provider']}/{entry['file']}", "capture_sha256": entry["sha256"],
                     "probe_request": request["method"] + " " + request["path"]
                     + ("?" + request["query"] if request["query"] else ""),
                     "expected_result": expected_result(row["provider"], capture, index["run_prefix"])}
            if "raw_wire" in entry:
                proof["raw_wire_file"] = f"{row['provider']}/{entry['raw_wire']['file']}"
                proof["raw_wire_sha256"] = entry["raw_wire"]["sha256"]
            receipt = receipts.get(f"{row['provider']}/{key}")
            if receipt is not None:
                proof.update({"replay_test": receipt["test"], "replay_test_path": receipt["test_path"],
                              "replay_result_file": receipt["file"],
                              "replay_result_sha256": receipt["sha256"]})
            proofs[version] = proof
            kinds.add(entry["kind"])
        if len(proofs) != len(row["api_versions"]) or len(kinds) != 1:
            continue
        row["evidence"] = proofs
        row["evidence_kind"] = kinds.pop()
        row["status"] = "REPLAY_MATCH" if all("replay_test" in p for p in proofs.values()) else "OBSERVED"
        if row["status"] == "OBSERVED":
            for proof in proofs.values():
                for key in ("replay_test", "replay_test_path", "replay_result_file", "replay_result_sha256"):
                    proof.pop(key, None)
    return ledger


def load_receipts(evidence_root: Path) -> dict[str, dict]:
    receipts = {}
    for path in sorted(evidence_root.glob("*/receipts/*.json")):
        payload = path.read_bytes()
        receipt = evidence.strict_json(payload.decode("utf-8"))
        if receipt.get("status") != "PASS":
            continue
        provider = path.parent.parent.name
        receipts[f"{provider}/{path.stem}"] = {**receipt, "file": f"{provider}/receipts/{path.name}",
                                               "sha256": hashlib.sha256(payload).hexdigest()}
    return receipts


def write_ledger(path: Path, ledger: dict) -> None:
    path.write_text(json.dumps(ledger, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["rows", "promote"])
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, help="evidence root with gcs/ and azure/ indexes")
    parser.add_argument("--repo-root", type=Path)
    parser.add_argument("--distribution", type=Path)
    args = parser.parse_args()
    ledger = evidence.strict_json(args.ledger.read_text(encoding="utf-8"))
    ledger["rows"] = generated_rows(ledger)
    manifests = {}
    if args.command == "promote":
        ledger = promote(ledger, args.evidence, load_receipts(args.evidence))
        for path in sorted(args.evidence.glob("*/manifest.*.json")):
            manifest = evidence.strict_json(path.read_text(encoding="utf-8"))
            evidence.validate_manifest(manifest)
            manifests[(manifest["provider"], manifest["api_version"])] = manifest
    commit = None
    if args.repo_root is not None:
        import subprocess
        commit = subprocess.run(["git", "-C", str(args.repo_root), "rev-parse", "HEAD"], check=True,
                                capture_output=True, text=True).stdout.strip()
    evidence.validate_ledger(ledger, manifests, args.evidence, args.repo_root, commit, args.distribution)
    matrix = rewrite_matrix(args.matrix.read_text(encoding="utf-8"), ledger["rows"])
    evidence.validate_matrix(matrix, ledger)
    write_ledger(args.ledger, ledger)
    args.matrix.write_text(matrix, encoding="utf-8")
    counts = {}
    for row in ledger["rows"]:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    print(json.dumps(counts, sort_keys=True))


if __name__ == "__main__":
    main()
