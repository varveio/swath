#!/usr/bin/env python3
"""Validate a dedicated corpus and sanitize native listing HTTP captures.

This tool never calls a provider or reads credentials. It consumes captures from an
independent harness and writes deterministic, redacted evidence plus a SHA-256 receipt.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree

REQUEST_HEADERS = {"accept", "x-ms-version", "x-ms-client-request-id", "content-type"}
RESPONSE_HEADERS = {"content-type", "x-ms-version", "x-ms-error-code"}
GCS_QUERY = {"prefix", "delimiter", "startOffset", "endOffset", "maxResults", "pageToken",
             "projection", "versions", "softDeleted", "includeFoldersAsPrefixes",
             "includeTrailingDelimiter", "matchGlob", "filter", "fields", "prettyPrint", "alt"}
AZURE_QUERY = {"restype", "comp", "prefix", "delimiter", "marker", "maxresults", "startFrom",
               "timeout", "include", "showonly", "endBefore"}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def canonical(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def headers(items: list[dict[str, str]], allowed: set[str]) -> list[dict[str, str]]:
    selected = []
    for item in items:
        name = item["name"].lower()
        if name in allowed:
            selected.append({"name": name, "value": item["value"]})
    return selected


def strict_json(raw: str) -> object:
    def unique(pairs: list[tuple[str, object]]) -> dict:
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON field {key!r}")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique)


def opaque_digest(token: str) -> str:
    return "token-sha256-" + hashlib.sha256(token.encode("utf-8")).hexdigest()


def sanitize_gcs_body(raw: str, bucket: str) -> str:
    body = strict_json(raw)
    if not isinstance(body, dict):
        raise ValueError("GCS capture body must be a JSON object")
    for item in body.get("items", []):
        if not isinstance(item, dict):
            raise ValueError("GCS items must be objects")
        if "bucket" in item:
            if item["bucket"] != bucket:
                raise ValueError("GCS item has unexpected bucket")
            item["bucket"] = "{bucket}"
        for native_only in ("id", "selfLink", "mediaLink", "owner", "metadata"):
            if native_only in item:
                item[native_only] = "{redacted-native-metadata}"
    if "nextPageToken" in body and body["nextPageToken"]:
        body["nextPageToken"] = opaque_digest(body["nextPageToken"])
    if "error" in body and isinstance(body["error"], dict):
        body["error"].pop("message", None)
        for error in body["error"].get("errors", []):
            error.pop("message", None)
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"))


def sanitize_azure_body(raw: str, account: str, container: str) -> str:
    parsed = ElementTree.fromstring(raw)
    if parsed.tag not in {"EnumerationResults", "Error"}:
        raise ValueError("unexpected Azure XML root")
    if parsed.tag == "Error":
        return raw
    if parsed.attrib.get("ContainerName") != container:
        raise ValueError("Azure response has unexpected container")
    if account not in parsed.attrib.get("ServiceEndpoint", ""):
        raise ValueError("Azure response has unexpected service endpoint")
    # Replace only the root's namespace attributes and opaque markers. Keep every Name
    # element and the rest of the wire bytes, including BOM/prolog and XML element order.
    head_end = raw.find(">", raw.find("<EnumerationResults"))
    if head_end < 0:
        raise ValueError("Azure EnumerationResults opening tag missing")
    head = raw[:head_end + 1]
    tail = raw[head_end + 1:]
    head, endpoint_count = re.subn(r'(ServiceEndpoint\s*=\s*")[^"]*(")',
                                   r'\g<1>https://{provider-origin}/\g<2>', head, count=1)
    head, container_count = re.subn(r'(ContainerName\s*=\s*")[^"]*(")',
                                    r'\g<1>{container}\g<2>', head, count=1)
    if endpoint_count != 1 or container_count != 1:
        raise ValueError("Azure namespace attributes missing")
    for tag in ("Marker", "NextMarker"):
        tail = re.sub(rf'(<{tag}>)([^<]+)(</{tag}>)',
                      lambda match: match.group(1) + opaque_digest(html.unescape(match.group(2))) + match.group(3),
                      tail)
    return head + tail


def sanitize_exchange(exchange: dict, provider: str, names: dict[str, str],
                      manifest_sha256: str, probe_id: str) -> dict:
    request = exchange["request"]
    response = exchange["response"]
    url = urlsplit(request["url"])
    if url.scheme not in {"http", "https"} or not url.hostname:
        raise ValueError("capture request needs an absolute HTTP URL")
    allowed = GCS_QUERY if provider == "gcs" else AZURE_QUERY
    for part in url.query.split("&") if url.query else []:
        key = part.partition("=")[0]
        if key not in allowed:
            raise ValueError(f"unrecognized query field {key!r}; refusing to retain potential secret")
    if not SHA256.fullmatch(manifest_sha256) or not probe_id:
        raise ValueError("capture needs manifest digest and probe ID")
    if provider == "gcs" and "bucket" not in names:
        raise ValueError("GCS capture needs bucket replacement")
    if provider == "azure" and ("account" not in names or "container" not in names):
        raise ValueError("Azure capture needs account and container replacements")

    body = base64.b64decode(response["body_base64"], validate=True)
    body_text = body.decode("utf-8", errors="strict")
    safe_body_text = sanitize_gcs_body(body_text, names["bucket"]) if provider == "gcs" \
        else sanitize_azure_body(body_text, names["account"], names["container"])
    safe_body = safe_body_text.encode("utf-8")
    segments = url.path.split("/")
    if provider == "gcs":
        if len(segments) != 6 or segments[:4] != ["", "storage", "v1", "b"] \
                or segments[4] != names["bucket"] or segments[5] != "o":
            raise ValueError("unexpected GCS listing path")
        segments[4] = "{bucket}"
    else:
        if len(segments) != 3 or segments[1] != names["account"] or segments[2] != names["container"]:
            raise ValueError("unexpected Azure listing path")
        segments[1:3] = ["{account}", "{container}"]
    path = "/".join(segments)
    query_parts = []
    for part in url.query.split("&") if url.query else []:
        key, sep, value = part.partition("=")
        query_parts.append(key + sep + (opaque_digest(unquote(value))
                                        if key in {"pageToken", "marker"} and value else value))
    query = "&".join(query_parts)
    request_headers = headers(request.get("headers", []), REQUEST_HEADERS)
    for header in request_headers:
        if header["name"] == "x-ms-client-request-id":
            header["value"] = "{client-request-id}"
    response_headers = headers(response.get("headers", []), RESPONSE_HEADERS)
    safe = {
        "schema_version": "provider-capture-v1",
        "provider": provider,
        "probe_id": probe_id,
        "manifest_sha256": manifest_sha256,
        "captured_at": exchange["captured_at"],
        "raw_body_sha256": hashlib.sha256(body).hexdigest(),
        "request": {"method": request["method"], "path": path, "query": query,
                    "headers": request_headers},
        "response": {"status": response["status"], "headers": response_headers,
                     "body_base64": base64.b64encode(safe_body).decode("ascii")},
    }
    datetime.fromisoformat(exchange["captured_at"].replace("Z", "+00:00"))
    visible = "\n".join([path, unquote(query), safe_body_text,
                         *[header["value"] for header in request_headers + response_headers]])
    if provider == "azure":
        xml = ElementTree.fromstring(safe_body)
        visible += "\n" + "\n".join(xml.itertext()) + "\n" + "\n".join(xml.attrib.values())
    for raw in names.values():
        if raw and raw in visible:
            raise ValueError("private identifier remained in decoded capture; review object names")
    for raw in (url.netloc, url.hostname):
        if raw and raw in visible:
            raise ValueError("private origin remained in decoded capture")
    return safe


def validate_manifest(manifest: dict) -> None:
    required = {"provider", "namespace_mode", "region", "api_version", "sdk_version", "capture_date", "objects"}
    if not required.issubset(manifest):
        raise ValueError(f"manifest missing {sorted(required - manifest.keys())}")
    if manifest["provider"] not in {"gcs", "azure"}:
        raise ValueError("manifest provider must be gcs or azure")
    if manifest["provider"] == "gcs" and manifest["namespace_mode"] != "flat-ubla":
        raise ValueError("GCS corpus must record flat namespace and UBLA")
    if manifest["provider"] == "azure" and manifest["namespace_mode"] != "flat-hns-off":
        raise ValueError("Azure corpus must record flat namespace with HNS off")
    if manifest["provider"] == "azure" and manifest["api_version"] not in {"2026-06-06", "2026-10-06"}:
        raise ValueError("Azure manifest needs an explicit supported service version")
    for key in ("region", "api_version", "sdk_version"):
        if not isinstance(manifest[key], str) or not manifest[key].strip():
            raise ValueError(f"manifest {key} must be nonempty")
    try:
        datetime.fromisoformat(manifest["capture_date"])
    except (TypeError, ValueError) as error:
        raise ValueError("manifest capture_date must be ISO date") from error
    if not manifest["objects"]:
        raise ValueError("corpus must contain at least one object")
    names = set()
    for item in manifest["objects"]:
        if not item["name"] or item["name"] in names:
            raise ValueError("manifest has empty or duplicate object name")
        names.add(item["name"])
        if type(item["size"]) is not int or item["size"] < 0 or not SHA256.fullmatch(item["sha256"]):
            raise ValueError("manifest object needs nonnegative size and SHA-256")


def validate_ledger(ledger: dict, manifests: dict[str, dict] | None = None, captures: Path | None = None,
                    repo_root: Path | None = None) -> None:
    manifests = manifests or {}
    seen = set()
    for row in ledger["rows"]:
        if not row.get("id") or row["id"] in seen:
            raise ValueError("ledger needs unique row IDs")
        seen.add(row["id"])
        if row.get("provider") not in {"gcs", "azure"}:
            raise ValueError("ledger row provider invalid")
        status = row["status"]
        if status not in {"UNMEASURED", "OBSERVED", "REPLAY_MATCH"}:
            raise ValueError("invalid evidence status")
        if status == "UNMEASURED":
            if row.get("capture_sha256") or row.get("replay_result_file"):
                raise ValueError("unmeasured row cannot carry promotion evidence")
            continue
        manifest = manifests.get(row["provider"])
        if manifest is None:
            raise ValueError("promoted row requires its provider manifest")
        manifest_digest = hashlib.sha256(canonical(manifest)).hexdigest()
        if not row.get("probe_request") or not row.get("expected_result") \
                or not SHA256.fullmatch(row.get("capture_sha256", "")):
            raise ValueError("observed row lacks request, expected result or capture checksum")
        if captures is None:
            raise ValueError("promoted row requires a capture directory")
        capture_path = checked_child(captures, row.get("capture_file", ""))
        payload = capture_path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != row["capture_sha256"]:
            raise ValueError("capture SHA-256 differs from ledger")
        capture = strict_json(payload.decode("utf-8"))
        if capture["provider"] != row["provider"] or capture["probe_id"] != row["id"] \
                or capture["manifest_sha256"] != manifest_digest:
            raise ValueError("capture provenance differs from ledger/manifest")
        request = capture["request"]
        exact_request = request["method"] + " " + request["path"] \
            + ("?" + request["query"] if request["query"] else "")
        if row["probe_request"] != exact_request:
            raise ValueError("ledger probe request differs from capture")
        if status == "REPLAY_MATCH":
            if repo_root is None or not row.get("replay_test") or not row.get("replay_test_path") \
                    or not SHA256.fullmatch(row.get("replay_result_sha256", "")):
                raise ValueError("replay match lacks verifiable test/result")
            test_path = checked_child(repo_root, row["replay_test_path"])
            if test_path.suffix not in {".java", ".py"} or not re.search(
                    rf'\b{re.escape(row["replay_test"])}\s*\(', test_path.read_text(encoding="utf-8")):
                raise ValueError("replay test identifier does not resolve to source")
            result_path = checked_child(captures, row.get("replay_result_file", ""))
            result_bytes = result_path.read_bytes()
            if hashlib.sha256(result_bytes).hexdigest() != row["replay_result_sha256"]:
                raise ValueError("replay result SHA-256 differs from ledger")
            result = strict_json(result_bytes.decode("utf-8"))
            if result.get("status") != "PASS" or result.get("capture_sha256") != row["capture_sha256"] \
                    or result.get("test") != row["replay_test"]:
                raise ValueError("replay result receipt does not prove matched capture/test")


def checked_child(root: Path, relative: str) -> Path:
    if not relative or Path(relative).is_absolute():
        raise ValueError("evidence path must be relative")
    resolved_root = root.resolve()
    path = (resolved_root / relative).resolve()
    if not path.is_relative_to(resolved_root) or not path.is_file():
        raise ValueError("evidence path is absent or escapes its root")
    return path


def validate_matrix(matrix: str, ledger: dict) -> None:
    rows = {row["id"]: row for row in ledger["rows"]}
    seen = set()
    for line in matrix.splitlines():
        if not line.startswith("| gcs-") and not line.startswith("| azure-"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 7:
            raise ValueError("matrix row has wrong column count")
        row_id, provider_display, feature, promise, checksum, replay_test, status = cells
        if row_id not in rows or row_id in seen:
            raise ValueError("matrix row missing from ledger or repeated")
        seen.add(row_id)
        row = rows[row_id]
        provider = "gcs" if provider_display.startswith("GCS") else "azure"
        if row["provider"] != provider or row["feature"] != feature \
                or row["profile_promise"] != promise or row["status"] != status:
            raise ValueError("matrix differs from evidence ledger")
        if status == "UNMEASURED" and (checksum != "—" or replay_test != "—"):
            raise ValueError("unmeasured matrix row claims capture or replay test")
        if status != "UNMEASURED" and checksum != row["capture_sha256"]:
            raise ValueError("matrix capture checksum differs from ledger")
        if status == "REPLAY_MATCH" and replay_test.strip("`") != row["replay_test"]:
            raise ValueError("matrix replay test differs from ledger")
    if seen != rows.keys():
        raise ValueError("matrix and ledger have different row sets")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sanitize = sub.add_parser("sanitize")
    sanitize.add_argument("--input", type=Path, required=True)
    sanitize.add_argument("--output", type=Path, required=True)
    sanitize.add_argument("--provider", choices=["gcs", "azure"], required=True)
    sanitize.add_argument("--bucket")
    sanitize.add_argument("--account")
    sanitize.add_argument("--container")
    sanitize.add_argument("--manifest", type=Path, required=True)
    sanitize.add_argument("--probe-id", required=True)
    check = sub.add_parser("validate")
    check.add_argument("--manifest", type=Path, action="append", default=[])
    check.add_argument("--ledger", type=Path, required=True)
    check.add_argument("--matrix", type=Path, required=True)
    check.add_argument("--captures", type=Path)
    check.add_argument("--repo-root", type=Path)
    args = parser.parse_args()
    if args.command == "validate":
        manifests = {}
        for manifest_path in args.manifest:
            manifest = strict_json(manifest_path.read_text(encoding="utf-8"))
            validate_manifest(manifest)
            manifests[manifest["provider"]] = manifest
        ledger = strict_json(args.ledger.read_text(encoding="utf-8"))
        validate_ledger(ledger, manifests, args.captures, args.repo_root)
        validate_matrix(args.matrix.read_text(encoding="utf-8"), ledger)
        print("provider evidence manifest and ledger valid")
        return
    raw = json.loads(args.input.read_text(encoding="utf-8"))
    manifest = strict_json(args.manifest.read_text(encoding="utf-8"))
    validate_manifest(manifest)
    if manifest["provider"] != args.provider:
        raise ValueError("capture provider differs from manifest")
    replacements = {name: value for name in ("bucket", "account", "container")
                    if (value := getattr(args, name)) is not None}
    safe = sanitize_exchange(raw, args.provider, replacements,
                             hashlib.sha256(canonical(manifest)).hexdigest(), args.probe_id)
    payload = canonical(safe)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(payload)
    print(hashlib.sha256(payload).hexdigest())


if __name__ == "__main__":
    main()
