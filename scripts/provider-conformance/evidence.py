#!/usr/bin/env python3
"""Validate a dedicated corpus and sanitize native listing HTTP captures.

This tool never calls a provider or reads credentials. It consumes captures from an
independent harness, writes deterministic redacted evidence, and prints its SHA-256.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import json
import re
import subprocess
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree

REQUEST_HEADERS = {"accept", "x-ms-version", "x-ms-client-request-id", "content-type",
                   "user-agent", "x-goog-api-client"}
RESPONSE_HEADERS = {"content-type", "date", "x-goog-request-id", "x-ms-request-id",
                    "x-ms-version", "x-ms-error-code", "x-ms-client-request-id",
                    "content-length", "server", "cache-control", "vary", "transfer-encoding"}
GCS_QUERY = {"prefix", "delimiter", "startOffset", "endOffset", "maxResults", "pageToken",
             "projection", "versions", "softDeleted", "includeFoldersAsPrefixes",
             "includeTrailingDelimiter", "matchGlob", "filter", "fields", "prettyPrint", "alt"}
AZURE_QUERY = {"restype", "comp", "prefix", "delimiter", "marker", "maxresults", "startFrom",
               "timeout", "include", "showonly", "endBefore"}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SDK_TOKEN = re.compile(
    r"(?<![A-Za-z0-9._-])(?:gcloud-java(?:-storage)?|google-cloud-storage|google-api-java-client|"
    r"google-http-java-client|google-http-client|azsdk-java-azure-storage-blob|"
    r"azure-storage-blob|gdcl|gccl|gapic|gl-java)/[0-9][A-Za-z0-9._+\-]*",
    re.IGNORECASE)
EVIDENCE_KINDS = {"exchange", "walk", "sdk_run", "retry_run"}
REQUIRED_KINDS = {"gcs-07": "walk", "gcs-09": "sdk_run", "gcs-10": "retry_run",
                  "azure-09": "walk", "azure-10": "sdk_run", "azure-11": "retry_run"}
SPLIT_BEFORE_PROMOTION = {"gcs-01", "gcs-03", "gcs-06", "azure-04", "azure-08", "azure-12"}
TOKEN_FIELDS = {"gcs": "pageToken", "azure": "marker"}


def canonical(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def headers(items: list[dict[str, str]], allowed: set[str], reject_unknown: bool = False) -> list[dict[str, str]]:
    selected = []
    for item in items:
        name = item["name"].lower()
        if name in allowed:
            selected.append({"name": name, "value": item["value"]})
        elif reject_unknown:
            raise ValueError(f"unclassified response header {name!r}")
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


def decoded_token(raw: str) -> str:
    if re.search(r"%(?![0-9A-Fa-f]{2})", raw):
        raise ValueError("malformed percent escape in page token")
    # urllib's default replacement decoder loses evidence for malformed UTF-8.
    return unquote(raw, encoding="utf-8", errors="strict")


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
        if "message" in body["error"]:
            body["error"]["message"] = "{redacted-provider-message}"
        for error in body["error"].get("errors", []):
            if "message" in error:
                error["message"] = "{redacted-provider-message}"
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"))


def sanitize_azure_body(raw: str, account: str, container: str) -> str:
    parsed = ElementTree.fromstring(raw)
    if parsed.tag not in {"EnumerationResults", "Error"}:
        raise ValueError("unexpected Azure XML root")
    if parsed.tag == "Error":
        allowed = {"Code", "Message", "QueryParameterName", "QueryParameterValue",
                   "HeaderName", "HeaderValue", "Reason", "RequestId", "Time",
                   "AuthenticationErrorDetail"}
        redact = {"Message", "QueryParameterValue", "HeaderValue", "Reason",
                  "AuthenticationErrorDetail", "Time"}
        if parsed.attrib or not list(parsed) or (parsed.text or "").strip():
            raise ValueError("Azure Error root has unclassified content")
        seen = set()
        for child in parsed:
            if child.tag not in allowed or child.tag in seen or child.attrib or list(child) \
                    or (child.tail or "").strip():
                raise ValueError("unclassified Azure error detail")
            seen.add(child.tag)
            if child.tag in redact:
                child.text = "{redacted-provider-detail}"
            elif child.tag == "Code" and not re.fullmatch(r"[A-Za-z]+", child.text or ""):
                raise ValueError("Azure Error Code is not a classified service code")
            elif child.tag == "RequestId":
                child.text = "request-id-sha256-" + hashlib.sha256(
                    (child.text or "").encode("utf-8")).hexdigest()
            elif child.tag == "QueryParameterName" and child.text not in AZURE_QUERY:
                raise ValueError("unclassified Azure error query field")
            elif child.tag == "HeaderName" and (child.text or "").lower() not in REQUEST_HEADERS:
                raise ValueError("unclassified Azure error header field")
        if "Code" not in seen or "Message" not in seen:
            raise ValueError("Azure Error lacks Code or Message")
        root_start = raw.find("<Error")
        if root_start < 0:
            raise ValueError("Azure Error opening tag missing")
        preamble = raw[:root_start]
        if not re.fullmatch(r'\ufeff?(?:<\?xml[^<>]*\?>)?\s*', preamble):
            raise ValueError("Azure Error preamble contains unclassified content")
        return preamble + ElementTree.tostring(parsed, encoding="unicode")
    if parsed.attrib.get("ContainerName") != container:
        raise ValueError("Azure response has unexpected container")
    endpoint = parsed.attrib.get("ServiceEndpoint", "")
    endpoint_url = urlsplit(endpoint)
    if endpoint_url.scheme != "https" or endpoint_url.hostname != f"{account}.blob.core.windows.net" \
            or endpoint_url.path != "/" or endpoint_url.query or endpoint_url.fragment:
        raise ValueError("Azure response has unexpected service endpoint shape")
    # Replace only the root's namespace attributes and opaque markers. Keep every Name
    # element and the rest of the wire bytes, including BOM/prolog and XML element order.
    head_end = raw.find(">", raw.find("<EnumerationResults"))
    if head_end < 0:
        raise ValueError("Azure EnumerationResults opening tag missing")
    head = raw[:head_end + 1]
    tail = raw[head_end + 1:]
    head, endpoint_count = re.subn(r'(ServiceEndpoint\s*=\s*")[^"]*(")',
                                   r'\g<1>https://{account}.blob.core.windows.net/\g<2>', head, count=1)
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
                      manifest_sha256: str, probe_id: str, api_version: str | None = None) -> dict:
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
    if provider == "gcs" and api_version is None:
        api_version = "json-v1"
    if provider == "azure" and ("account" not in names or "container" not in names):
        raise ValueError("Azure capture needs account and container replacements")
    if provider == "azure":
        if url.scheme != "https" or url.hostname != f"{names['account']}.blob.core.windows.net":
            raise ValueError("Azure native capture needs account host over HTTPS")
        version_headers = [item["value"] for item in request.get("headers", [])
                           if item["name"].lower() == "x-ms-version"]
        echoed_versions = [item["value"] for item in response.get("headers", [])
                           if item["name"].lower() == "x-ms-version"]
        if len(version_headers) != 1 or len(echoed_versions) != 1 \
                or version_headers[0] != api_version or echoed_versions[0] != api_version:
            raise ValueError("Azure capture version differs from manifest/request/response")

    body = base64.b64decode(response["body_base64"], validate=True)
    method = request["method"]
    azure_head_error = provider == "azure" and method == "HEAD" \
        and 400 <= response["status"] <= 599
    if provider == "azure" and method == "HEAD" and not azure_head_error:
        raise ValueError("Azure HEAD success is outside the captured error profile")
    if azure_head_error and body:
        raise ValueError("Azure HEAD error capture must have an empty body")
    body_text = body.decode("utf-8", errors="strict")
    safe_body_text = "" if azure_head_error else (sanitize_gcs_body(body_text, names["bucket"])
        if provider == "gcs" else sanitize_azure_body(body_text, names["account"], names["container"]))
    safe_body = safe_body_text.encode("utf-8")
    segments = url.path.split("/")
    if provider == "gcs":
        if len(segments) != 6 or segments[:4] != ["", "storage", "v1", "b"] \
                or segments[4] != names["bucket"] or segments[5] != "o":
            raise ValueError("unexpected GCS listing path")
        segments[4] = "{bucket}"
    else:
        if len(segments) != 2 or segments[1] != names["container"]:
            raise ValueError("unexpected Azure listing path")
        segments[1] = "{container}"
    path = "/".join(segments)
    query_parts = []
    token_wire_sha256 = []
    for part in url.query.split("&") if url.query else []:
        key, sep, value = part.partition("=")
        if key in {"pageToken", "marker"} and value:
            token_wire_sha256.append(hashlib.sha256(value.encode("utf-8")).hexdigest())
        query_parts.append(key + sep + (opaque_digest(decoded_token(value))
                                        if key in {"pageToken", "marker"} and value else value))
    query = "&".join(query_parts)
    request_headers = headers(request.get("headers", []), REQUEST_HEADERS)
    request_client_ids = [header["value"] for header in request_headers
                          if header["name"] == "x-ms-client-request-id"]
    response_client_ids = [item["value"] for item in response.get("headers", [])
                           if item["name"].lower() == "x-ms-client-request-id"]
    if provider == "azure" and request_client_ids and response_client_ids != request_client_ids:
        raise ValueError("Azure client request ID echo differs")
    for header in request_headers:
        if header["name"] == "x-ms-client-request-id":
            header["value"] = opaque_digest(header["value"])
        elif header["name"] in {"user-agent", "x-goog-api-client"}:
            tokens = SDK_TOKEN.findall(header["value"])
            if not tokens:
                raise ValueError("SDK header has no classified product/version token")
            header["value"] = " ".join(tokens)
    response_headers = headers(response.get("headers", []), RESPONSE_HEADERS, reject_unknown=True)
    for header in response_headers:
        if header["name"] in {"x-goog-request-id", "x-ms-request-id"}:
            header["value"] = "request-id-sha256-" + hashlib.sha256(header["value"].encode("utf-8")).hexdigest()
        elif header["name"] == "x-ms-client-request-id":
            header["value"] = opaque_digest(header["value"])
        elif header["name"] == "content-length" and not azure_head_error:
            header["value"] = str(len(safe_body))
        elif header["name"] == "content-length" and not re.fullmatch(r"[0-9]+", header["value"]):
            raise ValueError("Azure HEAD Content-Length is malformed")
    safe = {
        "schema_version": "provider-capture-v1",
        "provider": provider,
        "api_version": api_version,
        "probe_id": probe_id,
        "manifest_sha256": manifest_sha256,
        "captured_at": exchange["captured_at"],
        "raw_body_sha256": hashlib.sha256(body).hexdigest(),
        "request": {"method": request["method"], "path": path, "query": query,
                    "headers": request_headers, "token_wire_sha256": token_wire_sha256},
        "response": {"status": response["status"], "headers": response_headers,
                     "body_base64": base64.b64encode(safe_body).decode("ascii")},
    }
    datetime.fromisoformat(exchange["captured_at"].replace("Z", "+00:00"))
    visible_query = "&".join(part for part in query.split("&")
                             if not (provider == "azure" and part == "restype=container"))
    visible_parts = [unquote(visible_query),
                     *[header["value"] for header in request_headers + response_headers]]
    if provider == "azure" and not azure_head_error:
        xml = ElementTree.fromstring(safe_body)
        # Namespace fields were structurally replaced above. Inspect every other
        # body field without treating a placeholder such as {account} as a leak.
        xml.attrib.pop("ServiceEndpoint", None)
        xml.attrib.pop("ContainerName", None)
        visible_parts.append(ElementTree.tostring(xml, encoding="unicode"))
        for name in xml.findall(".//Name"):
            if name.attrib.get("Encoded", "").lower() == "true":
                visible_parts.append(unquote(name.text or "", errors="strict"))
    elif provider == "gcs":
        body_for_review = strict_json(safe_body_text)
        for item in body_for_review.get("items", []):
            item.pop("bucket", None)
        visible_parts.append(json.dumps(body_for_review, ensure_ascii=False))
    visible = "\n".join(visible_parts)
    for raw in names.values():
        if raw and raw in visible:
            raise ValueError("private identifier remained in decoded capture; review object names")
    for raw in (url.netloc, url.hostname):
        if raw and raw in visible:
            raise ValueError("private origin remained in decoded capture")
    return safe


def sanitize_run(raw: dict, provider: str, names: dict[str, str],
                 manifest_sha256: str, probe_id: str, api_version: str, kind: str) -> dict:
    if kind not in {"walk", "sdk_run", "retry_run"} or not isinstance(raw.get("steps"), list):
        raise ValueError("run capture needs a typed step list")
    steps = []
    for step in raw["steps"]:
        if not isinstance(step, dict) or "exchange" not in step:
            raise ValueError("run step needs an exchange")
        phase = step.get("phase", "walk")
        if phase not in ({"retry_disabled", "normal_retry"} if kind == "retry_run" else {"walk"}):
            raise ValueError("run step has an invalid phase")
        steps.append({"phase": phase, "exchange": sanitize_exchange(
            step["exchange"], provider, names, manifest_sha256, probe_id, api_version)})
    safe = {"schema_version": "provider-capture-run-v1", "evidence_kind": kind,
            "provider": provider, "api_version": api_version, "probe_id": probe_id,
            "manifest_sha256": manifest_sha256, "steps": steps}
    validate_run_shape(safe, kind)
    return safe


def query_parts(query: str) -> list[tuple[str, str]]:
    return [tuple(part.split("=", 1)) if "=" in part else (part, "")
            for part in query.split("&") if part]


def request_token(exchange: dict, provider: str) -> str | None:
    values = [value for key, value in query_parts(exchange["request"]["query"])
              if key == TOKEN_FIELDS[provider]]
    if len(values) > 1:
        raise ValueError("duplicate continuation token query field")
    return values[0] if values else None


def response_token(exchange: dict, provider: str) -> str | None:
    body = base64.b64decode(exchange["response"]["body_base64"], validate=True).decode("utf-8")
    if provider == "gcs":
        parsed = strict_json(body)
        token = parsed.get("nextPageToken")
    else:
        parsed = ElementTree.fromstring(body)
        token = parsed.findtext("NextMarker")
    if token is not None and (not isinstance(token, str) or not token.startswith("token-sha256-")):
        raise ValueError("run response token is not sanitized")
    return token or None


def validate_run_shape(capture: dict, kind: str) -> None:
    steps = capture.get("steps")
    if not isinstance(steps, list) or not all(isinstance(step, dict) for step in steps):
        raise ValueError("run capture steps malformed")
    provider = capture["provider"]
    if kind in {"walk", "sdk_run"}:
        if len(steps) < 2 or len(steps) > 1024 or any(step.get("phase") != "walk" for step in steps):
            raise ValueError("walk/SDK capture needs two or more bounded pages")
        first = steps[0]["exchange"]["request"]
        first_query = [(key, value) for key, value in query_parts(first["query"])
                       if key != TOKEN_FIELDS[provider]]
        for index, step in enumerate(steps):
            exchange = step["exchange"]
            request = exchange["request"]
            if request["method"] != first["method"] or request["path"] != first["path"] \
                    or [(key, value) for key, value in query_parts(request["query"])
                        if key != TOKEN_FIELDS[provider]] != first_query:
                raise ValueError("walk request scope changed between pages")
            expected = None if index == 0 else response_token(steps[index - 1]["exchange"], provider)
            if request_token(exchange, provider) != expected:
                raise ValueError("walk continuation token differs from preceding response")
            if exchange["response"]["status"] != 200:
                raise ValueError("walk/SDK page has non-success status")
        if response_token(steps[-1]["exchange"], provider) is not None:
            raise ValueError("walk capture ends before final page")
    else:
        phases = [step.get("phase") for step in steps]
        if phases.count("retry_disabled") != 1 or phases.count("normal_retry") < 2 \
                or len(steps) > 64 or phases != sorted(phases, key=lambda p: p != "retry_disabled"):
            raise ValueError("retry capture needs one disabled and at least two normal attempts")
        requests = [{key: step["exchange"]["request"][key] for key in ("method", "path", "query")}
                    for step in steps]
        if any(request != requests[0] for request in requests[1:]):
            raise ValueError("retry attempt request scopes differ")
        statuses = [step["exchange"]["response"]["status"] for step in steps]
        if statuses[0] < 500 or any(status < 500 for status in statuses[1:-1]):
            raise ValueError("retry capture lacks retryable failure before each retry")


def validate_manifest(manifest: dict) -> None:
    required = {"provider", "namespace_mode", "region", "api_version", "sdk_product",
                "sdk_version", "capture_date", "objects"}
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
    if manifest["provider"] == "gcs" and manifest["api_version"] != "json-v1":
        raise ValueError("GCS manifest needs json-v1 service API")
    for key in ("region", "api_version", "sdk_version"):
        if not isinstance(manifest[key], str) or not manifest[key].strip():
            raise ValueError(f"manifest {key} must be nonempty")
    products = {"gcs": {"gcloud-java"},
                "azure": {"azsdk-java-azure-storage-blob"}}
    if manifest["sdk_product"] not in products[manifest["provider"]]:
        raise ValueError("manifest SDK product is not the pinned provider client")
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


def validate_ledger(ledger: dict, manifests: dict[tuple[str, str], dict] | None = None,
                    captures: Path | None = None,
                    repo_root: Path | None = None, replay_commit: str | None = None,
                    distribution: Path | None = None) -> None:
    manifests = manifests or {}
    seen = set()
    for row in ledger["rows"]:
        if not row.get("id") or row["id"] in seen:
            raise ValueError("ledger needs unique row IDs")
        seen.add(row["id"])
        if row.get("provider") not in {"gcs", "azure"}:
            raise ValueError("ledger row provider invalid")
        if row.get("evidence_kind") not in EVIDENCE_KINDS:
            raise ValueError("ledger row needs explicit evidence kind")
        if row["id"] in REQUIRED_KINDS and row["evidence_kind"] != REQUIRED_KINDS[row["id"]]:
            raise ValueError("ledger row evidence kind differs from required probe kind")
        versions = row.get("api_versions")
        if not isinstance(versions, list) or not versions or len(versions) != len(set(versions)):
            raise ValueError("ledger row needs explicit distinct API versions")
        supported = {"json-v1"} if row["provider"] == "gcs" else {"2026-06-06", "2026-10-06"}
        if not set(versions).issubset(supported):
            raise ValueError("ledger row has unsupported API version")
        status = row["status"]
        if status not in {"UNMEASURED", "OBSERVED", "REPLAY_MATCH"}:
            raise ValueError("invalid evidence status")
        if status == "UNMEASURED":
            if row.get("evidence") or row.get("capture_file") or row.get("capture_sha256") \
                    or row.get("replay_test") or row.get("replay_result_file"):
                raise ValueError("unmeasured row cannot carry promotion evidence")
            continue
        if row["id"] in SPLIT_BEFORE_PROMOTION:
            raise ValueError("multi-case probe must split into case-specific rows before promotion")
        proofs = row.get("evidence")
        if not isinstance(proofs, dict) or set(proofs) != set(versions):
            raise ValueError("promoted row needs separate evidence for every API version")
        for version in versions:
            manifest = manifests.get((row["provider"], version))
            if manifest is None:
                raise ValueError("promoted row requires its provider/version manifest")
            validate_promotion(row, version, proofs[version], manifest, status, captures,
                               repo_root, replay_commit, distribution)


def validate_promotion(row: dict, version: str, proof: dict, manifest: dict,
                       status: str, captures: Path | None, repo_root: Path | None,
                       replay_commit: str | None, distribution: Path | None) -> None:
    if captures is None or not proof.get("probe_request") or not proof.get("expected_result") \
            or not SHA256.fullmatch(proof.get("capture_sha256", "")):
        raise ValueError("observed row lacks request, expected result, capture directory or checksum")
    manifest_digest = hashlib.sha256(canonical(manifest)).hexdigest()
    capture_path = checked_child(captures, proof.get("capture_file", ""))
    payload = capture_path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != proof["capture_sha256"]:
        raise ValueError("capture SHA-256 differs from ledger")
    capture = strict_json(payload.decode("utf-8"))
    if capture["provider"] != row["provider"] or capture["probe_id"] != row["id"] \
            or capture["manifest_sha256"] != manifest_digest or capture.get("api_version") != version:
        raise ValueError("capture provenance/version differs from ledger/manifest")
    kind = row["evidence_kind"]
    if kind == "exchange":
        if capture.get("schema_version") != "provider-capture-v1":
            raise ValueError("single-exchange row needs exchange capture")
        exchanges = [capture]
    else:
        if capture.get("schema_version") != "provider-capture-run-v1" \
                or capture.get("evidence_kind") != kind:
            raise ValueError("walk/SDK/retry row needs matching typed run capture")
        exchanges = [step["exchange"] for step in capture.get("steps", [])]
        validate_run_shape(capture, kind)
        for exchange in exchanges:
            if exchange["provider"] != row["provider"] or exchange["probe_id"] != row["id"] \
                    or exchange["manifest_sha256"] != manifest_digest \
                    or exchange["api_version"] != version \
                    or exchange["schema_version"] != "provider-capture-v1":
                raise ValueError("run step provenance/version differs from ledger")
    request = exchanges[0]["request"]
    for exchange in exchanges:
        token_hashes = exchange["request"].get("token_wire_sha256")
        parts = query_parts(exchange["request"]["query"])
        names = [key for key, _ in parts]
        if len(names) != len(set(names)) and row["id"] not in {"gcs-05", "azure-08"}:
            raise ValueError("capture has duplicate query field outside duplicate-parameter probe")
        token_values = [value for key, value in parts
                        if key == TOKEN_FIELDS[row["provider"]] and value]
        if not isinstance(token_hashes, list) or len(token_hashes) != len(token_values) \
                or any(not isinstance(value, str) or not SHA256.fullmatch(value)
                       for value in token_hashes):
            raise ValueError("capture lacks raw continuation-token spelling hashes")
    exact_request = request["method"] + " " + request["path"] \
        + ("?" + request["query"] if request["query"] else "")
    if proof["probe_request"] != exact_request:
        raise ValueError("ledger probe request differs from capture")
    if row["id"] == "gcs-08":
        if not SHA256.fullmatch(proof.get("raw_wire_sha256", "")):
            raise ValueError("GCS raw spelling row needs independently sanitized raw-wire artifact")
        raw_wire = checked_child(captures, proof.get("raw_wire_file", ""))
        if hashlib.sha256(raw_wire.read_bytes()).hexdigest() != proof["raw_wire_sha256"]:
            raise ValueError("GCS raw-wire SHA-256 differs from ledger")
    if row["provider"] == "azure":
        for exchange in exchanges:
            for side in ("request", "response"):
                version_headers = [header["value"] for header in exchange[side]["headers"]
                                   if header["name"] == "x-ms-version"]
                if version_headers != [version]:
                    raise ValueError("Azure capture header version differs from ledger")
    if kind in {"sdk_run", "retry_run"}:
        identity = manifest["sdk_product"] + "/" + manifest["sdk_version"]
        for exchange in exchanges:
            sdk_values = [token for header in exchange["request"]["headers"]
                          if header["name"] in {"user-agent", "x-goog-api-client"}
                          for token in header["value"].split()]
            if identity not in sdk_values:
                raise ValueError("SDK run lacks exact pinned product/version token")
    if status == "REPLAY_MATCH":
        if repo_root is None or not proof.get("replay_test") or not proof.get("replay_test_path") \
                or not SHA256.fullmatch(proof.get("replay_result_sha256", "")) \
                or replay_commit is None or distribution is None:
            raise ValueError("replay match lacks verifiable test/result")
        test_path = checked_child(repo_root, proof["replay_test_path"])
        source = test_path.read_text(encoding="utf-8")
        method = re.escape(proof["replay_test"])
        if test_path.suffix == ".java":
            found = re.search(rf'@(?:Test|ParameterizedTest|RepeatedTest)\b[^@{{}}]*?'
                              rf'\bvoid\s+{method}\s*\(', source, re.DOTALL)
        elif test_path.suffix == ".py":
            found = re.search(rf'^\s*def\s+{method}\s*\(\s*self\b', source, re.MULTILINE)
        else:
            found = None
        if not found:
            raise ValueError("replay test identifier does not resolve to source")
        result_path = checked_child(captures, proof.get("replay_result_file", ""))
        result_bytes = result_path.read_bytes()
        if hashlib.sha256(result_bytes).hexdigest() != proof["replay_result_sha256"]:
            raise ValueError("replay result SHA-256 differs from ledger")
        result = strict_json(result_bytes.decode("utf-8"))
        if result.get("status") != "PASS" or result.get("capture_sha256") != proof["capture_sha256"] \
                or result.get("test") != proof["replay_test"] or result.get("api_version") != version \
                or result.get("test_path") != proof["replay_test_path"] \
                or result.get("test_source_sha256") != hashlib.sha256(test_path.read_bytes()).hexdigest() \
                or result.get("schema_version") != "provider-match-receipt-v2" \
                or result.get("comparison") != "ProviderExchangeComparator" \
                or result.get("evidence_kind") != kind \
                or result.get("compared_steps") != len(exchanges) \
                or not isinstance(result.get("replay_commit"), str) \
                or not re.fullmatch(r"[0-9a-f]{40}", result["replay_commit"]) \
                or result.get("distribution_sha256") != hashlib.sha256(distribution.read_bytes()).hexdigest() \
                or result.get("distribution_name") != distribution.name:
            raise ValueError("replay result receipt does not prove matched capture/test/version")
        ancestor = subprocess.run(["git", "-C", str(repo_root), "merge-base", "--is-ancestor",
                                   result["replay_commit"], replay_commit], capture_output=True)
        if ancestor.returncode != 0:
            raise ValueError("receipt commit is not an ancestor of current replay source")


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
        if provider == "azure" and "`2026-" in provider_display:
            displayed_version = provider_display.split("`", 2)[1]
            if row["api_versions"] != [displayed_version]:
                raise ValueError("matrix Azure version differs from ledger")
        if row["provider"] != provider or row["feature"] != feature \
                or row["profile_promise"] != promise or row["status"] != status:
            raise ValueError("matrix differs from evidence ledger")
        if status == "UNMEASURED" and (checksum != "—" or replay_test != "—"):
            raise ValueError("unmeasured matrix row claims capture or replay test")
        if status != "UNMEASURED":
            proofs = row["evidence"]
            expected_checksum = "; ".join(
                version + ":" + proofs[version]["capture_sha256"] for version in row["api_versions"])
            if checksum != expected_checksum:
                raise ValueError("matrix capture checksum differs from ledger")
            if status == "OBSERVED" and replay_test != "—":
                raise ValueError("observed matrix row cannot claim replay test")
            if status == "REPLAY_MATCH":
                expected_tests = "; ".join(version + ":" + proofs[version]["replay_test"]
                                           for version in row["api_versions"])
                if replay_test.strip("`") != expected_tests:
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
    sanitize.add_argument("--kind", choices=sorted(EVIDENCE_KINDS), default="exchange")
    check = sub.add_parser("validate")
    check.add_argument("--manifest", type=Path, action="append", default=[])
    check.add_argument("--ledger", type=Path, required=True)
    check.add_argument("--matrix", type=Path, required=True)
    check.add_argument("--captures", type=Path)
    check.add_argument("--repo-root", type=Path)
    check.add_argument("--distribution", type=Path)
    args = parser.parse_args()
    if args.command == "validate":
        manifests = {}
        for manifest_path in args.manifest:
            manifest = strict_json(manifest_path.read_text(encoding="utf-8"))
            validate_manifest(manifest)
            key = (manifest["provider"], manifest["api_version"])
            if key in manifests:
                raise ValueError("duplicate provider/version manifest")
            manifests[key] = manifest
        ledger = strict_json(args.ledger.read_text(encoding="utf-8"))
        commit = None
        if args.repo_root is not None:
            commit = subprocess.run(["git", "-C", str(args.repo_root), "rev-parse", "HEAD"],
                                    check=True, capture_output=True, text=True).stdout.strip()
        validate_ledger(ledger, manifests, args.captures, args.repo_root,
                        commit, args.distribution)
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
    manifest_digest = hashlib.sha256(canonical(manifest)).hexdigest()
    safe = sanitize_exchange(raw, args.provider, replacements, manifest_digest, args.probe_id,
                             manifest["api_version"]) if args.kind == "exchange" else sanitize_run(
                                 raw, args.provider, replacements, manifest_digest,
                                 args.probe_id, manifest["api_version"], args.kind)
    payload = canonical(safe)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(payload)
    print(hashlib.sha256(payload).hexdigest())


if __name__ == "__main__":
    main()
