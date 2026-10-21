# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""GCS JSON v1 objects.list adapter, corpus and case matrix for campaign.py.

Every case is a separate ledger row so one observation proves one boundary. Names and
queries are built relative to the isolated run prefix; raw query spellings are written
out exactly where the spelling is the subject of the probe.
"""

from __future__ import annotations

import base64
import json
import re
import os
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

import evidence
from campaign import HttpClient, Listed, Request, Response
from matrix import (Case, CorpusObject, PriceTable, TokenSource, Untested, common_groups, corpus_objects,
                    enc, qs, spelling_probes)

HOST = "storage.googleapis.com"
API_VERSION = "json-v1"
SDK_PRODUCT = "gcloud-java"
SDK_VERSION = "2.73.0"
MAX_NAME_BYTES = 1024
# Exactly one page capacity under `b/0`, plus one more name under `b/`.
PAGE_CAPACITY = 1000
REFRESH_MARGIN_SECONDS = 300
TOKEN_HOST = "oauth2.googleapis.com"
DEFAULT_ADC = Path.home() / ".config/gcloud/application_default_credentials.json"
# Cases whose response body spelling is published as a separately checksummed artifact.
RAW_WIRE_CASES = frozenset({"gcs-08a", "gcs-08b", "gcs-04a"})
# Case SDK option names mapped to ProviderSdkCapture option fields.
SDK_OPTION_NAMES = {"walk": "walk", "prefix": "prefix", "delimiter": "delimiter",
                    "page_size": "pageSize", "start_offset": "startOffset", "end_offset": "endOffset"}


class _AuthResponse:
    """google-auth transport response over a campaign-counted exchange."""

    def __init__(self, response: Response):
        self.status = response.status
        self.headers = {name.lower(): value for name, value in response.headers}
        self.data = response.body


class GatedAuthRequest:
    """google-auth transport callable: every token request, including google-auth's own
    retries, is one counted, deadline-bound, byte-charged campaign attempt."""

    def __init__(self, client: HttpClient):
        self.client = client
        self.phase = None

    def __call__(self, url, method="GET", body=None, headers=None, timeout=None, **kwargs):
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.hostname != TOKEN_HOST:
            raise ValueError("credential refresh must go to the Google OAuth token endpoint")
        data = body.encode("utf-8") if isinstance(body, str) else (body or b"")
        request = Request(method, parts.hostname, parts.path, parts.query,
                          tuple((headers or {}).items()), data)
        return _AuthResponse(self.client.send(request, self.phase, authorize=False, nested=True))


class GcsAuthorizer:
    """User Application Default Credentials loaded from one explicit file, no discovery."""

    def __init__(self, client: HttpClient, credentials_file: Path):
        from google.oauth2.credentials import Credentials
        # gcloud's user credentials refresh with the scopes granted at login (cloud-platform);
        # requesting a narrower storage scope is refused for that OAuth client.
        self.credentials = Credentials.from_authorized_user_file(str(credentials_file))
        self.transport = GatedAuthRequest(client)
        self.lock = threading.Lock()

    def headers(self, phase: str, request: "Request") -> list[tuple[str, str]]:
        with self.lock:
            expiry = self.credentials.expiry
            if not self.credentials.token or expiry is None or \
                    expiry.replace(tzinfo=None).timestamp() - time.time() < REFRESH_MARGIN_SECONDS:
                self.transport.phase = phase
                self.credentials.refresh(self.transport)
            return [("Authorization", "Bearer " + self.credentials.token)]

    def credential_query(self) -> str:
        return ""


class GcsProvider:
    name = "gcs"
    api_versions = (API_VERSION,)

    def __init__(self, bucket: str, project: str, region: str, account: str | None,
                 credentials_file: Path = DEFAULT_ADC):
        if not bucket or not project:
            raise ValueError("GCS bucket and project must be configured explicitly")
        self.bucket = bucket
        self.project = project
        self.region = region
        self.account = account
        self.credentials_file = credentials_file

    @classmethod
    def from_environment(cls) -> "GcsProvider":
        return cls(os.environ.get("SWATH_CONFORMANCE_GCS_BUCKET", ""),
                   os.environ.get("SWATH_CONFORMANCE_GCS_PROJECT", ""),
                   os.environ.get("SWATH_CONFORMANCE_GCS_REGION", "us-east1"),
                   os.environ.get("SWATH_CONFORMANCE_GCS_ACCOUNT"),
                   Path(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", str(DEFAULT_ADC))))

    # Identity and policy ------------------------------------------------------------

    def redactions(self) -> dict[str, str]:
        mapping = {self.bucket: "{bucket}", self.project: "{project}"}
        if self.account:
            mapping[self.account] = "{account-email}"
        return mapping

    def sanitize_names(self) -> dict[str, str]:
        # Raw captures are redacted before they are written, so the sanitizer sees placeholders.
        return {"bucket": "{bucket}"}

    def private_identifiers(self) -> list[str]:
        return [value for value in (self.bucket, self.project, self.account) if value]

    def authorizer(self, client: HttpClient):
        return GcsAuthorizer(client, self.credentials_file)

    def version_echoed(self, version: str, response: Response) -> bool:
        return True

    def version_unavailable(self, response: Response) -> bool:
        return False

    def prices(self) -> PriceTable:
        # Class A, regional Standard, flat namespace: $0.005 per 1,000 operations; Class B
        # and deletes are cheaper, so every attempt is charged as Class A. Storage is
        # $0.020/GB-month (rounded up). Internet egress is charged at $0.23/GiB, above the
        # highest published worldwide tier, because the client's destination region may vary.
        return PriceTable("https://cloud.google.com/storage/pricing", "2026-10-21",
                          0.005 / 1000, 0.026, 0.23)

    def manifest_fields(self) -> dict:
        return {"namespace_mode": "flat-ubla", "region": self.region, "api_version": API_VERSION,
                "sdk_product": SDK_PRODUCT, "sdk_version": SDK_VERSION,
                "storage_class": "STANDARD",
                "bucket_policy": {"uniform_bucket_level_access": True,
                                  "public_access_prevention": "enforced",
                                  "soft_delete": "disabled", "lifecycle_delete_age_days": 7},
                "native_probe_client": "python http.client, no SDK, no Accept-Encoding"}

    def canonical_name(self, name: str) -> str:
        return name

    # Requests -----------------------------------------------------------------------

    def upload(self, item: CorpusObject) -> Request:
        name = item.wire_name if item.wire_name is not None else enc(item.name, safe="")
        query = f"uploadType=media&name={name}&ifGenerationMatch=0"
        return Request("POST", HOST, f"/upload/storage/v1/b/{self.bucket}/o", query,
                       (("Content-Type", "application/octet-stream"),), item.payload)

    def created(self, response: Response) -> bool:
        return response.status == 200

    def delete(self, name: str, version: str | None) -> Request | None:
        if not version:
            return None
        return Request("DELETE", HOST, f"/storage/v1/b/{self.bucket}/o/{enc(name, safe='')}",
                       f"ifGenerationMatch={version}")

    def deleted(self, response: Response) -> bool:
        return response.status == 204

    def list_request(self, query: str, version: str | None, headers: tuple = ()) -> Request:
        return Request("GET", HOST, f"/storage/v1/b/{self.bucket}/o", query,
                       (("Accept", "application/json"), *headers))

    def with_token(self, query: str, token: str) -> str:
        return query + ("&" if query else "") + "pageToken=" + enc(token, safe="")

    def page(self, body: bytes) -> tuple[list[Listed], str | None]:
        parsed = evidence.strict_json(body.decode("utf-8"))
        entries = [Listed(item["name"], size=int(item["size"]), updated=item["updated"],
                          version=item.get("generation")) for item in parsed.get("items", [])]
        entries.extend(Listed(prefix, prefix=True) for prefix in parsed.get("prefixes", []))
        return entries, parsed.get("nextPageToken") or None

    def inventory_query(self, prefix: str) -> str:
        return qs(("prefix", prefix)) if prefix else ""

    def scrub(self, body: bytes) -> bytes:
        # ACL entities name users and projects; uniform bucket-level access should omit them.
        return re.sub(rb'"(entity|email|entityId)":\s*"[^"]*"', rb'"\1":"{redacted-acl-entity}"', body)

    def raw_wire(self, raw: dict, manifest_sha256: str, probe_id: str,
                 capture_sha256: str) -> bytes | None:
        if probe_id not in RAW_WIRE_CASES:
            return None
        body = base64.b64decode(raw["response"]["body_base64"]).decode("utf-8")
        parsed = evidence.strict_json(body)
        token = parsed.get("nextPageToken")
        if token:
            body = body.replace(json.dumps(token)[1:-1], evidence.opaque_digest(token))
        # Namespace substitution only: object names never contain the bucket name, which
        # the capture sanitizer already proved for the same exchange.
        body = body.replace(self.bucket, "{bucket}")
        evidence.strict_json(body)
        return evidence.canonical({"schema_version": "provider-raw-wire-v1", "provider": self.name,
                                   "probe_id": probe_id, "manifest_sha256": manifest_sha256,
                                   "capture_sha256": capture_sha256, "body_utf8": body})

    def sdk_options(self, case: Case, version: str | None) -> dict:
        options = {"provider": "gcs", "credentials": "adc"}
        for source, target in SDK_OPTION_NAMES.items():
            if case.sdk.get(source) is not None:
                options[target] = case.sdk[source]
        return options

    def sdk_identity(self) -> dict[str, str]:
        return {"product": SDK_PRODUCT, "version": SDK_VERSION}

    def sdk_environment(self) -> dict[str, str]:
        # Identity and redaction reach the driver through its environment, never a file.
        return {"SWATH_CAPTURE_IDENTITY": json.dumps({"bucket": self.bucket, "projectId": self.project}),
                "SWATH_CAPTURE_REDACT": json.dumps(self.redactions()),
                "GOOGLE_APPLICATION_CREDENTIALS": str(self.credentials_file)}

    # Corpus -------------------------------------------------------------------------

    def corpus(self, run_prefix: str, profile: str) -> list[CorpusObject]:
        R = run_prefix
        groups = common_groups()
        if profile == "smoke":
            return corpus_objects(R, groups[:1])
        groups[3][1].append(("p/bs\\x", True))
        names: list[tuple] = [("n/" + n, True) for n in (
            "end.", "dots..", "./x", "../x", "trail/", "nbsp\u00a0x", "zwj\u200dx", "xml<&>x",
            "/".join(["seg"] * 12))]
        names += [("n/" + n, None) for n in ("tab\tx", "ctl\x01x", "del\x7fx", "ls\u2028x", "bom\ufeffx")]
        long_name = "n/L"
        long_name += "x" * (MAX_NAME_BYTES - len((R + long_name).encode("utf-8")))
        names.append((long_name, True))
        groups.append(("n", names))
        groups.append(("b", [(f"b/{i:04d}", True) for i in range(PAGE_CAPACITY)] + [("b/zzzz", True)]))
        over = "q/over"
        over += "x" * (MAX_NAME_BYTES + 1 - len((R + over).encode("utf-8")))
        multibyte = "q/mb"
        while len((R + multibyte + "\U0001f600").encode("utf-8")) <= MAX_NAME_BYTES:
            multibyte += "\U0001f600"
        multibyte += "x" * (MAX_NAME_BYTES - len((R + multibyte).encode("utf-8")))
        # The same multibyte name one byte over the limit.
        multibyte_over = "q/mo" + multibyte[len("q/mb"):] + "x"
        groups.append(("q", [(over, False), (multibyte_over, False), ("q/cr\rx", False), ("q/lf\nx", False),
                             ("q/nul\x00x", None), ("q/ffff\uffff", None), ("q/fffe\ufffe", None),
                             ("q/max\U0010ffff", None), (multibyte, None),
                             ("q/surrogate", False, "%ED%A0%80"),
                             ("q/surrogate-pair", False, "%ED%A0%BD%ED%B8%80"),
                             ("q/low-surrogate", False, "%ED%B0%80"),
                             ("q/invalid-utf8", False, "%FF")]))
        return corpus_objects(R, groups)

    # Case matrix --------------------------------------------------------------------

    def untested(self) -> list[Untested]:
        return [
            Untested("Bucket-not-found, wrong-method and HEAD list errors",
                     "would address another bucket name or a mutating route; the campaign touches "
                     "only its own bucket and read-only list requests"),
            Untested("Object names `.`, `..` and `.well-known/acme-challenge/` at bucket root",
                     "they cannot be created inside the isolated run prefix"),
            Untested("Unknown and percent-encoded query parameter names",
                     "the capture sanitizer matches a literal allowlist so a credential cannot be "
                     "retained; refusal stays a replay-only behavior"),
            Untested("Rejected-create exchanges as ledger rows",
                     "the ledger has no creation evidence kind; create status, typed error and wire "
                     "spelling are published in the evidence index instead"),
            Untested("Retryable native 5xx and SDK retry counts (gcs-10)",
                     "a provider 5xx cannot be induced on demand within the bounded campaign"),
            Untested("Hierarchical-namespace buckets and managed folders",
                     "the dedicated bucket is flat with uniform bucket-level access"),
            Untested("Object versions and soft-deleted listings beyond refusal checks",
                     "the bucket has no versioning or soft delete; only the query refusal shape is probed"),
            Untested("Consistency under concurrent writes",
                     "the corpus is quiescent by design; listing consistency is out of scope"),
        ]

    def cases(self, run_prefix: str, profile: str) -> list[Case]:
        R = run_prefix
        cases: list[Case] = []

        def ex(case_id: str, title: str, discriminates: str, query: str, smoke: bool = False) -> None:
            cases.append(Case(case_id, title, discriminates, "exchange", query, smoke=smoke))

        def walk(case_id: str, title: str, discriminates: str, query: str, smoke: bool = False) -> None:
            cases.append(Case(case_id, title, discriminates, "walk", query, smoke=smoke))

        def token(case_id: str, title: str, discriminates: str, query: str, source: TokenSource,
                  smoke: bool = False) -> None:
            cases.append(Case(case_id, title, discriminates, "token", query, token=source, smoke=smoke))

        def sdk(case_id: str, title: str, discriminates: str, options: dict, smoke: bool = False) -> None:
            cases.append(Case(case_id, title, discriminates, "sdk_run", sdk=options, smoke=smoke))

        r = R + "r/"
        # Ranges.
        ex("gcs-01a", "`startOffset` equal to an existing name is inclusive",
           "inclusive versus exclusive lower bound", qs(("prefix", r), ("startOffset", r + "k02")), True)
        ex("gcs-01b", "`endOffset` equal to an existing name is exclusive",
           "exclusive versus inclusive upper bound", qs(("prefix", r), ("endOffset", r + "k05")))
        ex("gcs-01c", "Equal `startOffset` and `endOffset` on an existing name",
           "empty result versus single object versus error", qs(("prefix", r), ("startOffset", r + "k03"),
                                                               ("endOffset", r + "k03")))
        ex("gcs-01d", "`startOffset` in a gap between names",
           "seek to the next name", qs(("prefix", r), ("startOffset", r + "k04")))
        ex("gcs-01e", "`endOffset` in a gap between names",
           "stop before the next name", qs(("prefix", r), ("endOffset", r + "k04")))
        ex("gcs-01f", "Inverted bounds with `startOffset` after `endOffset`",
           "empty result versus 400", qs(("prefix", r), ("startOffset", r + "k06"), ("endOffset", r + "k02")))
        ex("gcs-01g", "Both offsets without a prefix",
           "offsets apply to the whole bucket", qs(("startOffset", r + "k02"), ("endOffset", r + "k06")))
        ex("gcs-01h", "Offsets before and after every corpus name",
           "out-of-corpus offsets clip nothing", qs(("prefix", r), ("startOffset", R), ("endOffset", R + "zzz")))
        ex("gcs-01i", "Empty `startOffset` and `endOffset` values",
           "empty value means absent versus error", "prefix=" + enc(r) + "&startOffset=&endOffset=")
        ex("gcs-01j", "`startOffset` equal to the prefix", "prefix-equal lower bound",
           qs(("prefix", r), ("startOffset", r)))
        ex("gcs-01k", "`endOffset` equal to the prefix", "prefix-equal upper bound empties the scope",
           qs(("prefix", r), ("endOffset", r)))
        # Prefix intersections.
        x = R + "x/"
        ex("gcs-02a", "Prefix with `startOffset` before it and `endOffset` inside it",
           "intersection uses the later lower bound", qs(("prefix", x + "a"), ("startOffset", R + "w"),
                                                        ("endOffset", x + "abc")))
        ex("gcs-02b", "Prefix with `startOffset` inside it and `endOffset` after it",
           "intersection uses the earlier upper bound", qs(("prefix", x + "a"), ("startOffset", x + "ab"),
                                                          ("endOffset", R + "y")))
        ex("gcs-02c", "Prefix with both offsets before it", "disjoint range yields empty",
           qs(("prefix", x + "a"), ("startOffset", r), ("endOffset", R + "s")))
        ex("gcs-02d", "Prefix with both offsets after it", "disjoint range yields empty",
           qs(("prefix", x + "a"), ("startOffset", R + "y"), ("endOffset", R + "z")))
        ex("gcs-02e", "Prefix that is a string prefix of longer names", "byte prefix, not path segment",
           qs(("prefix", x + "a")))
        ex("gcs-02f", "Prefix equal to a full name with `endOffset` equal to that name",
           "exclusive end removes the only match", qs(("prefix", x + "ab"), ("endOffset", x + "ab")))
        # Delimiters and offsets.
        d = R + "d/"
        ex("gcs-03a", "Delimiter `/` baseline over objects, subtrees, a folder marker and an empty segment",
           "item and prefix membership", qs(("prefix", d), ("delimiter", "/")))
        ex("gcs-03b", "Delimiter with `startOffset` equal to a subtree prefix",
           "subtree prefix emitted at its own boundary", qs(("prefix", d), ("delimiter", "/"),
                                                            ("startOffset", d + "a/")))
        ex("gcs-03c", "Delimiter with `startOffset` inside a subtree with remaining descendants",
           "subtree prefix emitted from a mid-subtree offset", qs(("prefix", d), ("delimiter", "/"),
                                                                  ("startOffset", d + "a/m")))
        ex("gcs-03d", "Delimiter with `startOffset` inside a subtree after its last descendant",
           "prefix emitted only for in-range descendants", qs(("prefix", d), ("delimiter", "/"),
                                                              ("startOffset", d + "b/3")))
        ex("gcs-03e", "Delimiter with `startOffset` after a subtree, equal to an object",
           "earlier subtree excluded", qs(("prefix", d), ("delimiter", "/"), ("startOffset", d + "a0")))
        ex("gcs-03f", "Delimiter with `endOffset` inside a subtree",
           "subtree prefix emitted when only part of it is in range", qs(("prefix", d), ("delimiter", "/"),
                                                                         ("endOffset", d + "a/m")))
        ex("gcs-03g", "Delimiter with `endOffset` equal to a subtree prefix",
           "exclusive end at the subtree boundary", qs(("prefix", d), ("delimiter", "/"),
                                                       ("endOffset", d + "a/")))
        ex("gcs-03h", "Delimiter with `endOffset` just after a folder marker",
           "marker-only subtree in range", qs(("prefix", d), ("delimiter", "/"), ("endOffset", d + "a/1")))
        ex("gcs-03i", "Delimiter `/` with a prefix ending in the delimiter",
           "folder-marker object equal to the prefix is an item", qs(("prefix", d + "a/"), ("delimiter", "/")))
        ex("gcs-03j", "Delimiter `/` with a prefix that stops before the delimiter",
           "rollup immediately after the prefix", qs(("prefix", d + "a"), ("delimiter", "/")))
        ex("gcs-03k", "Pipe delimiter that never occurs in names", "flat result with no prefixes",
           qs(("prefix", d), ("delimiter", "|")))
        ex("gcs-03l", "Two-character delimiter `/1`", "multi-character rollup and prefix equal to a name",
           qs(("prefix", d), ("delimiter", "/1")))
        ex("gcs-03m", "Two-character delimiter `-g`", "multi-character rollup inside a segment",
           qs(("prefix", d), ("delimiter", "-g")))
        ex("gcs-03n", "Delimiter with both offsets cutting two subtrees",
           "prefix membership under both bounds", qs(("prefix", d), ("delimiter", "/"),
                                                     ("startOffset", d + "a/m"), ("endOffset", d + "b/2")))
        ex("gcs-03o", "Empty `delimiter` value", "empty value means absent versus error",
           "prefix=" + enc(d) + "&delimiter=")
        ex("gcs-03p", "`includeTrailingDelimiter=true` with delimiter `/`",
           "native feature replay refuses", qs(("prefix", d), ("delimiter", "/"),
                                               ("includeTrailingDelimiter", "true")))
        ex("gcs-03q", "Delimiter without a prefix from a mid-corpus `startOffset`",
           "top-level rollup with an offset", qs(("delimiter", "/"), ("startOffset", d), ("endOffset", R + "e")))
        # Unicode ordering.
        u = R + "u/"
        ex("gcs-04a", "BMP, surrogate-neighbor and supplementary names in list order",
           "unsigned UTF-8 byte order versus UTF-16 code-unit order", qs(("prefix", u)))
        ex("gcs-04b", "`startOffset` at U+E000", "offset comparison in byte order",
           qs(("prefix", u), ("startOffset", u + "")))
        ex("gcs-04c", "`endOffset` at U+10000", "offset comparison in byte order",
           qs(("prefix", u), ("endOffset", u + "\U00010000")))
        ex("gcs-04d", "Prefix in NFC form with an NFD sibling", "no Unicode normalization",
           qs(("prefix", u + "é")))
        ex("gcs-04e", "Prefix differing only in case", "case-sensitive names", qs(("prefix", u + "E")))
        ex("gcs-04f", "Supplementary-character `startOffset` percent-encoded in lowercase hex",
           "escape case does not change the offset", "prefix=" + enc(u) + "&startOffset=" + enc(u) + "%f0%9f%98%80")
        # Query spelling over names with reserved and escape-like characters.
        p = R + "p/"
        for suffix, title, question, query in spelling_probes("prefix", p):
            ex("gcs-05" + suffix, title, question, query)
        ex("gcs-05r", "Raw `=` inside a `startOffset` value", "first `=` splits name from value",
           "prefix=" + enc(p) + "&startOffset=" + enc(p) + "eq=x")
        # Page size.
        s = R + "s/"
        b = R + "b/"
        ex("gcs-06a", "`maxResults=0` on a three-object scope", "zero refused versus default",
           qs(("prefix", s), ("maxResults", "0")))
        walk("gcs-06b", "`maxResults=0` on a 1,001-object scope", "zero as default 1,000 versus unbounded",
             qs(("prefix", b), ("maxResults", "0")))
        walk("gcs-06c", "`maxResults=1` walk", "page size one with continuation",
             qs(("prefix", s), ("maxResults", "1")))
        walk("gcs-06d", "`maxResults=1000` walk over exactly 1,000 objects",
             "exact page multiple: terminal page versus trailing empty page",
             qs(("prefix", b + "0"), ("maxResults", "1000")))
        ex("gcs-06e", "`maxResults=1001` over 1,001 objects", "cap at 1,000 versus 400 versus 1,001",
           qs(("prefix", b), ("maxResults", "1001")))
        ex("gcs-06f", "`maxResults=-1`", "negative refused", qs(("prefix", s), ("maxResults", "-1")), True)
        ex("gcs-06g", "`maxResults=abc`", "non-numeric refused", qs(("prefix", s), ("maxResults", "abc")))
        ex("gcs-06h", "`maxResults=2147483648`", "above signed 32-bit", qs(("prefix", s), ("maxResults", "2147483648")))
        ex("gcs-06i", "`maxResults=9223372036854775808`", "above signed 64-bit",
           qs(("prefix", s), ("maxResults", "9223372036854775808")))
        ex("gcs-06j", "`maxResults=1.5`", "fractional refused", qs(("prefix", s), ("maxResults", "1.5")))
        ex("gcs-06k", "`maxResults=+2` with a raw plus", "sign or space before digits",
           "prefix=" + enc(s) + "&maxResults=+2")
        ex("gcs-06l", "`maxResults=002`", "leading zeros", qs(("prefix", s), ("maxResults", "002")))
        ex("gcs-06m", "Duplicate `maxResults` values", "first, last or 400",
           "prefix=" + enc(s) + "&maxResults=1&maxResults=2")
        ex("gcs-06n", "Empty `maxResults` value", "empty means default versus error",
           "prefix=" + enc(s) + "&maxResults=")
        ex("gcs-06o", "`maxResults=2147483647`", "signed 32-bit maximum", qs(("prefix", s), ("maxResults", "2147483647")))
        # Walks.
        walk("gcs-07a", "Delimiter walk with one entry per page over top-level subtrees",
             "prefix repetition across page boundaries over large collapsed subtrees",
             qs(("prefix", R), ("delimiter", "/"), ("maxResults", "1")))
        walk("gcs-07b", "Default page size walk over 1,001 objects", "default 1,000 and a short final page",
             qs(("prefix", b)))
        walk("gcs-07c", "Delimiter walk with two entries per page", "pages ending on a prefix boundary",
             qs(("prefix", d), ("delimiter", "/"), ("maxResults", "2")))
        walk("gcs-07d", "Delimiter walk with offsets and one entry per page",
             "offsets retained across continuation", qs(("prefix", d), ("delimiter", "/"),
                                                        ("startOffset", d + "a/m"), ("endOffset", d + "e"),
                                                        ("maxResults", "1")))
        walk("gcs-07e", "Flat walk with four entries per page over six names", "short final page",
             qs(("prefix", r), ("maxResults", "4")), True)
        ex("gcs-07f", "Prefix with no matching names", "empty result shape: omitted versus empty arrays",
           qs(("prefix", R + "none/")))
        walk("gcs-07n", "Flat walk with two entries per page over delimited names",
             "token source for adding a delimiter", qs(("prefix", d), ("maxResults", "2")))
        walk("gcs-07q", "Flat walk with two entries per page over six names",
             "exact small page multiple: terminal page versus trailing empty page",
             qs(("prefix", r), ("maxResults", "2")), True)
        same = qs(("prefix", r), ("maxResults", "2"))
        token("gcs-07g", "Malformed constant `pageToken`", "malformed token error", same,
              TokenSource(mutation="tampered"))
        token("gcs-07h", "Reused `pageToken` for the same query", "tokens are repeatable", same,
              TokenSource("gcs-07q", 0))
        token("gcs-07i", "`pageToken` from a different prefix", "token bound to query versus position only",
              qs(("prefix", x), ("maxResults", "2")), TokenSource("gcs-07q", 0))
        token("gcs-07j", "`pageToken` with a changed page size", "page size may change between pages",
              qs(("prefix", r), ("maxResults", "3")), TokenSource("gcs-07q", 0))
        token("gcs-07k", "`pageToken` from a flat walk with an added delimiter", "token bound to delimiter",
              qs(("prefix", d), ("delimiter", "/"), ("maxResults", "2")), TokenSource("gcs-07n", 0))
        token("gcs-07l", "`pageToken` with an added `startOffset` past the token position",
              "offset versus token precedence", qs(("prefix", r), ("maxResults", "2"),
                                                   ("startOffset", r + "k06")), TokenSource("gcs-07q", 0))
        ex("gcs-07m", "Empty `pageToken` value", "empty token means first page",
           "prefix=" + enc(r) + "&maxResults=2&pageToken=")
        token("gcs-07o", "`pageToken` from a delimiter walk with the delimiter removed",
              "token bound to delimiter", qs(("prefix", d), ("maxResults", "2")), TokenSource("gcs-07c", 0))
        token("gcs-07r", "Issued `pageToken` with its last character changed", "token integrity check",
              same, TokenSource("gcs-07q", 0, "flip"), True)
        token("gcs-07s", "Issued `pageToken` truncated by four characters", "token integrity check",
              same, TokenSource("gcs-07q", 0, "truncate"))
        token("gcs-07v", "Issued `pageToken` with one decoded byte changed and re-encoded",
              "integrity rejection versus malformed encoding", same, TokenSource("gcs-07q", 0, "flip-decoded"))
        token("gcs-07t", "`pageToken` with an `endOffset` excluding every remaining name",
              "empty-result continuation: entries and continuation", qs(("prefix", r), ("maxResults", "2"),
                                                                        ("endOffset", r + "k02")),
              TokenSource("gcs-07q", 0))
        token("gcs-07u", "`pageToken` with a prefix matching only names before it",
              "empty-result continuation under a narrower prefix", qs(("prefix", r + "k01"), ("maxResults", "2")),
              TokenSource("gcs-07q", 0))
        # Names and wire spelling.
        n = R + "n/"
        ex("gcs-08a", "Names with control, whitespace, dot, markup and 1,024-byte length",
           "accepted names and raw JSON escaping", qs(("prefix", n)))
        ex("gcs-08b", "Delimiter listing over dot segments and trailing-slash names",
           "dot segments are literal", qs(("prefix", n), ("delimiter", "/")))
        ex("gcs-08c", "Listing of exploratory creation-probe names", "which rejected names exist",
           qs(("prefix", R + "q/")))
        # Official SDK walks.
        sdk("gcs-09a", "HTTP Java SDK flat auto-pagination over Unicode names",
            "default projection, fields and token handling",
            {"walk": "flat", "prefix": u, "page_size": 5})
        sdk("gcs-09b", "HTTP Java SDK hierarchy auto-pagination", "currentDirectory request shape",
            {"walk": "hierarchy", "prefix": d, "delimiter": "/", "page_size": 2})
        sdk("gcs-09c", "HTTP Java SDK offsets with auto-pagination", "offsets on every page",
            {"walk": "flat", "prefix": r, "start_offset": r + "k02", "end_offset": r + "k07",
             "page_size": 2}, True)
        # Projection and refused features.
        ex("gcs-11a", "Explicit `projection=full` under uniform bucket-level access",
           "ACL and owner fields", qs(("prefix", s), ("projection", "full")))
        ex("gcs-11b", "Explicit `projection=noAcl`", "named default", qs(("prefix", s), ("projection", "noAcl")))
        ex("gcs-11c", "Unknown projection value", "400 shape", qs(("prefix", s), ("projection", "bogus")))
        ex("gcs-11d", "`prettyPrint=false` and `alt=json`", "transport forms",
           qs(("prefix", s), ("prettyPrint", "false"), ("alt", "json")))
        ex("gcs-11e", "Nonempty `fields` partial response", "native partial response replay refuses",
           qs(("prefix", s), ("fields", "items(name),nextPageToken")))
        ex("gcs-11f", "`versions=true`", "native feature replay refuses", qs(("prefix", s), ("versions", "true")))
        ex("gcs-11g", "`matchGlob`", "native feature replay refuses", qs(("prefix", s), ("matchGlob", s + "*")))
        ex("gcs-11h", "`softDeleted=true` with soft delete disabled", "native refusal shape",
           qs(("prefix", s), ("softDeleted", "true")))
        ex("gcs-11i", "`includeFoldersAsPrefixes=true` on a flat bucket", "native refusal shape",
           qs(("prefix", d), ("delimiter", "/"), ("includeFoldersAsPrefixes", "true")))
        ex("gcs-11j", "Named false forms of boolean options", "false equals omitted",
           qs(("prefix", s), ("versions", "false"), ("includeTrailingDelimiter", "false"),
              ("softDeleted", "false")))
        if profile == "smoke":
            return [case for case in cases if case.smoke]
        return cases
