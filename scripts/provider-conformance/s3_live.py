# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Amazon S3 ListObjectsV2 adapter, corpus and case matrix for campaign.py.

Requests are path-style against the regional endpoint and signed with SigV4 from
short-lived session credentials read from the environment at send time; the signature,
date and session token are never part of a recorded request. ListObjects (V1) appears
only where it discriminates a V2 ambiguity.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, unquote_plus, unquote_to_bytes
from xml.etree import ElementTree

from campaign import HttpClient, Listed, Request, Response
from matrix import (Case, CorpusObject, PriceTable, TokenSource, Untested, common_groups, corpus_objects,
                    enc, qs, spelling_probes)

API_VERSION = "2006-03-01"
SDK_PRODUCT = "aws-sdk-java"
SDK_VERSION = "2.31.78"
SERVICE = "s3"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
MAX_NAME_BYTES = 1024
PAGE_CAPACITY = 1000
LIST_V2 = "list-type=2"
# Per-object listing fields the replay fixture carries verbatim.
FIXTURE_ATTRIBUTES = ("ETag", "StorageClass", "ChecksumAlgorithm", "ChecksumType")
SDK_OPTION_NAMES = {"walk": "walk", "prefix": "prefix", "delimiter": "delimiter", "page_size": "pageSize",
                    "start_after": "startAfter", "encoding_type": "encodingType", "fetch_owner": "fetchOwner"}
CREDENTIAL_ENV = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def sigv4_query(raw_query: str) -> str:
    """Canonical query: each name and value decoded once (plus stays literal), re-encoded per
    RFC 3986, sorted. A raw spelling the service canonicalizes differently answers 403."""
    pairs = []
    for part in raw_query.split("&") if raw_query else []:
        name, _, value = part.partition("=")
        # Byte-preserving: an escape that is not valid UTF-8 is signed as those exact bytes.
        pairs.append((quote(unquote_to_bytes(name), safe="-_.~"), quote(unquote_to_bytes(value), safe="-_.~")))
    return "&".join(f"{name}={value}" for name, value in sorted(pairs))


class SigV4Authorizer:
    """Signs each request with session credentials; nothing it adds is ever recorded."""

    def __init__(self, credentials: dict[str, str], region: str, clock=lambda: datetime.now(timezone.utc)):
        if not all(credentials.get(key) for key in CREDENTIAL_ENV[:2]):
            raise ValueError("S3 credentials are incomplete")
        self.credentials = credentials
        self.region = region
        self.clock = clock
        self.lock = threading.Lock()

    def headers(self, phase: str, request: "Request") -> list[tuple[str, str]]:
        now = self.clock()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        payload = hashlib.sha256(request.body).hexdigest()
        token = self.credentials.get("AWS_SESSION_TOKEN")
        signed = {"host": request.host, "x-amz-content-sha256": payload, "x-amz-date": amz_date}
        if token:
            signed["x-amz-security-token"] = token
        for name, value in request.headers:
            if name.lower() in {"content-type", "if-none-match", "if-match"} or name.lower().startswith("x-amz-"):
                signed[name.lower()] = value
        names = sorted(signed)
        canonical = "\n".join([request.method, request.path, sigv4_query(request.query),
                               "".join(f"{name}:{signed[name].strip()}\n" for name in names),
                               ";".join(names), payload])
        scope = f"{amz_date[:8]}/{self.region}/{SERVICE}/aws4_request"
        to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope,
                             hashlib.sha256(canonical.encode("utf-8")).hexdigest()])
        key = ("AWS4" + self.credentials["AWS_SECRET_ACCESS_KEY"]).encode("utf-8")
        for part in (amz_date[:8], self.region, SERVICE, "aws4_request"):
            key = hmac.new(key, part.encode("utf-8"), hashlib.sha256).digest()
        signature = hmac.new(key, to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = (f"AWS4-HMAC-SHA256 Credential={self.credentials['AWS_ACCESS_KEY_ID']}/{scope}, "
                         f"SignedHeaders={';'.join(names)}, Signature={signature}")
        extra = [(name, signed[name]) for name in ("x-amz-content-sha256", "x-amz-date", "x-amz-security-token")
                 if name in signed]
        return extra + [("Authorization", authorization)]

    def credential_query(self) -> str:
        return ""


class S3Provider:
    name = "s3"
    api_versions = (API_VERSION,)

    def __init__(self, bucket: str, region: str, credentials: dict[str, str], account_id: str | None = None):
        if not bucket:
            raise ValueError("S3 bucket must be configured explicitly")
        self.bucket = bucket
        self.region = region
        self.credentials = credentials
        self.account_id = account_id
        self.host = f"s3.{region}.amazonaws.com"
        self.owner_ids: list[str] = []

    @classmethod
    def from_environment(cls) -> "S3Provider":
        bucket_file = os.environ.get("SWATH_CONFORMANCE_S3_BUCKET_FILE", "")
        bucket = Path(bucket_file).read_text(encoding="utf-8").strip() if bucket_file else \
            os.environ.get("SWATH_CONFORMANCE_S3_BUCKET", "")
        return cls(bucket, os.environ.get("SWATH_CONFORMANCE_S3_REGION", "us-east-1"),
                   {key: os.environ.get(key, "") for key in CREDENTIAL_ENV},
                   os.environ.get("SWATH_CONFORMANCE_S3_ACCOUNT_ID"))

    # Identity and policy ------------------------------------------------------------

    def redactions(self) -> dict[str, str]:
        mapping = {self.bucket: "{bucket}"}
        for key, placeholder in zip(CREDENTIAL_ENV, ("{access-key}", "{secret-key}", "{session-token}")):
            if self.credentials.get(key):
                mapping[self.credentials[key]] = placeholder
        if self.account_id:
            mapping[self.account_id] = "{account-id}"
        return mapping

    def private_identifiers(self) -> list[str]:
        return [value for value in (self.bucket, self.account_id, *self.credentials.values()) if value]

    def sanitize_names(self) -> dict[str, str]:
        return {"bucket": "{bucket}"}

    def authorizer(self, client: HttpClient):
        return SigV4Authorizer(self.credentials, self.region)

    def version_echoed(self, version: str, response: Response) -> bool:
        return True

    def version_unavailable(self, response: Response) -> bool:
        return False

    def prices(self) -> PriceTable:
        # S3 Standard, us-east-1: PUT/COPY/POST/LIST $0.005 per 1,000 requests, GET $0.0004,
        # DELETE free; storage $0.023/GB-month. Egress is charged at $0.09/GiB (first paid
        # internet tier) for every permitted response byte, ignoring the free allowance.
        return PriceTable("https://aws.amazon.com/s3/pricing/", "2026-10-21", 0.005 / 1000, 0.023, 0.09)

    def manifest_fields(self) -> dict:
        return {"namespace_mode": "general-purpose-flat", "region": self.region, "api_version": API_VERSION,
                "sdk_product": SDK_PRODUCT, "sdk_version": SDK_VERSION, "storage_class": "STANDARD",
                "bucket_policy": {"object_ownership": "BucketOwnerEnforced", "block_public_access": "all",
                                  "versioning": "never-enabled", "lifecycle_expiration_days": 7},
                "addressing": "path-style regional endpoint",
                "native_probe_client": "python http.client with SigV4, no SDK, no Accept-Encoding"}

    def canonical_name(self, name: str) -> str:
        return name

    def scrub(self, body: bytes) -> bytes:
        """Owner identifiers and any echoed ARN or access key, before a raw capture is written."""
        body = re.sub(rb"(<(?:[A-Za-z0-9]+:)?ID>)[^<]*(</(?:[A-Za-z0-9]+:)?ID>)", rb"\1{owner-id}\2", body)
        body = re.sub(rb"(<(?:[A-Za-z0-9]+:)?DisplayName>)[^<]*(</(?:[A-Za-z0-9]+:)?DisplayName>)",
                      rb"\1{owner-name}\2", body)
        body = re.sub(rb"arn:aws[a-z-]*:[^<\s\"']*", b"{redacted-arn}", body)
        return re.sub(rb"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b", b"{access-key}", body)

    def raw_wire(self, raw: dict, manifest_sha256: str, probe_id: str, capture_sha256: str) -> bytes | None:
        return None  # S3 sanitization keeps the XML wire bytes apart from namespace fields.

    # Requests -----------------------------------------------------------------------

    def _object_path(self, item_name: str, wire_name: str | None = None) -> str:
        return f"/{self.bucket}/" + (wire_name if wire_name is not None else enc(item_name, safe="/-_.~"))

    def upload(self, item: CorpusObject) -> Request:
        return Request("PUT", self.host, self._object_path(item.name, item.wire_name), "",
                       (("Content-Type", "application/octet-stream"), ("If-None-Match", "*")), item.payload)

    def created(self, response: Response) -> bool:
        return response.status == 200

    def delete(self, name: str, version: str | None) -> Request:
        # Unversioned bucket inside a claimed, provably empty prefix; DeleteObject conditions
        # are not relied on, so deletes are unconditional.
        return Request("DELETE", self.host, self._object_path(name))

    def deleted(self, response: Response) -> bool:
        return response.status == 204

    def list_request(self, query: str, version: str | None, headers: tuple = ()) -> Request:
        return Request("GET", self.host, f"/{self.bucket}", query, tuple(headers))

    def with_token(self, query: str, token: str) -> str:
        return query + ("&" if query else "") + "continuation-token=" + enc(token, safe="")

    def page(self, body: bytes) -> tuple[list[Listed], str | None]:
        root = ElementTree.fromstring(body)
        if root.tag != f"{NS}ListBucketResult":
            raise ValueError("S3 listing has an unexpected root or namespace")
        truncated = root.findtext(f"{NS}IsTruncated")
        if truncated not in ("true", "false") or \
                (truncated == "true") != bool(root.findtext(f"{NS}NextContinuationToken")):
            raise ValueError("S3 listing truncation and continuation disagree")
        url = root.findtext(f"{NS}EncodingType") == "url"

        def text(value: str | None) -> str:
            # Form encoding: a literal plus is %2B, so unquote_plus is exact either way.
            return unquote_plus(value or "", errors="strict") if url else (value or "")

        entries = [Listed(text(node.findtext(f"{NS}Key")), size=int(node.findtext(f"{NS}Size")),
                          updated=node.findtext(f"{NS}LastModified"), version=node.findtext(f"{NS}ETag"),
                          attributes=tuple((field, node.findtext(f"{NS}{field}"))
                                           for field in FIXTURE_ATTRIBUTES if node.find(f"{NS}{field}") is not None))
                   for node in root.findall(f"{NS}Contents")]
        entries += [Listed(text(node.findtext(f"{NS}Prefix")), prefix=True)
                    for node in root.findall(f"{NS}CommonPrefixes")]
        return entries, root.findtext(f"{NS}NextContinuationToken") or None

    def inventory_query(self, prefix: str) -> str:
        # Encoded keys: S3 accepts keys that XML 1.0 cannot carry unencoded.
        return LIST_V2 + "&encoding-type=url" + ("&" + qs(("prefix", prefix)) if prefix else "")

    def sdk_options(self, case: Case, version: str | None) -> dict:
        options = {"provider": "s3", "credentials": "env", "region": self.region}
        # The live endpoint is pinned so no shared profile can redirect signed requests.
        for source, target in SDK_OPTION_NAMES.items():
            if case.sdk.get(source) is not None:
                options[target] = case.sdk[source]
        return options

    def sdk_identity(self) -> dict[str, str]:
        return {"product": SDK_PRODUCT, "version": SDK_VERSION}

    def sdk_environment(self) -> dict[str, str]:
        return {**{key: self.credentials[key] for key in CREDENTIAL_ENV},
                "SWATH_CAPTURE_IDENTITY": json.dumps({"bucket": self.bucket, "endpoint": f"https://{self.host}"}),
                # No shared AWS configuration or credentials files are read by the driver.
                "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
                "SWATH_CAPTURE_REDACT": json.dumps(self.redactions())}

    # Corpus -------------------------------------------------------------------------

    def corpus(self, run_prefix: str, profile: str) -> list[CorpusObject]:
        R = run_prefix
        groups = common_groups()
        if profile == "smoke":
            return corpus_objects(R, groups[:1])
        groups[3][1].append(("p/bs\\x", True))
        names: list[tuple] = [("n/" + n, True) for n in (
            "end.", "dots..", "trail/", "nbsp x", "zwj‍x", "xml<&>x", "/".join(["seg"] * 12))]
        # Control and XML-1.0-invalid characters: S3 accepts them in keys, so listings without
        # encoding-type=url may not be well-formed XML. Acceptance is part of the observation.
        names += [("n/" + n, None) for n in ("tab\tx", "ctl\x01x", "del\x7fx", "ls x", "bom﻿x",
                                             "fffe￾", "ffff￿", "cr\rx", "lf\nx", "nul\x00x",
                                             "./x", "../x")]
        long_name = "n/L"
        long_name += "x" * (MAX_NAME_BYTES - len((R + long_name).encode("utf-8")))
        names.append((long_name, True))
        groups.append(("n", names))
        groups.append(("b", [(f"b/{i:04d}", True) for i in range(PAGE_CAPACITY)] + [("b/zzzz", True)]))
        over = "q/over"
        over += "x" * (MAX_NAME_BYTES + 1 - len((R + over).encode("utf-8")))
        groups.append(("q", [(over, False), ("q/invalid-utf8", None, "%FF"),
                             ("q/surrogate", None, "%ED%A0%80"), ("q/low-surrogate", None, "%ED%B0%80")]))
        return corpus_objects(R, groups)

    # Case matrix --------------------------------------------------------------------

    def untested(self) -> list[Untested]:
        return [
            Untested("Bucket-not-found, access-denied and wrong-region errors",
                     "they would address another bucket or region; the campaign touches only its own bucket"),
            Untested("Unknown and percent-encoded query parameter names",
                     "the capture sanitizer matches a literal allowlist so a credential cannot be retained"),
            Untested("Raw query spellings SigV4 canonicalizes differently from the service",
                     "such a request is answered 403 SignatureDoesNotMatch before listing; the 403 is "
                     "recorded, but the listing behavior behind it cannot be observed with signed requests"),
            Untested("Versioned listings, directory buckets and requester-pays",
                     "the bucket is a general-purpose bucket with versioning never enabled"),
            Untested("Retryable native 5xx and SDK retry counts",
                     "a provider 5xx cannot be induced on demand within the bounded campaign"),
            Untested("Conditional DeleteObject", "cleanup deletes unconditionally inside a claimed empty prefix"),
        ]

    def cases(self, run_prefix: str, profile: str) -> list[Case]:
        R = run_prefix
        cases: list[Case] = []

        def add(case_id: str, title: str, question: str, kind: str, query: str = "", smoke: bool = False,
                v1: bool = False, **options) -> None:
            selector = "" if v1 else LIST_V2
            full = "&".join(part for part in (selector, query) if part)
            cases.append(Case(case_id, title, question, kind, full, smoke=smoke, **options))

        r, x, u, p, d, s, b, n = (R + g for g in ("r/", "x/", "u/", "p/", "d/", "s/", "b/", "n/"))
        # start-after.
        add("s3-01a", "`start-after` equal to an existing key is exclusive", "exclusive lower bound", "exchange",
            qs(("prefix", r), ("start-after", r + "k02")), True)
        add("s3-01b", "`start-after` in a gap between keys", "seek to the next key", "exchange",
            qs(("prefix", r), ("start-after", r + "k04")))
        add("s3-01c", "`start-after` before the prefix", "prefix wins", "exchange",
            qs(("prefix", r), ("start-after", R)))
        add("s3-01d", "`start-after` after the prefix", "empty result", "exchange",
            qs(("prefix", r), ("start-after", R + "s")))
        add("s3-01e", "`start-after` equal to the prefix", "prefix-equal lower bound", "exchange",
            qs(("prefix", r), ("start-after", r)))
        add("s3-01f", "`start-after` without a prefix", "bucket-wide lower bound", "exchange",
            qs(("start-after", r + "k06"), ("max-keys", "3")))
        add("s3-01g", "Empty `start-after` value", "empty value means absent", "exchange",
            "prefix=" + enc(r) + "&start-after=")
        add("s3-02a", "Prefix that is a string prefix of longer keys", "byte prefix, not path segment", "exchange",
            qs(("prefix", x + "a")))
        add("s3-02b", "Prefix with no matching keys", "empty result shape and KeyCount 0", "exchange",
            qs(("prefix", R + "none/")))
        # Delimiters and start-after.
        add("s3-03a", "Delimiter `/` baseline over keys, subtrees, a folder marker and an empty segment",
            "Contents and CommonPrefixes membership and KeyCount", "exchange", qs(("prefix", d), ("delimiter", "/")))
        for suffix, title, question, start in (
                ("b", "Delimiter with `start-after` equal to a subtree prefix",
                 "prefix equal to start-after is suppressed or emitted", d + "a/"),
                ("c", "Delimiter with `start-after` inside a subtree with remaining descendants",
                 "prefix emitted after a mid-subtree start-after", d + "a/m"),
                ("d", "Delimiter with `start-after` inside a subtree after its last descendant",
                 "prefix only for remaining descendants", d + "b/3"),
                ("e", "Delimiter with `start-after` after a subtree, equal to a key", "earlier subtree excluded",
                 d + "a0")):
            add("s3-03" + suffix, title, question, "exchange",
                qs(("prefix", d), ("delimiter", "/"), ("start-after", start)))
        add("s3-03f", "Delimiter `/` with a prefix ending in the delimiter", "folder marker equal to the prefix",
            "exchange", qs(("prefix", d + "a/"), ("delimiter", "/")))
        add("s3-03g", "Delimiter `/` with a prefix that stops before the delimiter", "rollup after the prefix",
            "exchange", qs(("prefix", d + "a"), ("delimiter", "/")))
        add("s3-03h", "Pipe delimiter that never occurs in keys", "flat result", "exchange",
            qs(("prefix", d), ("delimiter", "|")))
        add("s3-03i", "Two-character delimiter `/1`", "multi-character rollup and prefix equal to a key", "exchange",
            qs(("prefix", d), ("delimiter", "/1")))
        add("s3-03j", "Two-character delimiter `-g`", "multi-character rollup inside a segment", "exchange",
            qs(("prefix", d), ("delimiter", "-g")))
        add("s3-03k", "Empty `delimiter` value", "empty value means absent", "exchange",
            "prefix=" + enc(d) + "&delimiter=")
        add("s3-03l", "Delimiter without a prefix from a mid-corpus `start-after`", "top-level rollup with an offset",
            "exchange", qs(("delimiter", "/"), ("start-after", d)))
        # Unicode ordering.
        add("s3-04a", "BMP, surrogate-neighbor and supplementary keys in list order",
            "unsigned UTF-8 order versus UTF-16 order", "exchange", qs(("prefix", u)))
        add("s3-04b", "`start-after` at U+E000", "offset comparison order", "exchange",
            qs(("prefix", u), ("start-after", u + "")))
        add("s3-04c", "Prefix in NFC form with an NFD sibling", "no Unicode normalization", "exchange",
            qs(("prefix", u + "é")))
        add("s3-04d", "Prefix differing only in case", "case-sensitive keys", "exchange", qs(("prefix", u + "E")))
        add("s3-04e", "Unicode keys with `encoding-type=url`", "encoded Key spelling for multibyte keys", "exchange",
            qs(("prefix", u), ("encoding-type", "url")))
        # Query spelling.
        for suffix, title, question, query in spelling_probes("prefix", p):
            add("s3-05" + suffix, title, question, "exchange", query)
        add("s3-05r", "Raw `+` in a `start-after` value", "plus as literal versus space", "exchange",
            "prefix=" + enc(p) + "&start-after=" + enc(p) + "a+b")
        # max-keys.
        for suffix, value, question in (("a", "0", "zero: empty page and IsTruncated"), ("f", "-1", "negative"),
                                        ("g", "abc", "non-numeric"), ("h", "2147483648", "above signed 32-bit"),
                                        ("i", "9223372036854775808", "above signed 64-bit"),
                                        ("j", "1.5", "fractional"), ("l", "002", "leading zeros"),
                                        ("o", "2147483647", "signed 32-bit maximum")):
            add("s3-06" + suffix, f"`max-keys={value}`", question, "exchange",
                qs(("prefix", s), ("max-keys", value)))
        add("s3-06b", "`max-keys=0` over 1,001 keys", "IsTruncated and continuation at zero", "exchange",
            qs(("prefix", b), ("max-keys", "0")))
        add("s3-06c", "`max-keys=1` walk", "page size one", "walk", qs(("prefix", s), ("max-keys", "1")))
        add("s3-06d", "`max-keys=1000` walk over exactly 1,000 keys",
            "exact page multiple: IsTruncated on the boundary", "walk", qs(("prefix", b + "0"), ("max-keys", "1000")))
        add("s3-06e", "`max-keys=1001` over 1,001 keys", "cap at 1,000 versus 1,001", "exchange",
            qs(("prefix", b), ("max-keys", "1001")))
        add("s3-06k", "`max-keys=+2` with a raw plus", "sign or space before digits", "exchange",
            "prefix=" + enc(s) + "&max-keys=+2")
        add("s3-06m", "Duplicate `max-keys` values", "first, last or 400", "exchange",
            "prefix=" + enc(s) + "&max-keys=1&max-keys=2")
        add("s3-06n", "Empty `max-keys` value", "empty means default versus error", "exchange",
            "prefix=" + enc(s) + "&max-keys=")
        # Walks and continuation tokens.
        add("s3-07a", "Delimiter walk with one entry per page over top-level subtrees",
            "CommonPrefixes re-emission across pages over large collapsed subtrees", "walk",
            qs(("prefix", R), ("delimiter", "/"), ("max-keys", "1")))
        add("s3-07b", "Default page size walk over 1,001 keys", "default 1,000 and a short final page", "walk",
            qs(("prefix", b)))
        add("s3-07c", "Delimiter walk with two entries per page", "pages ending on a prefix boundary and KeyCount",
            "walk", qs(("prefix", d), ("delimiter", "/"), ("max-keys", "2")))
        add("s3-07d", "Delimiter walk with `start-after` and one entry per page",
            "start-after retained or ignored on continuation", "walk",
            qs(("prefix", d), ("delimiter", "/"), ("start-after", d + "a/m"), ("max-keys", "1")))
        add("s3-07e", "Flat walk with four entries per page over six keys", "short final page", "walk",
            qs(("prefix", r), ("max-keys", "4")), True)
        add("s3-07n", "Flat walk with two entries per page over delimited keys", "token source for a delimiter",
            "walk", qs(("prefix", d), ("max-keys", "2")))
        add("s3-07q", "Flat walk with two entries per page over six keys",
            "exact small page multiple: IsTruncated on the last full page", "walk",
            qs(("prefix", r), ("max-keys", "2")), True)
        same = qs(("prefix", r), ("max-keys", "2"))
        add("s3-07g", "Malformed constant `continuation-token`", "malformed token error", "token", same,
            token=TokenSource(mutation="tampered"))
        add("s3-07h", "Reused `continuation-token` for the same query", "tokens are repeatable", "token", same,
            token=TokenSource("s3-07q", 0), smoke=True)
        add("s3-07i", "`continuation-token` from a different prefix", "token bound to query", "token",
            qs(("prefix", x), ("max-keys", "2")), token=TokenSource("s3-07q", 0))
        add("s3-07j", "`continuation-token` with a changed page size", "page size may change", "token",
            qs(("prefix", r), ("max-keys", "3")), token=TokenSource("s3-07q", 0))
        add("s3-07k", "`continuation-token` from a flat walk with an added delimiter", "token bound to delimiter",
            "token", qs(("prefix", d), ("delimiter", "/"), ("max-keys", "2")), token=TokenSource("s3-07n", 0))
        add("s3-07o", "`continuation-token` from a delimiter walk with the delimiter removed",
            "token bound to delimiter", "token", qs(("prefix", d), ("max-keys", "2")), token=TokenSource("s3-07c", 0))
        add("s3-07r", "Issued `continuation-token` with its last character changed", "token integrity check",
            "token", same, token=TokenSource("s3-07q", 0, "flip"))
        add("s3-07s", "Issued `continuation-token` truncated by four characters", "token integrity check", "token",
            same, token=TokenSource("s3-07q", 0, "truncate"))
        add("s3-07x", "Issued `continuation-token` with one decoded byte changed and re-encoded",
            "integrity rejection versus malformed encoding", "token", same,
            token=TokenSource("s3-07q", 0, "flip-decoded"))
        add("s3-07t", "`continuation-token` with a prefix matching only keys before it",
            "empty-result continuation", "token", qs(("prefix", r + "k01"), ("max-keys", "2")),
            token=TokenSource("s3-07q", 0))
        add("s3-07u", "`continuation-token` with a `start-after` before the token position",
            "token versus start-after precedence", "token",
            qs(("prefix", r), ("max-keys", "2"), ("start-after", r + "k01")), token=TokenSource("s3-07q", 0))
        add("s3-07v", "`continuation-token` with a `start-after` after the token position",
            "token versus start-after precedence", "token",
            qs(("prefix", r), ("max-keys", "2"), ("start-after", r + "k06")), token=TokenSource("s3-07q", 0))
        add("s3-07w", "Empty `continuation-token` value", "empty token means first page or error", "exchange",
            "prefix=" + enc(r) + "&max-keys=2&continuation-token=")
        # encoding-type=url and XML-invalid keys.
        add("s3-08a", "Keys with control, XML-invalid, whitespace and dot characters without encoding",
            "raw XML text for keys XML 1.0 cannot carry", "exchange", qs(("prefix", n)))
        add("s3-08b", "The same keys with `encoding-type=url`", "which keys and elements are URL-encoded",
            "exchange", qs(("prefix", n), ("encoding-type", "url")))
        add("s3-08c", "Delimiter listing with `encoding-type=url` and special-character prefix and start-after",
            "encoding of Prefix, Delimiter, StartAfter and CommonPrefixes, not the token", "walk",
            qs(("prefix", p), ("delimiter", " "), ("start-after", p + "a b"), ("encoding-type", "url"),
               ("max-keys", "2")))
        add("s3-08d", "Unknown `encoding-type` value", "400 InvalidArgument shape", "exchange",
            qs(("prefix", s), ("encoding-type", "bogus")))
        add("s3-08e", "Reserved-character keys with `encoding-type=url`", "encoded spelling of reserved characters",
            "exchange", qs(("prefix", p), ("encoding-type", "url")))
        add("s3-08f", "Creation-probe keys at and over length limits", "which limit keys exist", "exchange",
            qs(("prefix", R + "q/"), ("encoding-type", "url")))
        # Official SDK paginator.
        add("s3-09a", "Java SDK v2 ListObjectsV2 paginator over Unicode keys", "SDK request shape and tokens",
            "sdk_run", sdk={"walk": "flat", "prefix": u, "page_size": 5})
        add("s3-09b", "Java SDK v2 paginator with a delimiter", "CommonPrefixes paging", "sdk_run",
            sdk={"walk": "hierarchy", "prefix": d, "delimiter": "/", "page_size": 2})
        add("s3-09c", "Java SDK v2 paginator with `startAfter`", "start-after on every page", "sdk_run",
            sdk={"walk": "flat", "prefix": r, "start_after": r + "k02", "page_size": 2}, smoke=True)
        # Owner and optional attributes.
        add("s3-11a", "`fetch-owner=true`", "Owner element shape under BucketOwnerEnforced", "exchange",
            qs(("prefix", s), ("fetch-owner", "true")))
        add("s3-11b", "`fetch-owner=false`", "named default", "exchange", qs(("prefix", s), ("fetch-owner", "false")))
        add("s3-11c", "`fetch-owner=yes`", "non-boolean value", "exchange", qs(("prefix", s), ("fetch-owner", "yes")))
        add("s3-11d", "`x-amz-optional-object-attributes: RestoreStatus` header",
            "optional attribute replay does not serve", "exchange", qs(("prefix", s)),
            headers=(("x-amz-optional-object-attributes", "RestoreStatus"),))
        # Version and selector discrimination.
        add("s3-12a", "ListObjects V1 with `marker` equal to an existing key", "marker matches start-after",
            "exchange", qs(("prefix", r), ("marker", r + "k02")), v1=True)
        add("s3-12b", "ListObjects V1 delimiter page with `max-keys=1`", "NextMarker only with a delimiter",
            "exchange", qs(("prefix", d), ("delimiter", "/"), ("max-keys", "1")), v1=True)
        add("s3-12c", "`list-type=3`", "unknown list type", "exchange", "list-type=3&" + qs(("prefix", s)), v1=True)
        add("s3-12d", "Duplicate `list-type`", "duplicate selector", "exchange",
            "list-type=2&list-type=2&" + qs(("prefix", s)), v1=True)
        add("s3-12e", "V2 with `marker`", "V1 parameter on V2", "exchange", qs(("prefix", r), ("marker", r + "k02")))
        if profile == "smoke":
            return [case for case in cases if case.smoke]
        return cases
