# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Azure Blob XML List Blobs adapter, corpus and case matrix for campaign.py.

Every listing case runs once per service version, and each version is evidence-gated
separately. The container-scoped SAS is appended only at send time; it is never part of
a recorded URL. Requests use a flat (hierarchical namespace off) StorageV2 account.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import parse_qsl, unquote
from xml.etree import ElementTree

from campaign import HttpClient, Listed, Request, Response
from matrix import (Case, CorpusObject, PriceTable, TokenSource, Untested, common_groups, corpus_objects,
                    enc, qs, spelling_probes)

VERSIONS = ("2026-06-06", "2026-10-06")
WRITE_VERSION = VERSIONS[0]
SDK_PRODUCT = "azsdk-java-azure-storage-blob"
SDK_VERSION = "12.35.1"
# The pinned SDK enumerates service versions up to this one.
SDK_VERSIONS = ("2026-06-06",)
PAGE_CAPACITY = 5000
MAX_NAME_CHARS = 1024
CLIENT_ID_PROBE = "probe-client-request-id"
UNSUPPORTED_VERSION_CODE = "InvalidHeaderValue"
LIST_SELECTOR = "restype=container&comp=list"
SDK_OPTION_NAMES = {"walk": "walk", "prefix": "prefix", "delimiter": "delimiter",
                    "page_size": "pageSize", "start_from": "startFrom"}
SAS_FIELDS = frozenset({"sv", "ss", "srt", "sp", "se", "st", "spr", "sig", "sr", "si", "sip",
                        "skoid", "sktid", "skt", "ske", "sks", "skv", "saoid", "suoid", "scid",
                        "sdd", "ses", "rscc", "rscd", "rsce", "rscl", "rsct"})


class SasAuthorizer:
    def __init__(self, sas: str):
        # A SAS copied with its leading `?` is accepted; the field check sees the query alone.
        sas = sas.removeprefix("?")
        fields = {name for name, _ in parse_qsl(sas, keep_blank_values=True)}
        if not sas or "sig" not in fields or not fields <= SAS_FIELDS:
            raise ValueError("Azure SAS must be a container SAS query string")
        self.sas = sas

    def headers(self, phase: str, request: "Request") -> list[tuple[str, str]]:
        return []

    def credential_query(self) -> str:
        return self.sas


class AzureProvider:
    name = "azure"
    api_versions = VERSIONS

    def __init__(self, account: str, container: str, sas: str, region: str):
        # The stripped SAS is what is sent, so it is also what is redacted. It is a substring of
        # the spelling with `?`, so redacting it covers both.
        sas = sas.removeprefix("?")
        if not account or not container or not sas:
            raise ValueError("Azure account, container and SAS must be configured explicitly")
        self.account = account
        self.container = container
        self.sas = sas
        self.region = region
        self.host = f"{account}.blob.core.windows.net"

    @classmethod
    def from_environment(cls) -> "AzureProvider":
        settings = dict(os.environ)
        env_file = Path(settings.get("SWATH_CONFORMANCE_AZURE_ENV", ""))
        if env_file.is_file():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                if "=" in line and not line.lstrip().startswith("#"):
                    key, value = line.split("=", 1)
                    settings.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return cls(settings.get("AZURE_STORAGE_ACCOUNT", ""), settings.get("AZURE_CONTAINER", ""),
                   settings.get("AZURE_CONTAINER_SAS", ""),
                   settings.get("SWATH_CONFORMANCE_AZURE_REGION", "eastus"))

    # Identity and policy ------------------------------------------------------------

    def _signature(self) -> list[str]:
        return [value for name, value in parse_qsl(self.sas) if name == "sig"]

    def redactions(self) -> dict[str, str]:
        mapping = {self.account: "{account}", self.sas: "{sas}"}
        mapping.update({sig: "{sas-signature}" for sig in self._signature()})
        return mapping

    def sanitize_names(self) -> dict[str, str]:
        # Raw captures carry the account placeholder; the container is replaced structurally.
        return {"account": "{account}", "container": self.container}

    def private_identifiers(self) -> list[str]:
        # The container name is a generic word replaced structurally by the sanitizer; the
        # account name and SAS are the private identifiers.
        return [self.account, self.sas, *self._signature()]

    def authorizer(self, client: HttpClient):
        return SasAuthorizer(self.sas)

    def version_echoed(self, version: str, response: Response) -> bool:
        return response.header("x-ms-version") == version

    def version_unavailable(self, response: Response) -> bool:
        return response.status == 400 and response.header("x-ms-error-code") == UNSUPPORTED_VERSION_CODE

    def prices(self) -> PriceTable:
        # Hot LRS, East US, General Block Blob v2 (Azure retail prices API, 2026-10-21):
        # write and list operations $0.05 per 10,000, read $0.004, delete free; data stored
        # $0.0208/GB-month. Egress is charged at $0.12/GiB, above the $0.087 first paid tier,
        # ignoring the monthly free allowance.
        return PriceTable("https://prices.azure.com/api/retail/prices (Storage, eastus, Hot LRS)",
                          "2026-10-21", 0.05 / 10_000, 0.0208, 0.12)

    def manifest_fields(self) -> dict:
        return {"namespace_mode": "flat-hns-off", "region": self.region, "api_version": WRITE_VERSION,
                "sdk_product": SDK_PRODUCT, "sdk_version": SDK_VERSION,
                "account_kind": "StorageV2", "replication": "Standard_LRS",
                "container_policy": {"public_access": "off", "soft_delete": "disabled",
                                     "versioning": "disabled", "lifecycle_delete_age_days": 7},
                "write_service_version": WRITE_VERSION,
                "native_probe_client": "python http.client, no SDK, no Accept or client request ID "
                                       "unless the case names one"}

    def canonical_name(self, name: str) -> str:
        # Blob Storage rewrites a backslash in the request path to a forward slash.
        return name.replace("\\", "/")

    def scrub(self, body: bytes) -> bytes:
        return body

    def raw_wire(self, raw: dict, manifest_sha256: str, probe_id: str,
                 capture_sha256: str) -> bytes | None:
        # Azure sanitization keeps the XML wire bytes apart from namespace attributes and
        # opaque markers, so the capture is its own raw-wire artifact.
        return None

    # Requests -----------------------------------------------------------------------

    def _blob_path(self, item_name: str, wire_name: str | None = None) -> str:
        return f"/{self.container}/" + (wire_name if wire_name is not None else enc(item_name, safe="/"))

    def upload(self, item: CorpusObject) -> Request:
        return Request("PUT", self.host, self._blob_path(item.name, item.wire_name), "",
                       (("x-ms-version", WRITE_VERSION), ("x-ms-blob-type", "BlockBlob"),
                        ("Content-Type", "application/octet-stream"), ("If-None-Match", "*")),
                       item.payload)

    def created(self, response: Response) -> bool:
        return response.status == 201

    def delete(self, name: str, version: str | None) -> Request | None:
        if not version:
            return None
        return Request("DELETE", self.host, self._blob_path(name), "",
                       (("x-ms-version", WRITE_VERSION), ("If-Match", version)))

    def deleted(self, response: Response) -> bool:
        return response.status == 202

    def list_request(self, query: str, version: str | None, headers: tuple = ()) -> Request:
        names = {name.lower() for name, _ in headers}
        base = () if "x-ms-version" in names else (("x-ms-version", version or WRITE_VERSION),)
        full = LIST_SELECTOR + ("&" + query if query else "")
        return Request("GET", self.host, f"/{self.container}", full, (*base, *headers))

    def with_token(self, query: str, token: str) -> str:
        return query + ("&" if query else "") + "marker=" + enc(token, safe="")

    def page(self, body: bytes) -> tuple[list[Listed], str | None]:
        root = ElementTree.fromstring(body)
        if root.tag != "EnumerationResults" or root.find("Blobs") is None:
            raise ValueError("Azure listing has an unexpected root")
        entries = []
        for node in root.find("Blobs") if root.find("Blobs") is not None else []:
            name_node = node.find("Name")
            name = name_node.text or ""
            if name_node.attrib.get("Encoded", "").lower() == "true":
                name = unquote(name, errors="strict")
            if node.tag == "BlobPrefix":
                entries.append(Listed(name, prefix=True))
            else:
                properties = node.find("Properties")
                entries.append(Listed(name, size=int(properties.findtext("Content-Length")),
                                      updated=properties.findtext("Last-Modified"),
                                      version=properties.findtext("Etag")))
        return entries, root.findtext("NextMarker") or None

    def inventory_query(self, prefix: str) -> str:
        return qs(("prefix", prefix)) if prefix else ""

    def sdk_options(self, case: Case, version: str | None) -> dict:
        options = {"provider": "azure", "credentials": "sas-env", "serviceVersion": version,
                   "clientRequestIdPrefix": f"probe-{case.id}-{version}"}
        for source, target in SDK_OPTION_NAMES.items():
            if case.sdk.get(source) is not None:
                options[target] = case.sdk[source]
        return options

    def sdk_identity(self) -> dict[str, str]:
        return {"product": SDK_PRODUCT, "version": SDK_VERSION}

    def sdk_environment(self) -> dict[str, str]:
        # Identity, redaction and the SAS reach the driver through its environment only.
        return {"AZURE_CONTAINER_SAS": self.sas,
                "SWATH_CAPTURE_IDENTITY": json.dumps({"endpoint": f"https://{self.host}",
                                                      "container": self.container}),
                "SWATH_CAPTURE_REDACT": json.dumps(self.redactions())}

    # Corpus -------------------------------------------------------------------------

    def corpus(self, run_prefix: str, profile: str) -> list[CorpusObject]:
        R = run_prefix
        groups = common_groups()
        if profile == "smoke":
            return corpus_objects(R, [groups[0], groups[5]])
        # The service rewrites backslash to slash in the path; its listed name is the observation.
        groups[3][1].append(("p/bs\\x", None))
        names: list[tuple] = [("n/" + n, True) for n in (
            "dots..x", "nbsp x", "zwj‍x", "xml<&>x", "sp x", "/".join(["seg"] * 12))]
        # Dot segments, trailing dots and slashes, control characters and XML-invalid
        # characters are exploratory: acceptance, rewriting and Encoded listing are observed.
        names += [("n/" + n, None) for n in (
            "end.", "./x", "../x", "trail/", "tab\tx", "ctl\x01x", "del\x7fx", "ls x",
            "bom﻿x", "fffe￾", "ffff￿", "nul\x00x", "cr\rx", "lf\nx")]
        long_name = "n/L"
        long_name += "x" * (MAX_NAME_CHARS - len(R + long_name))
        names.append((long_name, True))
        groups.append(("n", names))
        groups.append(("b", [(f"b/{i:05d}", True) for i in range(PAGE_CAPACITY)] + [("b/zzzzz", True)]))
        over = "q/over"
        over += "x" * (MAX_NAME_CHARS + 1 - len(R + over))

        def utf16_units(value: str) -> int:
            return len(value.encode("utf-16-le")) // 2

        # 1,024 UTF-16 code units made mostly of supplementary characters: fewer than
        # 1,024 code points, more than 1,024 UTF-8 bytes.
        surrogates = "q/su"
        while utf16_units(R + surrogates + "\U0001f600") <= MAX_NAME_CHARS:
            surrogates += "\U0001f600"
        surrogates += "x" * (MAX_NAME_CHARS - utf16_units(R + surrogates))
        segments_254 = "q/" + "/".join(["s"] * (254 - R.count("/") - 1))
        segments_255 = "q/t/" + "/".join(["s"] * (255 - R.count("/") - 2))
        groups.append(("q", [(over, False), (surrogates, None), (surrogates + "x", None),
                             (segments_254, None), (segments_255, None),
                             ("q/invalid-utf8", False, "%FF"), ("q/surrogate", None, "%ED%A0%80"),
                             ("q/surrogate-pair", None, "%ED%A0%BD%ED%B8%80"),
                             ("q/low-surrogate", None, "%ED%B0%80")]))
        return corpus_objects(R, groups)

    # Case matrix --------------------------------------------------------------------

    def untested(self) -> list[Untested]:
        return [
            Untested("Unsupported or missing `x-ms-version` error shapes",
                     "the capture sanitizer binds each exchange to one supported service version"),
            Untested("Container-not-found and account-level listing errors",
                     "the SAS is container-scoped; another container name returns an authorization "
                     "error rather than the listing error, and no other resource may be touched"),
            Untested("Get Container Properties (`restype=container` without `comp=list`)",
                     "not a listing response; the sanitizer requires an EnumerationResults or Error body"),
            Untested("Apache Arrow listing responses", "binary bodies are outside the XML capture profile; "
                     "the Arrow spike is tracked separately"),
            Untested("Percent-encoded query parameter names and unknown parameters",
                     "the capture sanitizer matches the literal allowlist so a credential cannot be retained"),
            Untested("Retryable native 5xx and SDK retry counts (azure-11)",
                     "a provider 5xx cannot be induced on demand within the bounded campaign"),
            Untested("Official SDK walks at 2026-10-06",
                     f"the pinned azure-storage-blob {SDK_VERSION} has no V2026_10_06 constant; "
                     "a newer pinned SDK is required"),
            Untested("Hierarchical-namespace accounts, snapshots, versions and soft-deleted blobs",
                     "the dedicated account is flat with versioning and soft delete disabled"),
        ]

    def cases(self, run_prefix: str, profile: str) -> list[Case]:
        R = run_prefix
        cases: list[Case] = []

        def add(case_id: str, title: str, discriminates: str, kind: str, query: str = "",
                versions: tuple[str, ...] = VERSIONS, smoke: bool = False, **options) -> None:
            cases.append(Case(case_id, title, discriminates, kind, query, versions=versions,
                              smoke=smoke, **options))

        r, x, u, p, d, s, b, n = (R + g for g in ("r/", "x/", "u/", "p/", "d/", "s/", "b/", "n/"))
        # Service versions.
        add("azure-01", "Account accepts service version and echoes it", "explicit version echo",
            "exchange", qs(("prefix", s)), versions=(VERSIONS[0],), smoke=True, version_gate=True)
        add("azure-02", "Account rollout and version echo", "2026-10-06 accepted or refused",
            "exchange", qs(("prefix", s)), versions=(VERSIONS[1],), smoke=True, version_gate=True)
        add("azure-13a", "Explicit client request ID echo", "x-ms-client-request-id echoed verbatim",
            "exchange", qs(("prefix", s)), headers=(("x-ms-client-request-id", CLIENT_ID_PROBE),))
        add("azure-13b", "Explicit `Accept: application/xml`", "explicit XML accept",
            "exchange", qs(("prefix", s)), headers=(("Accept", "application/xml"),))
        # startFrom and prefix.
        add("azure-03a", "`startFrom` equal to an existing name is inclusive", "inclusive lower bound",
            "exchange", qs(("prefix", r), ("startFrom", r + "k02")), smoke=True)
        add("azure-03b", "`startFrom` in a gap between names", "seek to the next name",
            "exchange", qs(("prefix", r), ("startFrom", r + "k04")))
        add("azure-03c", "`startFrom` before the prefix", "prefix wins", "exchange",
            qs(("prefix", r), ("startFrom", R)))
        add("azure-03d", "`startFrom` after the prefix", "empty result", "exchange",
            qs(("prefix", r), ("startFrom", R + "s")))
        add("azure-03e", "`startFrom` equal to the prefix", "prefix-equal lower bound", "exchange",
            qs(("prefix", r), ("startFrom", r)))
        add("azure-03f", "`startFrom` without a prefix", "container-wide lower bound", "exchange",
            qs(("startFrom", r + "k06"), ("maxresults", "3")))
        add("azure-03g", "Empty `startFrom` value", "empty value means absent versus error", "exchange",
            "prefix=" + enc(r) + "&startFrom=")
        add("azure-03h", "`endBefore`", "native exclusive upper bound that replay refuses", "exchange",
            qs(("prefix", r), ("endBefore", r + "k05")))
        add("azure-03i", "`startFrom` with `endBefore`", "both bounds", "exchange",
            qs(("prefix", r), ("startFrom", r + "k02"), ("endBefore", r + "k06")))
        add("azure-03j", "Prefix that is a string prefix of longer names", "byte prefix, not path segment",
            "exchange", qs(("prefix", x + "a")))
        add("azure-03k", "`startFrom` inside a prefix that is a string prefix", "intersection",
            "exchange", qs(("prefix", x + "a"), ("startFrom", x + "ab")))
        # Delimiter and startFrom.
        add("azure-04a", "Delimiter `/` baseline over objects, subtrees, a folder marker and an empty segment",
            "Blob and BlobPrefix interleaving", "exchange", qs(("prefix", d), ("delimiter", "/")))
        for suffix, title, question, start in (
                ("b", "Delimiter with `startFrom` equal to a subtree prefix", "prefix at its own boundary", d + "a/"),
                ("c", "Delimiter with `startFrom` inside a subtree with remaining descendants",
                 "prefix from a mid-subtree offset", d + "a/m"),
                ("d", "Delimiter with `startFrom` inside a subtree after its last descendant",
                 "prefix only for in-range descendants", d + "b/3"),
                ("e", "Delimiter with `startFrom` after a subtree, equal to an object", "earlier subtree excluded",
                 d + "a0")):
            add("azure-04" + suffix, title, question, "exchange",
                qs(("prefix", d), ("delimiter", "/"), ("startFrom", start)))
        add("azure-04f", "Delimiter `/` with a prefix ending in the delimiter", "folder marker equal to the prefix",
            "exchange", qs(("prefix", d + "a/"), ("delimiter", "/")))
        add("azure-04g", "Delimiter `/` with a prefix that stops before the delimiter", "rollup after the prefix",
            "exchange", qs(("prefix", d + "a"), ("delimiter", "/")))
        add("azure-04h", "Pipe delimiter that never occurs in names", "flat result", "exchange",
            qs(("prefix", d), ("delimiter", "|")))
        add("azure-04i", "Two-character delimiter `/1`", "multi-character rollup", "exchange",
            qs(("prefix", d), ("delimiter", "/1")))
        add("azure-04j", "Two-character delimiter `-g`", "multi-character rollup inside a segment", "exchange",
            qs(("prefix", d), ("delimiter", "-g")))
        add("azure-04k", "Empty `delimiter` value", "empty value means absent versus error", "exchange",
            "prefix=" + enc(d) + "&delimiter=")
        add("azure-04l", "Delimiter without a prefix from a mid-corpus `startFrom`", "top-level rollup with an offset",
            "exchange", qs(("delimiter", "/"), ("startFrom", d)))
        add("azure-04m", "Delimiter walk with `startFrom` inside a subtree", "startFrom retained across markers",
            "walk", qs(("prefix", d), ("delimiter", "/"), ("startFrom", d + "a/m"), ("maxresults", "1")))
        # marker and startFrom precedence.
        add("azure-05a", "Walk with `startFrom` and one entry per page", "startFrom retained on continuation",
            "walk", qs(("prefix", r), ("startFrom", r + "k02"), ("maxresults", "1")))
        add("azure-05b", "`marker` with a later `startFrom`", "later bound wins", "token",
            qs(("prefix", r), ("startFrom", r + "k06"), ("maxresults", "1")),
            token=TokenSource("azure-05a", 0))
        add("azure-05c", "`marker` with an earlier `startFrom`", "marker wins over an earlier startFrom", "token",
            qs(("prefix", r), ("startFrom", r + "k01"), ("maxresults", "2")),
            token=TokenSource("azure-09q", 0))
        add("azure-05d", "`marker` issued under `startFrom` used without it", "marker is self-contained", "token",
            qs(("prefix", r), ("maxresults", "1")), token=TokenSource("azure-05a", 0))
        # Unicode ordering.
        add("azure-06a", "BMP, surrogate-neighbor and supplementary names in list order",
            "unsigned UTF-8 order versus UTF-16 order", "exchange", qs(("prefix", u)))
        add("azure-06b", "`startFrom` at U+E000", "offset comparison order", "exchange",
            qs(("prefix", u), ("startFrom", u + "")))
        add("azure-06c", "Prefix in NFC form with an NFD sibling", "no Unicode normalization", "exchange",
            qs(("prefix", u + "é")))
        add("azure-06d", "Prefix differing only in case", "case-sensitive names", "exchange",
            qs(("prefix", u + "E")))
        add("azure-06e", "Supplementary-character `startFrom` in lowercase hex escapes", "escape case",
            "exchange", "prefix=" + enc(u) + "&startFrom=" + enc(u) + "%f0%9f%98%80")
        add("azure-06f", "Delimiter `é` over Unicode names", "multibyte delimiter", "exchange",
            qs(("prefix", u), ("delimiter", "é")))
        # Query spelling and encoded names.
        for suffix, title, question, query in spelling_probes("prefix", p):
            add("azure-07" + suffix, title, question, "exchange", query)
        add("azure-07r", "Names with control, XML-invalid, whitespace and dot characters",
            "Encoded attribute and exact Name text", "exchange", qs(("prefix", n)))
        add("azure-07s", "Delimiter listing over dot segments and trailing-slash names",
            "dot segments and BlobPrefix encoding", "exchange", qs(("prefix", n), ("delimiter", "/")))
        add("azure-07t", "Raw `+` in a `startFrom` value", "plus as literal versus space", "exchange",
            "prefix=" + enc(p) + "&startFrom=" + enc(p) + "a+b")
        # maxresults.
        for suffix, value, question in (("a", "0", "zero refused versus default"), ("f", "-1", "negative"),
                                        ("g", "abc", "non-numeric"), ("h", "2147483648", "above signed 32-bit"),
                                        ("i", "9223372036854775808", "above signed 64-bit"),
                                        ("j", "1.5", "fractional"), ("l", "002", "leading zeros"),
                                        ("o", "2147483647", "signed 32-bit maximum")):
            add("azure-08" + suffix, f"`maxresults={value}`", question, "exchange",
                qs(("prefix", s), ("maxresults", value)))
        add("azure-08k", "`maxresults=+2` with a raw plus", "sign or space before digits", "exchange",
            "prefix=" + enc(s) + "&maxresults=+2")
        add("azure-08m", "Duplicate `maxresults` values", "first, last or 400", "exchange",
            "prefix=" + enc(s) + "&maxresults=1&maxresults=2")
        add("azure-08n", "Empty `maxresults` value", "empty means default versus error", "exchange",
            "prefix=" + enc(s) + "&maxresults=")
        add("azure-08c", "`maxresults=1` walk", "page size one", "walk", qs(("prefix", s), ("maxresults", "1")))
        add("azure-08d", "`maxresults=5000` walk over exactly 5,000 blobs",
            "exact page multiple: terminal page versus trailing empty page", "walk",
            qs(("prefix", b + "0"), ("maxresults", "5000")))
        add("azure-08e", "`maxresults=5001` walk over 5,001 blobs", "cap at 5,000 versus 400", "walk",
            qs(("prefix", b), ("maxresults", "5001")))
        add("azure-08p", "Duplicate `restype` selector", "duplicate selector", "exchange",
            "restype=container&prefix=" + enc(s))
        # Walks and markers.
        add("azure-09a", "Delimiter walk with one entry per page over top-level subtrees",
            "BlobPrefix repetition across pages over large collapsed subtrees", "walk",
            qs(("prefix", R), ("delimiter", "/"), ("maxresults", "1")))
        add("azure-09b", "Default page size walk over 5,001 blobs", "default 5,000 and a short final page", "walk",
            qs(("prefix", b)))
        add("azure-09c", "Delimiter walk with two entries per page", "pages ending on a prefix boundary", "walk",
            qs(("prefix", d), ("delimiter", "/"), ("maxresults", "2")))
        add("azure-09e", "Flat walk with four entries per page over six names", "short final page", "walk",
            qs(("prefix", r), ("maxresults", "4")), smoke=True)
        add("azure-09n", "Flat walk with two entries per page over delimited names",
            "marker source for adding a delimiter", "walk", qs(("prefix", d), ("maxresults", "2")))
        add("azure-09q", "Flat walk with two entries per page over six names",
            "exact small page multiple: terminal page versus trailing empty page", "walk",
            qs(("prefix", r), ("maxresults", "2")), smoke=True)
        add("azure-09f", "Prefix with no matching names", "empty Blobs element shape", "exchange",
            qs(("prefix", R + "none/")))
        same = qs(("prefix", r), ("maxresults", "2"))
        add("azure-09g", "Malformed constant `marker`", "malformed marker error", "token", same,
            token=TokenSource(mutation="tampered"))
        add("azure-09h", "Reused `marker` for the same query", "markers are repeatable", "token", same,
            token=TokenSource("azure-09q", 0), smoke=True)
        add("azure-09i", "`marker` from a different prefix", "marker bound to query versus position", "token",
            qs(("prefix", x), ("maxresults", "2")), token=TokenSource("azure-09q", 0))
        add("azure-09j", "`marker` with a changed page size", "page size may change", "token",
            qs(("prefix", r), ("maxresults", "3")), token=TokenSource("azure-09q", 0))
        add("azure-09k", "`marker` from a flat walk with an added delimiter", "marker bound to delimiter", "token",
            qs(("prefix", d), ("delimiter", "/"), ("maxresults", "2")), token=TokenSource("azure-09n", 0))
        add("azure-09o", "`marker` from a delimiter walk with the delimiter removed", "marker bound to delimiter",
            "token", qs(("prefix", d), ("maxresults", "2")), token=TokenSource("azure-09c", 0))
        add("azure-09r", "Issued `marker` with its last character changed", "marker integrity check", "token",
            same, token=TokenSource("azure-09q", 0, "flip"))
        add("azure-09s", "Issued `marker` truncated by four characters", "marker integrity check", "token",
            same, token=TokenSource("azure-09q", 0, "truncate"))
        add("azure-09v", "Issued `marker` with one decoded byte changed and re-encoded",
            "integrity rejection versus malformed encoding", "token", same,
            token=TokenSource("azure-09q", 0, "flip-decoded"))
        add("azure-09t", "`marker` with a prefix matching only names before it",
            "empty-result continuation: entries and NextMarker", "token",
            qs(("prefix", r + "k01"), ("maxresults", "2")), token=TokenSource("azure-09q", 0))
        add("azure-09u", "`marker` with an `endBefore` excluding every remaining name",
            "empty-result continuation under an upper bound", "token",
            qs(("prefix", r), ("maxresults", "2"), ("endBefore", r + "k02")), token=TokenSource("azure-09q", 0))
        add("azure-09l", "Empty `marker` value", "empty marker means first page", "exchange",
            "prefix=" + enc(r) + "&maxresults=2&marker=")
        # Official SDK walks; the pinned SDK supports only the first version.
        add("azure-10a", "Java SDK flat auto-pagination", "SDK request shape and marker handling", "sdk_run",
            versions=SDK_VERSIONS, sdk={"walk": "flat", "prefix": u, "page_size": 5})
        add("azure-10b", "Java SDK hierarchy auto-pagination", "listBlobsByHierarchy request shape", "sdk_run",
            versions=SDK_VERSIONS, sdk={"walk": "hierarchy", "prefix": d, "delimiter": "/", "page_size": 2})
        add("azure-10c", "Java SDK `setStartFrom` auto-pagination", "startFrom on every page", "sdk_run",
            versions=SDK_VERSIONS, smoke=True,
            sdk={"walk": "flat", "prefix": r, "start_from": r + "k02", "page_size": 2})
        # Names at length and segment limits.
        add("azure-12a", "Listing of length and segment creation-probe names", "which limit names exist",
            "exchange", qs(("prefix", R + "q/")))
        # Optional listing parameters.
        add("azure-14a", "`include=metadata`", "native expansion replay refuses", "exchange",
            qs(("prefix", s), ("include", "metadata")))
        add("azure-14b", "Empty `include` value", "empty include equals omitted", "exchange",
            "prefix=" + enc(s) + "&include=")
        add("azure-14c", "`include=deleted` with soft delete disabled", "native refusal or empty", "exchange",
            qs(("prefix", s), ("include", "deleted")))
        add("azure-14d", "`timeout=1`", "server timeout parameter accepted", "exchange",
            qs(("prefix", s), ("timeout", "1")))
        add("azure-14e", "`timeout=0`", "zero timeout", "exchange", qs(("prefix", s), ("timeout", "0")))
        add("azure-14f", "`timeout=abc`", "non-numeric timeout", "exchange", qs(("prefix", s), ("timeout", "abc")))
        add("azure-14g", "`showonly=files` on a flat account", "hierarchical-only filter", "exchange",
            qs(("prefix", s), ("showonly", "files")))
        if profile == "smoke":
            return [case for case in cases if case.smoke]
        return cases
