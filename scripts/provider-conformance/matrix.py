# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Case-matrix vocabulary shared by every live provider adapter.

A case is one ledger row: one request shape whose native answer settles one question. The
shared corpus groups and query-spelling family keep providers comparable; each adapter adds
its own groups (page-capacity bulk, name limits) and provider-specific cases.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import quote

CASE_KINDS = frozenset({"exchange", "walk", "token", "sdk_run"})
# Derived-token probes. `tampered` replaces the token with a malformed constant; `flip` and
# `truncate` alter a freshly issued token while keeping its outer encoding valid.
TOKEN_MUTATIONS = frozenset({"tampered", "flip", "truncate", "flip-decoded"})
TAMPERED_TOKEN = "CONFORMANCE-TAMPERED-TOKEN"
RUN_PREFIX_ROOT = "probe/"
RUN_PREFIX = re.compile(r"probe/r[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}/\Z")


def enc(value: str, safe: str = "/") -> str:
    return quote(value, safe=safe)


def qs(*pairs: tuple) -> str:
    """Build a raw query; a third element True keeps the value spelling verbatim."""
    return "&".join(f"{pair[0]}={pair[1] if len(pair) == 3 and pair[2] else enc(pair[1])}"
                    for pair in pairs)


def mutate_token(token: str, mutation: str) -> str:
    """Deterministic token alteration; NativeEvidenceReplayTest applies the same rule."""
    if mutation == "tampered":
        return TAMPERED_TOKEN
    if mutation == "flip":
        last = token[-1]
        return token[:-1] + ("B" if last == "A" else "A")
    if mutation == "truncate":
        return token[:-4] if len(token) > 4 else token[:1]
    if mutation == "flip-decoded":
        return flip_decoded(token)
    raise ValueError(f"unknown token mutation {mutation}")


def flip_decoded(token: str) -> str:
    """Change the lowest bit of the last decoded byte of a base64 token and re-encode it with
    the same alphabet and padding, so the result is well-formed but no longer issued."""
    import base64
    import binascii
    urlsafe = "-" in token or "_" in token
    padded = token + "=" * (-len(token) % 4)
    try:
        raw = (base64.urlsafe_b64decode if urlsafe else base64.b64decode)(padded.encode("ascii"))
    except (binascii.Error, ValueError, UnicodeEncodeError) as error:
        raise LookupError("token is not base64; no encoding-preserving mutation exists") from error
    if not raw:
        raise LookupError("token decodes to no bytes")
    changed = raw[:-1] + bytes([raw[-1] ^ 1])
    encoded = (base64.urlsafe_b64encode if urlsafe else base64.b64encode)(changed).decode("ascii")
    return encoded if token.endswith("=") else encoded.rstrip("=")


@dataclass(frozen=True)
class CorpusObject:
    name: str
    payload: bytes
    group: str
    # Raw percent-encoded name sent instead of `name` when the bytes are not valid UTF-8.
    wire_name: str | None = None
    # True: must be created or the run stops. None: an exploratory creation probe whose
    # acceptance is itself the observation. False: a provider rejection is expected.
    expect_created: bool | None = True


def payload_for(name: str, index: int) -> bytes:
    """Tiny deterministic payload; names ending in `/` are empty folder markers."""
    if name.endswith("/"):
        return b""
    return hashlib.sha256(name.encode("utf-8")).hexdigest()[:1 + index % 7].encode("ascii")


def corpus_objects(run_prefix: str, groups: list[tuple[str, list[tuple]]]) -> list[CorpusObject]:
    """Entries are (relative name, expect_created) or (relative name, expect, raw escape suffix)."""
    objects = []
    index = 0
    for group, entries in groups:
        for entry in entries:
            name = run_prefix + entry[0]
            wire = enc(name + "-", safe="") + entry[2] if len(entry) == 3 else None
            objects.append(CorpusObject(name, payload_for(name, index), group, wire_name=wire,
                                        expect_created=entry[1]))
            index += 1
    return objects


def common_groups() -> list[tuple[str, list[tuple]]]:
    """Name groups shared by every provider corpus: ranges, prefixes, Unicode, spelling,
    delimiters and a small scope. Provider-specific groups are appended by each adapter."""
    unicode_names = [("u/" + n, True) for n in (
        "E", "e", "z", "é", unicodedata.normalize("NFD", "é"), "ÿ", "퟿",
        "", "�", "\U00010000", "\U0001f600", "\U0010fffd")]
    # Noncharacters compete directly with U+10000 in the same scope; acceptance is observed.
    unicode_names += [("u/" + n, None) for n in ("￾", "￿")]
    return [
        ("r", [(f"r/k0{i}", True) for i in (1, 2, 3, 5, 6, 7)]),
        ("x", [(n, True) for n in ("x/a", "x/ab", "x/abc", "x/b")]),
        # Byte order versus UTF-16 order: U+D7FF/U+E000 surround the surrogate block,
        # U+FFFD/U+FFFF sort before U+10000 in UTF-8 but after it in UTF-16. NFC/NFD and
        # case pairs are distinct names.
        ("u", unicode_names),
        ("p", [("p/" + n, True) for n in (
            "a b", "a+b", "a%2Bb", "a%20b", "a%25b", "a%b", "a%ZZ", "�", "k&v=1", "q?x", "h#x",
            "semi;x", "col:x", "at@x", "com,x", "star*x", "brk[x]", "apos'x", 'quot"x', "tld~x",
            "dol$x", "exc!x", "par(x)", "eq=x", "pct%", "end ", " lead")]),
        ("d", [("d/" + n, True) for n in (
            "a/", "a/1", "a/m/1", "a/z", "a0", "b/1", "b/2", "c", "e/1", "f-g/1", "f-g-h", "/x")]),
        ("s", [(f"s/{i}", True) for i in (1, 2, 3)]),
    ]


def spelling_probes(prefix_param: str, scope: str) -> list[tuple[str, str, str, str]]:
    """Raw query-spelling probes over the shared `p/` names: (suffix, title, question, query)."""
    p = enc(scope)
    return [
        ("a", f"Raw `+` in a `{prefix_param}` value", "plus as literal versus space", f"{prefix_param}={p}a+b"),
        ("b", f"`%2B` in a `{prefix_param}` value", "encoded plus", f"{prefix_param}={p}a%2Bb"),
        ("c", f"`%20` in a `{prefix_param}` value", "encoded space", f"{prefix_param}={p}a%20b"),
        ("d", "`%25` before `2B`", "single decoding pass", f"{prefix_param}={p}a%252Bb"),
        ("e", "Malformed percent escape `%ZZ` beside a literal `a%ZZ` name", "400 versus literal",
         f"{prefix_param}={p}a%ZZ"),
        ("f", "Trailing lone `%`", "400 versus literal", f"{prefix_param}={p}pct%"),
        ("g", "Escape decoding to invalid UTF-8 beside a U+FFFD name", "400 versus replacement",
         f"{prefix_param}={p}%FF"),
        ("h", f"Duplicate `{prefix_param}` with identical values", "duplicate tolerated versus 400",
         f"{prefix_param}={p}a&{prefix_param}={p}a"),
        ("i", f"Duplicate `{prefix_param}` with different values", "first, last or 400",
         f"{prefix_param}={p}a&{prefix_param}={p}k"),
        ("j", "Percent-encoded `&`, `=` and `#`", "reserved characters in a value", f"{prefix_param}={p}k%26v%3D1"),
        ("k", "Raw `?` inside a value", "query characters after the first `?`", f"{prefix_param}={p}q?x"),
        ("l", "Fully encoded slashes `%2F`", "encoded slash equals literal slash",
         f"{prefix_param}={enc(scope, safe='')}"),
        ("m", "Lowercase hex escape `%2b`", "escape case", f"{prefix_param}={p}a%2bb"),
        ("n", "Parameter name without `=`", "bare name means empty versus error", f"{prefix_param}={p}&delimiter"),
        ("o", "Every reserved ASCII character in names under one prefix", "listing of reserved-character names",
         f"{prefix_param}={p}"),
        ("p", "Trailing space in a prefix", "spaces are significant", f"{prefix_param}={p}end%20"),
        ("q", "Raw `=` inside a value", "first `=` splits name from value", f"{prefix_param}={p}eq=x"),
    ]


@dataclass(frozen=True)
class TokenSource:
    """Where a token probe gets its token: a page of another case's walk, optionally altered."""

    case_id: str | None = None
    page: int = 0
    mutation: str | None = None

    def __post_init__(self) -> None:
        if self.mutation is not None and self.mutation not in TOKEN_MUTATIONS:
            raise ValueError(f"unknown token mutation {self.mutation}")
        if self.case_id is None and self.mutation != "tampered":
            raise ValueError("only the tampered constant needs no source case")


@dataclass(frozen=True)
class Case:
    id: str
    title: str
    discriminates: str
    kind: str
    query: str = ""
    token: TokenSource | None = None
    sdk: dict | None = None
    versions: tuple[str, ...] = ()
    headers: tuple[tuple[str, str], ...] = ()
    smoke: bool = False
    # A version gate answering "version not supported" skips that version's cases; the
    # coverage stays requested-but-unexecuted, so the run is INCOMPLETE.
    version_gate: bool = False

    def __post_init__(self) -> None:
        if self.kind not in CASE_KINDS:
            raise ValueError(f"case {self.id} has unknown kind {self.kind}")
        if (self.kind == "token") != (self.token is not None):
            raise ValueError(f"case {self.id} token source does not match its kind")
        if (self.kind == "sdk_run") != (self.sdk is not None):
            raise ValueError(f"case {self.id} SDK options do not match its kind")
        if "|" in self.title:
            raise ValueError(f"case {self.id} title cannot contain a table delimiter")


@dataclass(frozen=True)
class Untested:
    topic: str
    reason: str


@dataclass(frozen=True)
class PriceTable:
    source: str
    retrieved: str
    per_attempt_usd: float
    storage_gib_month_usd: float
    egress_gib_usd: float


def case_record(case: Case) -> dict:
    record = {"id": case.id, "title": case.title, "discriminates": case.discriminates,
              "kind": case.kind, "query": case.query, "versions": list(case.versions),
              "headers": [list(h) for h in case.headers]}
    if case.token is not None:
        record["token"] = {"case_id": case.token.case_id, "page": case.token.page,
                           "mutation": case.token.mutation}
    if case.sdk is not None:
        record["sdk"] = case.sdk
    return record
