#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Consistency checks for the published website and the current public documentation.

The website source lives in `site/` on `main` and CI publishes it as a GitHub Pages
artifact. That makes the site reviewable alongside the code and docs it describes --
and it makes the drift these checks look for fixable in the first place, because a
behavior change and the page that explains it now land in one pull request.

Six checks, each independently runnable with `--only`:

  links        every site-local href, media source, and fragment resolves, and every
               link into this repository names a file (and heading) that exists here
  media        the videos the deploy fetches from a release are declared, digest-pinned,
               and referenced -- and match their pin wherever they are already present
  terminology  the public spelling and naming rules in docs/style.md, over the site
               and the current documentation -- not over code or dated evidence notes
  channel      the version the site advertises matches the channel record and the
               newest released version, and runnable image tags are pinned to it
  run-facts    site/data/runs/*.json is well formed, and the figures the pages quote
               equal the record they came from
  a11y         landmark, skip-link, duplicate-id, and text-alternative smoke tests

Scope boundary with the Java tests: `PublicRunFactsTest` guards the run figures
quoted in README.md and docs/full-scale-demo.md and the internal shape of the
records (explicit nulls, one interchangeable key set, numeric fields never strings).
This script guards the same records where the *site* quotes them. The only overlap
is deliberate: the schema version and the run id are re-checked here as this
consumer's own preconditions, since every page assertion below reads through them.

Usage:
    python3 scripts/ci/check-site.py
    python3 scripts/ci/check-site.py --only links --only a11y
    python3 scripts/ci/check-site.py --site build/site   # check a built tree
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote

REPO = Path(__file__).resolve().parents[2]

# --------------------------------------------------------------------------------------
# Terminology rules
# --------------------------------------------------------------------------------------
# docs/style.md is the authority; these are the mechanically checkable parts of it plus
# the stale names a rename left behind. Each rule is (id, pattern, message).
#
# Deliberately NOT mechanized: the object-vs-file rule in general. `docs/style.md` allows
# "objects -- the files stored in the bucket" as a one-time gloss and "Parquet files" is
# correct for swath's own output, so a bare /files/ ban would be all exemptions. The
# specific phrasings below are the ones that were actually wrong on the site.

TERMINOLOGY_RULES: list[tuple[str, re.Pattern[str], str]] = [
    (
        "replay-tool-name",
        re.compile(r"swath-replay-server"),
        "the replay toolkit is `swath-replay`",
    ),
    (
        "replay-doc-path",
        re.compile(r"docs/swath-replay-server\.md"),
        "the replay guide is `docs/swath-replay.md`",
    ),
    (
        "us-english",
        # `analyses` is the correct US plural of `analysis`, so only the verb forms are
        # listed; `\b` keeps `analyse` from matching inside it.
        re.compile(r"\b(?:colours?|coloured|colouring|behaviours?|modell(?:ed|ing)|"
                   r"utilisations?|organis(?:e|es|ed|ing|ation)|analys(?:e|ed|ing))\b",
                   re.IGNORECASE),
        "first-party documentation uses US English (color, behavior, modeled, utilization)",
    ),
    (
        # Any casing of the word except the exact lowercase one: `Swath`, `SWATH`, and
        # `SwatH` are the same mistake. Identifiers that merely contain the word
        # (`SWATH_REPLAY_OPTS`, `SwathException`) are not word-bounded matches. Written
        # case-sensitively so the lookahead can name the one spelling that is allowed.
        "project-casing",
        re.compile(r"\b(?!swath\b)[Ss][Ww][Aa][Tt][Hh]\b"),
        "the project name is lowercase `swath` everywhere, including at the start of a sentence",
    ),
    (
        "single-file-parquet",
        re.compile(r"single[- ]file Parquet", re.IGNORECASE),
        "`.parquet`-looking output is a legacy one-writer directory, not one physical file",
    ),
    (
        # Two shapes: a sub-cent figure, which is only ever a per-request rate, and any
        # price quoted per thousand. A run's own estimated total ("~$0.208") is evidence
        # and stays; docs/operating.md's "current provider price per 1,000 LIST requests"
        # survives because it names no figure.
        "evergreen-pricing",
        re.compile(r"\$\s?0\.00\d+|"
                   r"\$\s?\d+(?:\.\d+)?\s*(?:/|per\s+|for\s+every\s+)\s*1[,.]?(?:000|k)\b",
                   re.IGNORECASE),
        "quote the provider's current LIST rate as an input, not a hard-coded price",
    ),
    (
        # Only phrasings that are unambiguously about bucket contents. A bare /files/ rule
        # would fail correct sentences such as "list files in the output directory" and
        # "query the Parquet files", so those are left to review rather than to a regex.
        "objects-not-files",
        re.compile(r"\b(?:S3|bucket|object[- ]store)\s+files\b|"
                   r"\bfiles\s+in\s+(?:the\s+|a\s+|your\s+)?bucket\b", re.IGNORECASE),
        "S3 technical claims say object and key, not file",
    ),
]

# Files the terminology rules apply to, beyond the site tree itself (which is checked
# wherever `--site` points, source or built). Globs are relative to the repository root.
TERMINOLOGY_INCLUDE = [
    "README.md",
    "CONTRIBUTING.md",
    "CODE_OF_CONDUCT.md",
    "SECURITY.md",
    "ROADMAP.md",
    "RELEASING.md",
    "CHANGELOG.md",
    "docs/**/*.md",
    # Not documentation, but these render the public trace report: a British spelling
    # here becomes one on the site at the next regeneration.
    "tools/explainer/README.md",
    "tools/explainer/src/swath_explainer/templates/*.html",
]

# Dated raw evidence, per docs/style.md's ownership table. An investigation note records
# what was written at the time, including project casing that has since changed; editing
# it would falsify the record rather than fix a public page.
TERMINOLOGY_EXCLUDE = [
    "docs/ops/dev/field-investigations.md",
]

# Specific, justified exemptions: (path suffix, rule id, matched text, why).
# Keep this list short. An entry here is a claim that the match is correct writing, not
# a way to silence a finding.
TERMINOLOGY_ALLOW: list[tuple[str, str, str, str]] = [
    # (none today -- kept explicit so an addition is a reviewed decision)
]

# --------------------------------------------------------------------------------------
# Run-facts rules
# --------------------------------------------------------------------------------------
RUN_FACTS_SCHEMA = "swath-public-run-v1"

TRACE_RUN = "noaa-gestofs-pds-field-guide-trace"
RECORDING_RUN = "noaa-gestofs-pds-2026-08-03-505ae26"

# Generated report pages that embed their own run-facts record, as
# `<script type="application/json" id="swath-run-facts">`: page -> run id.
EMBEDDED_RUN_FACTS = {
    "runs/noaa-gestofs-pds/index.html": TRACE_RUN,
}

# The field guide's result panels quote the trace record. Label -> (run id, JSON pointer,
# format). Every `<dt>` carrying one of these labels must show the record's value.
FIELD_GUIDE_FIGURES: list[tuple[str, str, str, str]] = [
    ("Objects listed", TRACE_RUN, "/result/objects", "int"),
    ("Initial ranges", TRACE_RUN, "/result/initial_ranges", "int"),
    ("In one range", TRACE_RUN, "/result/heaviest_initial_range/share_percent", "percent"),
    ("Behind one range", TRACE_RUN, "/result/heaviest_initial_range/objects", "int"),
    ("Splits in that lineage", TRACE_RUN, "/result/heaviest_initial_range/splits_in_lineage", "int"),
    ("Ranges completed", TRACE_RUN, "/result/completed_ranges", "int"),
    ("Ranges failed", TRACE_RUN, "/result/failed_ranges", "int"),
    ("Listing workers", TRACE_RUN, "/result/listing_workers_observed", "int"),
    ("Trace event span", TRACE_RUN, "/clocks/trace_event_span/display", "text"),
    ("Worker utilization", TRACE_RUN, "/analysis/share_of_perfect_speedup_percent", "percent"),
]


def quoted_figures(runs: dict[str, dict]) -> tuple[list[tuple[str, str, int, str]], list[str]]:
    """Sentences on the site whose numbers come from a run record.

    Each entry is (page, exact expected text, how many times it appears, what it is). The
    text is *built* from the record, so rewording the record's figures without reworking the
    prose fails here. The count matters: a figure quoted twice must stay right in both
    places, and a bare "is it present" test would pass on one of them being edited.

    A field these sentences need can legitimately be null in a record -- provenance that
    was never captured. That is a failure of this pairing, not a crash: a page cannot
    quote a figure the record does not establish.
    """
    trace = runs[TRACE_RUN]
    rec = runs[RECORDING_RUN]
    sources = {
        "objects": (trace, "/result/objects"),
        "ranges": (trace, "/result/initial_ranges"),
        "splits": (trace, "/result/splits"),
        "owner_splits": (trace, "/result/owner_splits"),
        "share": (trace, "/result/heaviest_initial_range/share_percent"),
        "span_ms": (trace, "/clocks/trace_event_span/ms"),
        "rec_objects": (rec, "/result/objects"),
    }
    values = {name: get(record, pointer) for name, (record, pointer) in sources.items()}
    missing = [f"{record['run_id']}.json: {pointer} is null, but the site quotes it"
               for name, (record, pointer) in sources.items() if values[name] is None]
    if missing:
        return [], missing

    objects = values["objects"]
    ranges = values["ranges"]
    splits = values["splits"]
    owner_splits = values["owner_splits"]
    share = values["share"]
    span_ms = values["span_ms"]
    rec_objects = values["rec_objects"]
    return [
        ("index.html", TRACE_RUN, 1, "the trace run's id"),
        ("index.html", RECORDING_RUN, 1, "the recording run's id"),
        ("index.html", f"{span_ms:,.1f} ms", 1, "the trace event span in milliseconds"),
        ("index.html", f"{span_ms / 1000:.1f} seconds", 1, "the trace event span in seconds"),
        ("index.html", f"created {ranges:,} ranges", 1, "the initial range count"),
        ("index.html", f"({rec_objects:,} objects)", 2, "the recording's object total"),
        ("field-guide/index.html", f"{splits - owner_splits:,} of the run's {splits:,} splits",
         1, "thief-taken splits out of all splits"),
        ("field-guide/index.html",
         f"one of {ranges:,} initial ranges held {share}% of the {objects:,} objects",
         1, "the skew sentence"),
        ("field-guide/index.html", f"returned {rec_objects:,} objects",
         1, "the recording's object total, as the other capture"),
    ], []


# --------------------------------------------------------------------------------------
# Release-channel rules
# --------------------------------------------------------------------------------------
# Sentences in which a page declares which swath it documents. Every one of them, on every
# page, must name the version in site/data/channel.json -- otherwise the homepage and the
# field guide can drift a release apart from each other, which is how this started.
VERSION_DECLARATIONS = [
    re.compile(r"Documentation for swath ([\w.+-]+)\.(?=\s|$)"),
    re.compile(r"This guide describes swath ([\w.+-]+)\.(?=\s|$)"),
]

# The page that must always carry one, so the declaration cannot simply be deleted.
VERSION_DECLARATION_REQUIRED_ON = "index.html"


# --------------------------------------------------------------------------------------
# Accessibility rules
# --------------------------------------------------------------------------------------
# Generated report pages are exempt from the landmark/skip-link rule: they are rendered by
# tools/explainer from a template, so the fix belongs in that template and ships with the
# next regeneration, not as a hand edit to a generated artifact. Tracked in issue #195.
A11Y_LANDMARK_EXEMPT = {"runs/noaa-gestofs-pds/index.html"}


# --------------------------------------------------------------------------------------
# Machinery
# --------------------------------------------------------------------------------------
@dataclass
class Page:
    """What the checks need from one HTML file, collected in a single parse."""

    path: Path
    rel: str
    text: str
    visible: str = ""
    ids: list[str] = field(default_factory=list)
    names: list[str] = field(default_factory=list)
    links: list[tuple[str, str, str]] = field(default_factory=list)  # (tag, attr, value)
    definitions: list[tuple[str, str]] = field(default_factory=list)  # (dt text, dd text)
    mains: int = 0
    imgs_without_alt: int = 0
    canvases_without_label: list[tuple[str, list[str]]] = field(default_factory=list)
    skip_links: list[str] = field(default_factory=list)


class PageParser(HTMLParser):
    """Collects ids, links, definition pairs, and a11y-relevant elements."""

    # Attributes that carry a URL we can resolve locally.
    URL_ATTRS = {"href", "src", "poster", "data"}

    def __init__(self, page: Page) -> None:
        super().__init__(convert_charrefs=False)
        self.page = page
        self._capture: str | None = None
        self._buffer: list[str] = []
        self._pending_dt: str | None = None
        self._in_a: str | None = None
        self._a_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = {k: (v or "") for k, v in attrs}
        if "id" in a:
            self.page.ids.append(a["id"])
        if tag == "a" and "name" in a:
            self.page.names.append(a["name"])
        for attr in self.URL_ATTRS:
            if attr in a and a[attr]:
                self.page.links.append((tag, attr, a[attr]))
        if tag == "meta":
            prop = a.get("property", "") or a.get("name", "")
            if prop in ("og:image", "og:url", "twitter:image") and a.get("content"):
                self.page.links.append((tag, prop, a["content"]))
        if tag == "main":
            self.page.mains += 1
        if tag == "img" and "alt" not in a:
            self.page.imgs_without_alt += 1
        if tag == "canvas":
            name = a.get("aria-label", "").strip()
            refs = a.get("aria-labelledby", "").split()
            if not name and not refs:
                self.page.canvases_without_label.append((a.get("id", "<unnamed>"), []))
            elif not name:
                # Recorded for resolution once the page's ids are all known.
                self.page.canvases_without_label.append((a.get("id", "<unnamed>"), refs))
        if tag in ("dt", "dd"):
            self._capture = tag
            self._buffer = []
        if tag == "a":
            self._in_a = a.get("href", "")
            self._a_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "dt" and self._capture == "dt":
            self._pending_dt = "".join(self._buffer).strip()
            self._capture = None
        elif tag == "dd" and self._capture == "dd":
            if self._pending_dt is not None:
                self.page.definitions.append(
                    (self._pending_dt, "".join(self._buffer).strip()))
                self._pending_dt = None
            self._capture = None
        elif tag == "a" and self._in_a is not None:
            text = "".join(self._a_text).strip()
            if re.search(r"skip to\b", text, re.IGNORECASE):
                self.page.skip_links.append(self._in_a)
            self._in_a = None

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._buffer.append(data)
        if self._in_a is not None:
            self._a_text.append(data)

    def handle_entityref(self, name: str) -> None:
        # Definition values are compared literally, so keep entities as written.
        if self._capture:
            self._buffer.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self._capture:
            self._buffer.append(f"&#{name};")


def visible_text(markup: str) -> str:
    """The page as a reader sees it: no script, style, markup, or entity encoding.

    Prose checks run against this rather than the raw file so that a line break or an
    `&nbsp;` inside a sentence is not the difference between pass and fail.
    """
    stripped = re.sub(r"<(script|style)\b.*?</\1>", " ", markup, flags=re.S | re.I)
    stripped = re.sub(r"<!--.*?-->", " ", stripped, flags=re.S)
    stripped = re.sub(r"<[^>]+>", " ", stripped)
    return " ".join(html.unescape(stripped).split())


def load_pages(site: Path) -> list[Page]:
    pages = []
    for path in sorted(site.rglob("*.html")):
        text = path.read_text(encoding="utf-8")
        page = Page(path=path, rel=path.relative_to(site).as_posix(),
                    text=text, visible=visible_text(text))
        parser = PageParser(page)
        parser.feed(page.text)
        parser.close()
        pages.append(page)
    return pages


def get(node: dict, pointer: str):
    """Minimal JSON-pointer read; returns None for a missing branch."""
    cur = node
    for part in pointer.strip("/").split("/"):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def strip_markup(value: str) -> str:
    """Definition values as a reader sees them: markup and footnote daggers removed."""
    value = re.sub(r"<[^>]+>", "", value)
    value = value.replace("&#8224;", "").replace("&dagger;", "").replace("†", "")
    value = value.replace("&nbsp;", " ").replace("\xa0", " ")
    return " ".join(value.split())


def gh_slug(heading: str) -> str:
    """GitHub's heading-anchor slug, for links into this repository's Markdown."""
    text = heading.strip()
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.lower()
    text = re.sub(r"[^\w\- ]", "", text, flags=re.UNICODE)
    return text.replace(" ", "-")


def markdown_anchors(path: Path) -> set[str]:
    """Every fragment GitHub will resolve on a rendered Markdown file.

    ATX headings only -- this repository writes no setext headings, and a link into one
    would fail here rather than pass wrongly, which is the safe direction.
    """
    anchors: set[str] = set()
    seen: dict[str, int] = {}
    fence: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        marker = re.match(r"\s*(```+|~~~+)", line)
        if marker:
            token = marker.group(1)[:3]
            if fence is None:
                fence = token
            elif token == fence:
                fence = None
            continue
        if fence is not None:
            continue
        m = re.match(r"#{1,6}\s+(.*)$", line)
        if m:
            slug = gh_slug(m.group(1))
            n = seen.get(slug, 0)
            seen[slug] = n + 1
            candidate = slug if n == 0 else f"{slug}-{n}"
            while candidate in anchors:  # a heading may already own the generated name
                n += 1
                seen[slug] = n + 1
                candidate = f"{slug}-{n}"
            anchors.add(candidate)
        for explicit in re.findall(r'<a\s+(?:id|name)="([^"]+)"', line):
            anchors.add(explicit)
    return anchors


# --------------------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------------------
SITE_ORIGIN = "https://swath.varve.io"
REPO_BLOB = "https://github.com/varveio/swath/blob/main/"
REPO_TREE = "https://github.com/varveio/swath/tree/main/"

SKIP_SCHEMES = ("data:", "mailto:", "javascript:", "tel:")


def check_links(site: Path, pages: list[Page], repo: Path) -> list[str]:
    """Site-local targets exist, fragments resolve, repo links name files that are here."""
    failures: list[str] = []
    ids_by_page = {p.rel: set(p.ids) | set(p.names) for p in pages}
    md_anchors: dict[str, set[str]] = {}
    # The videos are not in git; the deploy fetches them from a release and verifies their
    # digests. A declared path counts as resolvable so this check runs offline against a
    # source tree, and `media` is what holds the declaration itself to account.
    manifest, _ = load_media_manifest(site)
    fetched = {entry.get("path") for entry in manifest.get("files", [])}

    def resolve_local(page: Page, target: str) -> str:
        """Map a site-local URL path to the file a server would return for it.

        A directory becomes its `index.html` whether or not the URL had a trailing slash,
        so `href="field-guide#terms"` is checked against the guide's ids rather than
        silently passing because the directory exists.
        """
        target = unquote(target)
        if target.startswith("/"):
            rel = target.lstrip("/")
        else:
            rel = (Path(page.rel).parent / target).as_posix()
        parts: list[str] = []
        for part in rel.split("/"):
            if part in ("", "."):
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        rel = "/".join(parts)
        if rel == "" or (site / rel).is_dir():
            rel = f"{rel}/index.html".lstrip("/")
        return rel

    def check_fragment(page: Page, rel: str, frag: str, where: str) -> None:
        if rel.endswith(".html"):
            known = ids_by_page.get(rel)
            if known is None:
                return  # target file already reported as missing
            if frag not in known:
                failures.append(f"{page.rel}: {where} -> #{frag} is not an id in {rel}")

    for page in pages:
        for tag, attr, value in page.links:
            value = value.strip()
            if not value or value.startswith(SKIP_SCHEMES):
                continue
            where = f"<{tag} {attr}>"

            if value.startswith("#"):
                frag = unquote(value[1:])
                if frag and frag not in set(page.ids) | set(page.names):
                    failures.append(f"{page.rel}: {where} -> {value} has no matching id")
                continue

            if value == SITE_ORIGIN or value.startswith(SITE_ORIGIN + "/"):
                value = value[len(SITE_ORIGIN):] or "/"
            elif value.startswith(REPO_BLOB) or value.startswith(REPO_TREE):
                prefix = REPO_BLOB if value.startswith(REPO_BLOB) else REPO_TREE
                doc, _, frag = value[len(prefix):].partition("#")
                doc, frag = unquote(doc), unquote(frag)
                target = repo / doc
                if not target.exists():
                    failures.append(f"{page.rel}: {where} -> {doc} does not exist on main")
                    continue
                if frag and target.suffix == ".md":
                    if doc not in md_anchors:
                        md_anchors[doc] = markdown_anchors(target)
                    if frag not in md_anchors[doc]:
                        failures.append(
                            f"{page.rel}: {where} -> {doc}#{frag} is not a heading there")
                continue
            elif re.match(r"^[a-z][a-z0-9+.-]*:", value):
                continue  # some other external origin; not resolvable offline

            path, _, frag = value.partition("#")
            path = path.partition("?")[0]  # a query string is not part of the file path
            frag = unquote(frag)
            rel = resolve_local(page, path) if path else page.rel
            if not (site / rel).is_file() and rel not in fetched:
                failures.append(f"{page.rel}: {where} -> {value} is missing from the site "
                                f"tree and is not declared in data/media.json")
                continue
            if frag:
                check_fragment(page, rel, frag, where)

    return failures


def scoped_files(repo: Path) -> list[Path]:
    excluded = {repo / p for p in TERMINOLOGY_EXCLUDE}
    files: list[Path] = []
    for pattern in TERMINOLOGY_INCLUDE:
        for path in sorted(repo.glob(pattern)):
            if path.is_file() and path not in excluded and path not in files:
                files.append(path)
    return files


def check_terminology(repo: Path, pages: list[Page]) -> list[str]:
    failures: list[str] = []
    allowed = {(suffix, rule, text) for suffix, rule, text, _ in TERMINOLOGY_ALLOW}
    subjects = [(page.rel, page.text) for page in pages]
    subjects += [(path.relative_to(repo).as_posix(), path.read_text(encoding="utf-8"))
                 for path in scoped_files(repo)]
    for rel, content in subjects:
        for lineno, line in enumerate(content.splitlines(), 1):
            for rule_id, pattern, message in TERMINOLOGY_RULES:
                for m in pattern.finditer(line):
                    if any(rel.endswith(s) and r == rule_id and t == m.group(0)
                           for s, r, t in allowed):
                        continue
                    failures.append(f"{rel}:{lineno}: {rule_id}: {m.group(0)!r} -- {message}")
    return failures


def newest_released_version(repo: Path) -> str | None:
    for line in (repo / "CHANGELOG.md").read_text(encoding="utf-8").splitlines():
        m = re.match(r"^##\s+(\d+\.\d+\.\d+(?:-[\w.]+)?)\b", line)
        if m:
            return m.group(1)
    return None


def check_channel(site: Path, pages: list[Page], repo: Path) -> list[str]:
    failures: list[str] = []
    record = site / "data" / "channel.json"
    if not record.exists():
        return [f"{record} is missing: the site must declare its channel and version"]
    channel = json.loads(record.read_text(encoding="utf-8"))
    name = channel.get("channel")
    version = channel.get("version")
    if name not in ("release", "main"):
        failures.append(f"data/channel.json: channel {name!r} must be 'release' or 'main'")
    if not isinstance(version, str) or not version:
        return failures + ["data/channel.json: version must be a non-empty string"]

    required = next((p for p in pages if p.rel == VERSION_DECLARATION_REQUIRED_ON), None)
    if required is None:
        return failures + [f"{VERSION_DECLARATION_REQUIRED_ON} is missing from the site tree"]

    for page in pages:
        declared = [m for pattern in VERSION_DECLARATIONS
                    for m in pattern.findall(page.visible)]
        if page is required and not declared:
            failures.append(
                f"{page.rel}: no line declaring which swath version the site documents")
        for shown in declared:
            if shown != version:
                failures.append(
                    f"{page.rel}: declares swath {shown} but data/channel.json says {version}")

    if name == "release":
        released = newest_released_version(repo)
        if released is None:
            failures.append("CHANGELOG.md: no released version heading to compare against")
        elif released != version:
            failures.append(
                f"data/channel.json: release channel pinned to {version} but the newest "
                f"release in CHANGELOG.md is {released}")
        if "-SNAPSHOT" in version or "-rc." in version:
            failures.append(
                f"data/channel.json: {version} is not a release version for a release channel")

    # Runnable container examples on a release-pinned site must name the pinned version,
    # never a tag that moves under the reader.
    tag_pattern = re.compile(r"ghcr\.io/varveio/swath(?:-replay)?:([\w.\-]+)")
    for page in pages:
        for tag in set(tag_pattern.findall(page.text)):
            if name == "release" and tag != version:
                failures.append(
                    f"{page.rel}: image tag ghcr.io/varveio/swath:{tag} does not match the "
                    f"declared release version {version}")
    return failures


def load_run_records(site: Path) -> tuple[dict[str, dict], list[str]]:
    failures: list[str] = []
    runs: dict[str, dict] = {}
    directory = site / "data" / "runs"
    if not directory.is_dir():
        return runs, [f"{directory} is missing: the site has no canonical run records"]
    for path in sorted(directory.glob("*.json")):
        rel = path.relative_to(site).as_posix()
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            failures.append(f"{rel}: does not parse: {exc}")
            continue
        schema = record.get("schema_version")
        if schema != RUN_FACTS_SCHEMA:
            failures.append(f"{rel}: schema_version is {schema!r}, expected {RUN_FACTS_SCHEMA!r}")
        run_id = record.get("run_id")
        if run_id != path.stem:
            failures.append(f"{rel}: run_id {run_id!r} does not match the file name")
        if run_id in runs:
            failures.append(f"{rel}: run_id {run_id!r} is already used by another record")
        if isinstance(run_id, str):
            runs[run_id] = record
    return runs, failures


def format_figure(value, kind: str) -> str:
    if kind == "int":
        return f"{value:,}"
    if kind == "percent":
        return f"{value}%"
    return str(value)


def check_run_facts(site: Path, pages: list[Page]) -> list[str]:
    runs, failures = load_run_records(site)

    for rel, run_id in EMBEDDED_RUN_FACTS.items():
        page = next((p for p in pages if p.rel == rel), None)
        if page is None:
            failures.append(f"{rel} is missing from the site tree")
            continue
        m = re.search(
            r'<script type="application/json" id="swath-run-facts">(.*?)</script>',
            page.text, re.S)
        if not m:
            failures.append(f"{rel}: no embedded swath-run-facts record")
            continue
        try:
            embedded = json.loads(m.group(1))
        except json.JSONDecodeError as exc:
            failures.append(f"{rel}: embedded run facts do not parse: {exc}")
            continue
        canonical = runs.get(run_id)
        if canonical is None:
            failures.append(f"{rel}: embeds run {run_id!r}, which has no canonical record")
        elif embedded != canonical:
            failures.append(
                f"{rel}: the embedded run facts differ from site/data/runs/{run_id}.json "
                f"(regenerate the report from the record rather than editing the page)")

    guide = next((p for p in pages if p.rel == "field-guide/index.html"), None)
    if guide is None:
        failures.append("field-guide/index.html is missing from the site tree")
    else:
        labels = {label for label, _, _, _ in FIELD_GUIDE_FIGURES}
        shown: dict[str, list[str]] = {label: [] for label in labels}
        for term, definition in guide.definitions:
            term = strip_markup(term)
            if term in labels:
                shown[term].append(strip_markup(definition))
        for label, run_id, pointer, kind in FIELD_GUIDE_FIGURES:
            record = runs.get(run_id)
            if record is None:
                failures.append(f"field guide quotes run {run_id!r}, which has no record")
                continue
            value = get(record, pointer)
            if value is None:
                failures.append(f"{run_id}.json: {pointer} is null but the field guide shows it")
                continue
            expected = format_figure(value, kind)
            if not shown[label]:
                failures.append(f"field-guide/index.html: no figure labeled {label!r}")
            for actual in shown[label]:
                if actual != expected:
                    failures.append(
                        f"field-guide/index.html: {label!r} shows {actual!r}, but "
                        f"{run_id}.json {pointer} is {expected!r}")

    if TRACE_RUN in runs and RECORDING_RUN in runs:
        by_rel = {p.rel: p for p in pages}
        expectations, missing = quoted_figures(runs)
        failures.extend(missing)
        for rel, expected, times, what in expectations:
            page = by_rel.get(rel)
            if page is None:
                failures.append(f"{rel} is missing from the site tree")
                continue
            seen = page.visible.count(expected)
            if seen != times:
                failures.append(
                    f"{rel}: states {what} as the record has it {seen} time(s), expected "
                    f"{times}: {expected!r}")
    else:
        failures.append("the two published NOAA captures must both have canonical records")

    return failures


def load_media_manifest(site: Path) -> tuple[dict, list[str]]:
    """The pinned video assets: what the deploy fetches and the digest it must see."""
    record = site / "data" / "media.json"
    if not record.exists():
        return {}, [f"{record} is missing: the site cannot declare its off-repository media"]
    try:
        manifest = json.loads(record.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, [f"data/media.json: does not parse: {exc}"]
    return manifest, []


def check_media(site: Path, pages: list[Page]) -> list[str]:
    """Every off-repository video is declared, pinned, referenced, and (if here) intact."""
    manifest, failures = load_media_manifest(site)
    if not manifest:
        return failures
    if not manifest.get("release"):
        failures.append("data/media.json: no release tag naming where the assets live")
    declared = manifest.get("files")
    if not isinstance(declared, list) or not declared:
        return failures + ["data/media.json: files must be a non-empty list"]

    referenced = {
        value.lstrip("/").partition("#")[0]
        for page in pages for _, _, value in page.links
        if not value.startswith(SKIP_SCHEMES) and not re.match(r"^[a-z][a-z0-9+.-]*:", value)
    }
    for entry in declared:
        path = entry.get("path", "")
        if not path or not entry.get("asset"):
            failures.append(f"data/media.json: an entry is missing path or asset: {entry}")
            continue
        if not re.fullmatch(r"[0-9a-f]{64}", entry.get("sha256", "")):
            failures.append(f"data/media.json: {path} has no sha256 digest to verify against")
        if path not in referenced:
            failures.append(
                f"data/media.json: {path} is declared but no page references it -- "
                f"drop the entry or the release asset it pins")
        local = site / path
        if local.exists():
            digest = hashlib.sha256(local.read_bytes()).hexdigest()
            if digest != entry.get("sha256"):
                failures.append(
                    f"{path}: present in the checked tree but hashes to {digest}, "
                    f"not the {entry.get('sha256')} pinned in data/media.json")
    return failures


def check_a11y(pages: list[Page]) -> list[str]:
    failures: list[str] = []
    for page in pages:
        duplicates = sorted(i for i, n in Counter(page.ids).items() if n > 1)
        if duplicates:
            failures.append(f"{page.rel}: duplicate ids: {', '.join(duplicates)}")
        if page.imgs_without_alt:
            failures.append(f"{page.rel}: {page.imgs_without_alt} <img> without alt")
        ids = set(page.ids)
        for canvas, refs in page.canvases_without_label:
            if not refs:
                failures.append(f"{page.rel}: <canvas id={canvas}> has no accessible name")
            elif not any(ref in ids for ref in refs):
                failures.append(
                    f"{page.rel}: <canvas id={canvas}> is labelled by "
                    f"{' '.join(refs)}, which is not an id on this page")
        if page.rel in A11Y_LANDMARK_EXEMPT:
            continue
        if page.mains != 1:
            failures.append(f"{page.rel}: {page.mains} <main> landmarks, expected exactly 1")
        if not page.skip_links:
            failures.append(f"{page.rel}: no 'Skip to ...' link")
        for target in page.skip_links:
            if not target.startswith("#") or target[1:] not in page.ids:
                failures.append(f"{page.rel}: skip link points at {target}, which is not an id")
    return failures


# --------------------------------------------------------------------------------------
CHECKS = ("links", "media", "terminology", "channel", "run-facts", "a11y")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--site", type=Path, default=None,
                        help="the site tree to check (default: <repo>/site)")
    parser.add_argument("--repo", type=Path, default=REPO,
                        help="the repository root that documentation links resolve against")
    parser.add_argument("--only", action="append", choices=CHECKS, metavar="CHECK",
                        help=f"run only this check; repeatable ({', '.join(CHECKS)})")
    args = parser.parse_args(list(argv) if argv is not None else None)

    repo = args.repo.resolve()
    site = (args.site or repo / "site").resolve()
    if not site.is_dir():
        print(f"site tree not found: {site}", file=sys.stderr)
        return 2

    pages = load_pages(site)
    if not pages:
        print(f"no HTML pages under {site}", file=sys.stderr)
        return 2

    selected = args.only or list(CHECKS)
    results: dict[str, list[str]] = {}
    if "links" in selected:
        results["links"] = check_links(site, pages, repo)
    if "media" in selected:
        results["media"] = check_media(site, pages)
    if "terminology" in selected:
        results["terminology"] = check_terminology(repo, pages)
    if "channel" in selected:
        results["channel"] = check_channel(site, pages, repo)
    if "run-facts" in selected:
        results["run-facts"] = check_run_facts(site, pages)
    if "a11y" in selected:
        results["a11y"] = check_a11y(pages)

    total = 0
    for name in CHECKS:
        if name not in results:
            continue
        failures = results[name]
        total += len(failures)
        if failures:
            print(f"FAIL {name} ({len(failures)})")
            for line in failures:
                print(f"  {line}")
        else:
            print(f"ok   {name}")

    if total:
        print(f"\n{total} site consistency failure(s) in {len(pages)} page(s) under {site}")
        return 1
    print(f"\nall checks passed over {len(pages)} page(s) under {site}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
