#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Consistency checks for the published website and the current public documentation.

The website is built from `site/` and `docs/` on `main` (scripts/ci/build-site.sh) and CI
publishes it as a GitHub Pages artifact. That makes the site reviewable alongside the code
and docs it describes -- and it makes the drift these checks look for fixable in the first
place, because a behavior change and the page that explains it now land in one pull request.

Four checks, each independently runnable with `--only`:

  links        every site-local href, asset and fragment resolves, every link into this
               repository names a file (and heading) that exists here, and no contributor
               page (docs/internals/, docs/ops/) is published
  terminology  the public spelling and naming rules in docs/style.md, over the site
               and the current documentation -- not over code or dated evidence notes
  channel      the version the home page offers matches the channel record (and, on the
               release channel, the newest released version), and image tags name it
  a11y         landmark, skip-link, duplicate-id, and text-alternative smoke tests

Usage:
    python3 scripts/ci/check-site.py
    python3 scripts/ci/check-site.py --only links --only a11y
    python3 scripts/ci/check-site.py --site build/site   # the tree build-site.sh built
"""

from __future__ import annotations

import argparse
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
        "the replay guide is `docs/internals/swath-replay.md`",
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
        # and stays; "current provider price per 1,000 LIST requests" survives because it
        # names no figure.
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
    ("docs/dataset.md", "project-casing", "Swath",
     "the reader module's Go field `reader.Metadata.Swath`, an identifier, not the project name"),
    ("docs/dataset.html", "project-casing", "Swath",
     "the same identifier on the published page built from docs/dataset.md"),
]

# --------------------------------------------------------------------------------------
# Accessibility rules
# --------------------------------------------------------------------------------------
# VitePress's default home layout (hero and features) renders no <main> landmark, and the
# site uses the default theme without custom components. The home page still has its skip
# link to the content; every other page is held to exactly one <main>.
A11Y_MAIN_EXEMPT = {"index.html"}

# VitePress writes 404.html with an empty #app on purpose: Pages serves it for every missing
# URL, so it is rendered in the browser (the theme's not-found page, inside its layout and skip
# link). There is no static markup here to hold to the landmark and skip-link rules.
A11Y_CLIENT_RENDERED = {"404.html"}

# --------------------------------------------------------------------------------------
# Release-channel rules
# --------------------------------------------------------------------------------------
# Sentences in which a page declares which swath it offers: "The newest published build is
# swath X, ...", beside the note that the pages describe the upcoming release. Every one of them,
# on every page, must name the version in data/channel.json (source: site/static/data/channel.json),
# as must every container image tag, so the home page and the install page cannot drift apart.
VERSION_DECLARATIONS = [
    re.compile(r"The newest published build is swath ([\w.+-]+?),(?=\s)"),
]

# The pages that must always carry one, so the declaration cannot simply be deleted.
VERSION_DECLARATION_REQUIRED_ON = ("index.html", "docs/install.html")

# Contributor pages are published on GitHub only (site/.vitepress/config.mts excludes them).
UNPUBLISHED_PREFIXES = ("docs/internals/", "docs/ops/")


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
    mains: int = 0
    imgs_without_alt: int = 0
    canvases_without_label: list[tuple[str, list[str]]] = field(default_factory=list)
    skip_links: list[str] = field(default_factory=list)


class PageParser(HTMLParser):
    """Collects ids, links, and a11y-relevant elements."""

    # Attributes that carry a URL we can resolve locally.
    URL_ATTRS = {"href", "src", "poster", "data"}

    def __init__(self, page: Page) -> None:
        super().__init__(convert_charrefs=False)
        self.page = page
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
        if tag == "a":
            self._in_a = a.get("href", "")
            self._a_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._in_a is not None:
            text = "".join(self._a_text).strip()
            if re.search(r"skip to\b", text, re.IGNORECASE):
                self.page.skip_links.append(self._in_a)
            self._in_a = None

    def handle_data(self, data: str) -> None:
        if self._in_a is not None:
            self._a_text.append(data)


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
    failures: list[str] = [f"{page.rel}: a contributor page is published; exclude it in "
                           f"site/.vitepress/config.mts"
                           for page in pages if page.rel.startswith(UNPUBLISHED_PREFIXES)]
    ids_by_page = {p.rel: set(p.ids) | set(p.names) for p in pages}
    md_anchors: dict[str, set[str]] = {}

    def resolve_local(page: Page, target: str) -> str:
        """Map a site-local URL path to the file a server would return for it.

        A directory becomes its `index.html` whether or not the URL had a trailing slash,
        so `href="docs#start"` is checked against the index's ids rather than silently
        passing because the directory exists.
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
            if not (site / rel).is_file():
                failures.append(f"{page.rel}: {where} -> {value} is missing from the site tree")
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

    present = {p.rel for p in pages}
    missing = [rel for rel in VERSION_DECLARATION_REQUIRED_ON if rel not in present]
    if missing:
        return failures + [f"{rel} is missing from the site tree" for rel in missing]

    for page in pages:
        declared = [m for pattern in VERSION_DECLARATIONS
                    for m in pattern.findall(page.visible)]
        if page.rel in VERSION_DECLARATION_REQUIRED_ON and not declared:
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

    # Runnable container examples must name the version the site offers, never a tag that
    # moves under the reader.
    tag_pattern = re.compile(r"ghcr\.io/varveio/swath(?:-replay)?:([\w.\-]+)")
    for page in pages:
        for tag in set(tag_pattern.findall(page.text)):
            if tag != version:
                failures.append(
                    f"{page.rel}: image tag ghcr.io/varveio/swath:{tag} does not match the "
                    f"version {version} in data/channel.json")
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
        if page.rel in A11Y_CLIENT_RENDERED:
            continue
        if page.mains != 1 and not (page.mains == 0 and page.rel in A11Y_MAIN_EXEMPT):
            failures.append(f"{page.rel}: {page.mains} <main> landmarks, expected exactly 1")
        if not page.skip_links:
            failures.append(f"{page.rel}: no 'Skip to ...' link")
        for target in page.skip_links:
            if not target.startswith("#") or target[1:] not in page.ids:
                failures.append(f"{page.rel}: skip link points at {target}, which is not an id")
    return failures


# --------------------------------------------------------------------------------------
CHECKS = ("links", "terminology", "channel", "a11y")


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
    if "terminology" in selected:
        results["terminology"] = check_terminology(repo, pages)
    if "channel" in selected:
        results["channel"] = check_channel(site, pages, repo)
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
