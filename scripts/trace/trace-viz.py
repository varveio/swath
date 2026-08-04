#!/usr/bin/env python3
"""trace-viz — render a swath ``--trace`` JSONL run as a self-contained HTML page.

Two things a trace file can show that a counter summary cannot: *where* in the keyspace the
run's work actually was, and *when* each range was carved out of which parent. This renders
both, as one replayable keyspace-over-time map.

Self-contained (stdlib only), matching ``scripts/ci/check-instrumentation-drift.py`` — the
public repo carries no external Python dependency. The generated page is likewise
self-contained: no CDN, no external images, safe to open from a file:// URL.

Usage:
    scripts/trace/trace-viz.py TRACE.jsonl [-o OUT.html] [--title NAME] [--anonymize]
    scripts/trace/trace-viz.py --self-test

Exit codes: 0 = page written, 1 = the trace yielded nothing renderable, 2 = script error.

The keyspace axis
-----------------
Keys are byte strings, so "position in the keyspace" needs a projection, and the two obvious
ones both fail on real data:

  * A **byte-value axis** (read N bytes as a fraction) collapses. Every Common Crawl key shares
    roughly 25 bytes of prefix, so the whole run lands on a hairline — design law L1's failure
    mode, reached from the other direction.
  * A **rank axis over every observed key** flattens. Page cursors arrive about one per 1000
    keys, so ranking them spaces the axis by key count and the density profile becomes a
    straight line by construction.

So the axis is **anchored on the engine's own structural boundaries**: the distinct ``lo`` /
``hi`` / ``pivot`` keys — one per range edge — spread evenly across the width, with any other
key placed inside its enclosing interval by byte-value interpolation. Monotone, legible on real
buckets, and density survives: an interval holding 5,000,000 keys and one holding 1,000 get the
same width, so the profile above them differs by three orders of magnitude.

The tradeoff is stated on the generated page as well as here — equal screen distance means an
equal number of range boundaries, **not** an equal byte range.

Density comes from ``page_committed``: each event carries the page's new ``cursor`` and its
``keys`` count, and the node's previous cursor is known, so every page contributes its key
count to the keyspace interval it actually covered. The profile is therefore measured at
page resolution, not modelled.

Sensitivity
-----------
A trace carries real key names on nearly every event (metrics-internals.md §7) — the same
sensitivity class as the listing output itself. The rendered page inherits that: bounds,
cursors, and pivots appear in the event table and in tooltips. Pass ``--anonymize`` to emit
positions and counts only, with every key name withheld, when the picture needs to travel
further than the trace does.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

SCHEMA_VERSION = 1
POS_BYTES = 8
MASS_BINS = 2048
MAX_PAGE_EVENTS = 8000

# Event kinds as emitted into the page's compact event stream.
K_SEED, K_CLAIM, K_PAGE, K_SPLIT, K_OWNER_SPLIT, K_DONE, K_FAIL, K_STEAL = range(8)

_ESCAPE_RE = re.compile(r"\\x([0-9a-fA-F]{2})")


# --------------------------------------------------------------------------- keys


def unescape(text):
    """Reverse ``ControlCharEscaper``: ``\\xHH`` back to the character it stands for.

    The escaping is applied to C0 controls and DEL only, so this is exact for every key
    that does not itself contain the literal sequence backslash-x-hexdigit-hexdigit — an
    ambiguity inherent to the trace format, not introduced here.
    """
    return _ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 16)), text)


def key_bytes(text):
    """A trace key string as the UTF-8 bytes swath itself compares."""
    return unescape(text).encode("utf-8", errors="surrogatepass")


def common_prefix(items):
    if not items:
        return b""
    lo, hi = min(items), max(items)
    n = 0
    while n < len(lo) and n < len(hi) and lo[n] == hi[n]:
        n += 1
    return lo[:n]


def _fraction(kb, skip):
    """The bytes of ``kb`` after ``skip``, read as a base-256 fraction in [0, 1)."""
    value = 0.0
    scale = 1.0
    for byte in kb[skip:skip + POS_BYTES]:
        scale /= 256.0
        value += byte * scale
    return value


class Axis:
    """Projects key bytes onto [0, 1] as a piecewise-linear *anchored* keyspace axis.

    A plain byte-value axis does not survive contact with a real bucket. Every Common Crawl key
    shares roughly 25 bytes of prefix, so reading a fixed 8 bytes maps the entire run onto a
    single hairline — the exact failure mode design law L1 names, arrived at from the other
    direction. A rank axis over every observed key has the opposite problem: page cursors arrive
    about one per 1000 keys, so ranking them spaces the axis by key count and flattens the
    density profile into a straight line by construction.

    So the anchors are the engine's own **structural boundaries** — the distinct ``lo``/``hi``/
    ``pivot`` keys, one per range edge — spaced evenly across the width. Any other key (a page
    cursor) is placed inside its enclosing anchor interval by byte-value interpolation. The
    result is monotone, legible on real buckets, and still shows density: an interval holding
    5,000,000 keys and one holding 1,000 get the same width, so the profile above them differs
    by three orders of magnitude, which is the fact worth seeing.

    The tradeoff, stated on the page as well as here: equal screen distance means an equal
    number of range boundaries, **not** an equal byte range.
    """

    def __init__(self, anchors):
        self.anchors = sorted(anchors)
        self.count = len(self.anchors)

    def pos(self, kb, default=0.0):
        """Position of a key in [0, 1]; ``default`` for a null bound (⊥ / open frontier)."""
        if kb is None:
            return default
        if self.count < 2:
            return 0.5
        span = float(self.count - 1)

        lo, hi = 0, self.count
        while lo < hi:                       # first anchor >= kb
            mid = (lo + hi) // 2
            if self.anchors[mid] < kb:
                lo = mid + 1
            else:
                hi = mid
        idx = lo
        if idx < self.count and self.anchors[idx] == kb:
            return idx / span
        if idx == 0:
            return 0.0
        if idx >= self.count:
            return 1.0

        left, right = self.anchors[idx - 1], self.anchors[idx]
        skip = len(common_prefix([left, right]))
        a, b, k = _fraction(left, skip), _fraction(right, skip), _fraction(kb, skip)
        frac = 0.5 if b <= a else min(1.0, max(0.0, (k - a) / (b - a)))
        return (idx - 1 + frac) / span


# --------------------------------------------------------------------------- input


def load_events(path):
    """Parse a trace file leniently.

    Tolerates the two things metrics-internals.md §7 requires a consumer to tolerate: a torn
    final line after a hard kill, and event kinds this reader does not know. Returns the
    parsed events plus a count of lines that could not be used.
    """
    events, skipped = [], 0
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except ValueError:
                skipped += 1
                continue
            if isinstance(event, dict) and "event" in event and "ts_ns" in event:
                events.append(event)
            else:
                skipped += 1
    return events, skipped


def collect_anchors(events):
    """The distinct structural boundary keys — range edges and split pivots, never cursors."""
    seen = set()
    for event in events:
        for field in ("lo", "hi", "pivot"):
            value = event.get(field)
            if value:
                seen.add(key_bytes(value))
    return seen


# --------------------------------------------------------------------------- model


def build_model(events, skipped, title, anonymize):
    """Reduce a raw event list to everything the page needs, and nothing it doesn't."""
    events.sort(key=lambda e: e["ts_ns"])
    anchors = collect_anchors(events)
    axis = Axis(anchors)
    t0 = events[0]["ts_ns"]

    def ms(event):
        return round((event["ts_ns"] - t0) / 1e6, 1)

    def at(event, field, default=0.0):
        value = event.get(field)
        return round(axis.pos(key_bytes(value) if value else None, default), 5)

    def name(event, field):
        value = event.get(field)
        if value is None:
            return None
        return None if anonymize else value

    mass = [0.0] * MASS_BINS
    cursors = {}          # node_id -> position of its last committed page
    bounds = {}           # node_id -> (lo, hi) positions, for a page whose node was never claimed
    open_seg = {}         # node_id -> [x0, x1, t_start, worker] for the static keyspace×time map
    segments = []         # closed [x0, x1, t0, t1, worker] rectangles
    mechanisms = []       # split mechanism names, in first-seen order
    outcomes = []         # steal outcome:reason pairs, in first-seen order
    stream = []
    pages_kept = pages_seen = 0
    total_keys = 0
    pending_keys = 0      # keys from dropped page events, carried onto the next kept one
    workers = set()
    keep_every = 1

    page_total = sum(1 for e in events if e.get("event") == "page_committed")
    if page_total > MAX_PAGE_EVENTS:
        keep_every = (page_total // MAX_PAGE_EVENTS) + 1

    def index(table, value):
        if value not in table:
            table.append(value)
        return table.index(value)

    def close_segment(node, t):
        """End a node's current keyspace×time rectangle (it completed, failed, or was narrowed)."""
        seg = open_seg.pop(node, None)
        if seg and t > seg[2]:
            segments.append([seg[0], seg[1], seg[2], t, seg[3]])
        return seg

    def deposit(start, end, keys):
        """Attribute one page's keys to the keyspace interval that page actually covered."""
        if keys <= 0:
            return
        first = min(MASS_BINS - 1, max(0, int(start * MASS_BINS)))
        last = min(MASS_BINS - 1, max(0, int(end * MASS_BINS)))
        if last < first:
            first, last = last, first
        share = keys / float(last - first + 1)
        for b in range(first, last + 1):
            mass[b] += share

    for event in events:
        kind = event.get("event")
        node = event.get("node_id", -1)
        worker = event.get("worker_id", -1)
        if worker >= 0:
            workers.add(worker)
        t = ms(event)

        if kind == "seeded":
            lo, hi = at(event, "lo", 0.0), at(event, "hi", 1.0)
            bounds[node] = (lo, hi)
            stream.append([t, K_SEED, node, lo, hi])

        elif kind == "claimed":
            lo, hi = at(event, "lo", 0.0), at(event, "hi", 1.0)
            cur = at(event, "cursor", lo)
            bounds[node] = (lo, hi)
            cursors.setdefault(node, cur)
            open_seg[node] = [lo, hi, t, worker]
            stream.append([t, K_CLAIM, node, worker, lo, cur, hi, name(event, "lo"), name(event, "hi")])

        elif kind == "page_committed":
            pages_seen += 1
            keys = int(event.get("keys", 0) or 0)
            total_keys += keys
            new = at(event, "cursor", cursors.get(node, 0.0))
            prev = cursors.get(node, bounds.get(node, (new, new))[0])
            deposit(prev, new, keys)
            cursors[node] = new
            pending_keys += keys
            if pages_seen % keep_every == 0 or event.get("completed"):
                stream.append([t, K_PAGE, node, worker, new, pending_keys])
                pending_keys = 0
                pages_kept += 1

        elif kind in ("split", "owner_split"):
            mech = index(mechanisms, event.get("mechanism") or "?")
            child = event.get("child_node_id", -1)
            pivot, hi = at(event, "pivot", 0.0), at(event, "hi", 1.0)
            bounds[child] = (pivot, hi)
            # The parent kept (lo, pivot] from here on — close its old rectangle and open a
            # narrower one, so the map shows the keyspace each node actually owned when.
            prior = close_segment(node, t)
            if prior:
                open_seg[node] = [prior[0], pivot, t, prior[3]]
            stream.append([t, K_SPLIT if kind == "split" else K_OWNER_SPLIT,
                           node, child, worker, pivot, hi, mech, name(event, "pivot")])

        elif kind == "completed":
            close_segment(node, t)
            stream.append([t, K_DONE, node])

        elif kind == "failed":
            close_segment(node, t)
            stream.append([t, K_FAIL, node, event.get("reason") or "?"])

        elif kind == "steal_attempt":
            label = "%s / %s" % (event.get("outcome") or "?", event.get("reason") or "?")
            stream.append([t, K_STEAL, worker, index(outcomes, label)])

        # Unknown event kinds are ignored by design (§7's additive-schema contract).

    if pending_keys:                       # never lose the tail of a downsampled run
        stream.append([ms(events[-1]), K_PAGE, -1, -1, 1.0, pending_keys])

    end = ms(events[-1])
    for node in list(open_seg):            # a run that was killed leaves ranges still open
        close_segment(node, end)

    peak = max(mass) or 1.0
    profile = [round(v / peak, 4) for v in mass]

    return {
        "meta": {
            "title": title,
            "events": len(events),
            "skippedLines": skipped,
            "pagesSeen": pages_seen,
            "pagesPlotted": pages_kept,
            "keepEvery": keep_every,
            "totalKeys": total_keys,
            "workers": len(workers),
            "durationMs": round((events[-1]["ts_ns"] - t0) / 1e6, 1),
            "anchors": axis.count,
            "anonymized": anonymize,
        },
        "profile": profile,
        "mechanisms": mechanisms,
        "outcomes": outcomes,
        "stream": stream,
        "segments": segments,
    }


# --------------------------------------------------------------------------- output


def render(model):
    payload = json.dumps(model, separators=(",", ":"))
    return TEMPLATE.replace("/*__MODEL__*/null", payload)


TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>swath run trace</title>
<style>
  :root {
    --paper:#F1F2EB; --panel:#E7E9DF; --deep:#DDE0D3;
    --ink:#131A18; --ink2:#4E5A55; --ink3:#7C8781; --rule:#CBD0C2;
    --accent:#0D6E60; --signal:#9A6710; --alert:#A33F28;
    --mono:ui-monospace,"SFMono-Regular","Cascadia Mono",Menlo,Consolas,monospace;
    --serif:Charter,"Bitstream Charter","Sitka Text",Cambria,Georgia,serif;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --paper:#0E1312; --panel:#161D1B; --deep:#1E2724;
      --ink:#E7EBE3; --ink2:#9BA69F; --ink3:#74807A; --rule:#29332F;
      --accent:#56C9B1; --signal:#E0AC55; --alert:#E4826A;
    }
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--paper); color:var(--ink); font-family:var(--serif);
         font-size:16px; line-height:1.6; }
  .wrap { max-width:74rem; margin:0 auto; padding:2rem 1.5rem 4rem; }
  h1 { font-family:var(--mono); font-weight:500; font-size:1.7rem; letter-spacing:-0.03em; margin:0 0 .3rem; }
  .sub { font-family:var(--mono); font-size:.75rem; color:var(--ink3); letter-spacing:.04em; margin-bottom:1.6rem; }
  .panel { background:var(--panel); border:1px solid var(--rule); border-radius:2px; }
  .bar { display:flex; align-items:center; gap:.8rem; flex-wrap:wrap; padding:.65rem .9rem;
         border-bottom:1px solid var(--rule); }
  button { font-family:var(--mono); font-size:.72rem; padding:.32rem .7rem; background:var(--paper);
           color:var(--ink); border:1px solid var(--rule); border-radius:2px; cursor:pointer; }
  button:hover { border-color:var(--ink2); }
  button:focus-visible { outline:2px solid var(--accent); outline-offset:2px; }
  input[type=range] { flex:1 1 12rem; min-width:8rem; accent-color:var(--accent); }
  .clock { font-family:var(--mono); font-size:.75rem; color:var(--ink2);
           font-variant-numeric:tabular-nums; white-space:nowrap; }
  canvas { display:block; width:100%; height:300px; }
  dl.readout { display:grid; grid-template-columns:repeat(auto-fit,minmax(8.5rem,1fr));
               margin:0; border-top:1px solid var(--rule); }
  dl.readout > div { padding:.6rem .9rem; border-right:1px solid var(--rule); }
  dl.readout > div:last-child { border-right:0; }
  dt { font-family:var(--mono); font-size:.62rem; letter-spacing:.12em; text-transform:uppercase;
       color:var(--ink3); margin-bottom:.15rem; }
  dd { margin:0; font-family:var(--mono); font-size:1.05rem; font-variant-numeric:tabular-nums; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(19rem,1fr)); gap:1.2rem; margin-top:1.4rem; }
  section.card { background:var(--panel); border:1px solid var(--rule); border-radius:2px; padding:1rem 1.1rem; }
  section.card h2 { font-family:var(--mono); font-size:.86rem; font-weight:600; margin:0 0 .6rem; }
  table { border-collapse:collapse; width:100%; font-size:.8rem; }
  td, th { text-align:left; padding:.28rem .5rem .28rem 0; border-bottom:1px solid var(--rule); }
  th { font-family:var(--mono); font-size:.62rem; letter-spacing:.1em; text-transform:uppercase; color:var(--ink3); }
  td.n { font-family:var(--mono); text-align:right; font-variant-numeric:tabular-nums; }
  td.k { font-family:var(--mono); font-size:.74rem; word-break:break-all; }
  tr:last-child td { border-bottom:0; }
  .swatch { display:inline-block; width:.7rem; height:.7rem; border-radius:2px; margin-right:.4rem;
            vertical-align:baseline; }
  .guide li { margin-bottom:.4em; }
  .note { font-size:.86rem; color:var(--ink2); }
  code { font-family:var(--mono); font-size:.86em; }
  footer { margin-top:2rem; font-family:var(--mono); font-size:.7rem; color:var(--ink3); line-height:1.7; }
  @media (prefers-reduced-motion: reduce) { * { animation-duration:.001ms !important; } }
</style>
</head>
<body>
<div class="wrap">
  <h1 id="title">swath run trace</h1>
  <div class="sub" id="subtitle"></div>

  <div class="panel">
    <div class="bar">
      <button id="play" type="button">Pause</button>
      <button id="restart" type="button">Restart</button>
      <button id="speed" type="button">1&times;</button>
      <input id="scrub" type="range" min="0" max="1000" value="0" aria-label="Scrub through the run">
      <span class="clock" id="clock">0.0s</span>
    </div>
    <canvas id="map" role="img"
      aria-label="Keyspace over time: a measured key-density profile above, and the run's live ranges below, each drained by one worker, with split pivots marked."></canvas>
    <dl class="readout">
      <div><dt>Keys listed</dt><dd id="m-keys">0</dd></div>
      <div><dt>Pages</dt><dd id="m-pages">0</dd></div>
      <div><dt>Live ranges</dt><dd id="m-live">0</dd></div>
      <div><dt>Busy workers</dt><dd id="m-busy">0</dd></div>
      <div><dt>Splits</dt><dd id="m-splits">0</dd></div>
      <div><dt>Steal attempts</dt><dd id="m-steals">0</dd></div>
    </dl>
  </div>

  <section class="card" style="margin-top:1.4rem">
    <h2>Keyspace &times; time — the whole run at once</h2>
    <p class="note" style="margin:0 0 .8rem">Horizontal is the same keyspace axis as the map above;
    vertical is time running downward. Each rectangle is one range over the span it owned — a split
    ends the parent's rectangle and starts a narrower one. Wide and short means a big slice eaten
    fast. <b>A single tall thin column is a serial tail.</b></p>
    <canvas id="static" role="img"
      aria-label="Static keyspace-over-time map: each range drawn as a rectangle whose width is the keyspace it owned and whose height is how long it owned it."></canvas>
  </section>

  <div class="grid">
    <section class="card">
      <h2>Split mechanisms</h2>
      <table><thead><tr><th>Mechanism</th><th style="text-align:right">Count</th></tr></thead>
      <tbody id="mech-table"></tbody></table>
      <p class="note" id="mech-empty" hidden>No split committed in this trace — the run never
      rebalanced, either because the seed was already sufficient or because nothing was splittable.</p>
    </section>

    <section class="card">
      <h2>Steal outcomes</h2>
      <table><thead><tr><th>Outcome / reason</th><th style="text-align:right">Count</th></tr></thead>
      <tbody id="steal-table"></tbody></table>
      <p class="note" id="steal-empty" hidden>No steal attempt in this trace.</p>
    </section>

    <section class="card">
      <h2>Reading the map</h2>
      <ul class="guide">
        <li><b>Healthy run</b> — confetti: many short drains, everyone busy, a map of wide short rectangles.</li>
        <li><b>Dense serial tail</b> — a single tall thin column outliving the rest of the run.</li>
        <li><b>Thief probe storm</b> — a blizzard of steal-attempt markers with few splits behind them.</li>
        <li><b>Owner-split confetti</b> — a stack of hairline rectangles in one keyspace region.</li>
        <li><b>Density vs. progress</b> — a worker crossing a tall part of the profile crawls; one
            crossing a flat part rips. Keyspace distance is not work.</li>
      </ul>
    </section>
  </div>

  <footer id="provenance"></footer>
</div>

<script>
(function () {
  "use strict";
  var MODEL = /*__MODEL__*/null;
  if (!MODEL) { return; }

  var K_SEED=0, K_CLAIM=1, K_PAGE=2, K_SPLIT=3, K_OWNER=4, K_DONE=5, K_FAIL=6, K_STEAL=7;
  var meta = MODEL.meta, stream = MODEL.stream, profile = MODEL.profile;

  var LANES = ["#0D6E60","#2E7C86","#46765A","#6E7B3C","#38617E","#7A6A46","#3F7F70","#8A7A4E"];
  var LANES_DARK = ["#56C9B1","#6FBBD0","#86C495","#BCC076","#7FA8CE","#C7B187","#5FD0C4","#D5C08F"];

  function css(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }
  function dark() {
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  }
  function lane(id) {
    var table = dark() ? LANES_DARK : LANES;
    return table[((id % table.length) + table.length) % table.length];
  }
  function fmt(n) { return Math.round(n).toLocaleString("en-US"); }

  // ---- header -------------------------------------------------------------
  document.getElementById("title").textContent = meta.title || "swath run trace";
  var bits = [fmt(meta.totalKeys) + " keys", fmt(meta.pagesSeen) + " pages",
              meta.workers + " workers", (meta.durationMs / 1000).toFixed(1) + "s",
              "keyspace axis anchored on " + fmt(meta.anchors) + " range boundaries"];
  document.getElementById("subtitle").textContent = bits.join("  ·  ");

  var prov = ["Rendered from a swath --trace JSONL file by scripts/trace/trace-viz.py.",
              meta.events + " events parsed"];
  if (meta.skippedLines) { prov.push(meta.skippedLines + " unparseable line(s) skipped"); }
  if (meta.keepEvery > 1) {
    prov.push("page events downsampled " + meta.keepEvery + ":1 for playback (" +
              fmt(meta.pagesPlotted) + " of " + fmt(meta.pagesSeen) +
              " plotted; key counts and the density profile use every page)");
  }
  if (meta.anonymized) { prov.push("ANONYMIZED — key names withheld"); }
  document.getElementById("provenance").textContent = prov.join(". ") + ".";

  // ---- static tables ------------------------------------------------------
  var mechCounts = {}, stealCounts = {}, splitTotal = 0, stealTotal = 0;
  stream.forEach(function (e) {
    if (e[1] === K_SPLIT || e[1] === K_OWNER) { mechCounts[e[7]] = (mechCounts[e[7]] || 0) + 1; splitTotal++; }
    else if (e[1] === K_STEAL) { stealCounts[e[3]] = (stealCounts[e[3]] || 0) + 1; stealTotal++; }
  });

  function fill(tbodyId, emptyId, counts, labels, colorize) {
    var rows = Object.keys(counts).map(Number).sort(function (a, b) { return counts[b] - counts[a]; });
    var body = document.getElementById(tbodyId);
    if (!rows.length) { document.getElementById(emptyId).hidden = false; return; }
    rows.forEach(function (idx) {
      var tr = document.createElement("tr");
      var td = document.createElement("td");
      if (colorize) {
        var sw = document.createElement("span");
        sw.className = "swatch";
        sw.style.background = lane(idx + 1);
        td.appendChild(sw);
      }
      td.appendChild(document.createTextNode(labels[idx] || "?"));
      td.className = "k";
      var n = document.createElement("td");
      n.className = "n";
      n.textContent = fmt(counts[idx]);
      tr.appendChild(td); tr.appendChild(n); body.appendChild(tr);
    });
  }
  fill("mech-table", "mech-empty", mechCounts, MODEL.mechanisms, true);
  fill("steal-table", "steal-empty", stealCounts, MODEL.outcomes, false);

  // ---- replay state -------------------------------------------------------
  var nodes, live, marks, cursor, stats;

  function reset() {
    nodes = {};        // node_id -> {lo, hi, cur, worker, state}
    live = 0;
    marks = [];        // recent split pivots, for the flash
    cursor = 0;        // index into stream
    stats = { keys: 0, pages: 0, splits: 0, steals: 0, busy: {} };
  }

  function apply(e) {
    var kind = e[1], n;
    if (kind === K_SEED) {
      nodes[e[2]] = { lo: e[3], hi: e[4], cur: e[3], worker: -1, state: "pending" };
    } else if (kind === K_CLAIM) {
      n = nodes[e[2]] || (nodes[e[2]] = { lo: e[4], hi: e[6], cur: e[5], worker: -1, state: "pending" });
      n.lo = e[4]; n.hi = e[6]; n.cur = e[5]; n.worker = e[3]; n.state = "live";
      stats.busy[e[3]] = (stats.busy[e[3]] || 0) + 1;
    } else if (kind === K_PAGE) {
      stats.keys += e[5];
      stats.pages++;
      n = nodes[e[2]];
      if (n) { n.cur = e[4]; n.worker = e[3]; }
    } else if (kind === K_SPLIT || kind === K_OWNER) {
      n = nodes[e[2]];
      if (n) { n.hi = e[5]; }                    // parent narrows to the pivot
      nodes[e[3]] = { lo: e[5], hi: e[6], cur: e[5], worker: -1, state: "pending" };
      marks.push({ x: e[5], mech: e[7], owner: kind === K_OWNER, age: 0 });
      stats.splits++;
    } else if (kind === K_DONE || kind === K_FAIL) {
      n = nodes[e[2]];
      if (n) {
        n.state = kind === K_DONE ? "done" : "failed";
        if (n.worker >= 0) {
          stats.busy[n.worker]--;
          if (stats.busy[n.worker] <= 0) { delete stats.busy[n.worker]; }
        }
      }
    } else if (kind === K_STEAL) {
      stats.steals++;
    }
  }

  function seekTo(tMs) {
    if (tMs < clockMs) { reset(); }
    while (cursor < stream.length && stream[cursor][0] <= tMs) { apply(stream[cursor++]); }
    live = 0;
    for (var id in nodes) { if (nodes[id].state === "live" || nodes[id].state === "pending") { live++; } }
  }

  // ---- canvas -------------------------------------------------------------
  var cv = document.getElementById("map"), ctx = cv.getContext("2d");
  var W = 0, H = 0, PAD = 24;

  function resize() {
    var dpr = window.devicePixelRatio || 1, rect = cv.getBoundingClientRect();
    W = Math.max(320, rect.width);
    H = W < 640 ? 340 : 300;
    cv.style.height = H + "px";
    cv.width = Math.round(W * dpr); cv.height = Math.round(H * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  function px(x) { return PAD + x * (W - PAD * 2); }

  function draw() {
    var profTop = 20, profH = 92, base = profTop + profH;
    ctx.clearRect(0, 0, W, H);
    ctx.font = "10px " + css("--mono");

    // density profile — compressive scale, or one spike flattens everything else
    ctx.beginPath();
    ctx.moveTo(px(0), base);
    for (var i = 0; i < profile.length; i += 2) {
      ctx.lineTo(px((i + 0.5) / profile.length), base - Math.pow(profile[i], 0.42) * profH * 0.94);
    }
    ctx.lineTo(px(1), base);
    ctx.closePath();
    ctx.fillStyle = css("--deep"); ctx.fill();
    ctx.strokeStyle = css("--rule"); ctx.lineWidth = 1; ctx.stroke();

    ctx.fillStyle = css("--ink3"); ctx.textAlign = "left";
    ctx.fillText(W < 560 ? "key density" : "key density — measured from page commits", px(0), 13);
    ctx.textAlign = "right";
    ctx.fillText("keyspace →", px(1), 13);

    var top = base + 26, bandH = 30;
    ctx.strokeStyle = css("--rule");
    ctx.beginPath(); ctx.moveTo(px(0), top - 9); ctx.lineTo(px(1), top - 9); ctx.stroke();

    var doneY = top, activeY = top + 10;
    for (var id in nodes) {
      var n = nodes[id], x0 = px(n.lo), x1 = px(n.hi), w = Math.max(1, x1 - x0);
      if (n.state === "done") {
        ctx.globalAlpha = 0.45; ctx.fillStyle = css("--ink3");
        ctx.fillRect(x0, doneY, w - 0.5, 5); ctx.globalAlpha = 1;
      } else if (n.state === "failed") {
        ctx.globalAlpha = 0.5; ctx.fillStyle = css("--alert");
        ctx.fillRect(x0, doneY, w - 0.5, 5); ctx.globalAlpha = 1;
      } else if (n.state === "pending") {
        ctx.strokeStyle = css("--ink3"); ctx.lineWidth = 1; ctx.setLineDash([3, 2]);
        ctx.strokeRect(x0 + 0.5, activeY + 0.5, Math.max(2, w) - 1, bandH - 1);
        ctx.setLineDash([]);
      } else {
        var col = lane(n.worker), xc = px(n.cur);
        ctx.strokeStyle = col; ctx.lineWidth = 1.2;
        ctx.strokeRect(x0 + 0.5, activeY + 0.5, Math.max(2, w) - 1, bandH - 1);
        ctx.fillStyle = col; ctx.globalAlpha = 0.3;
        ctx.fillRect(x0 + 1, activeY + 1, Math.max(0, xc - x0) - 1, bandH - 2);
        ctx.globalAlpha = 1;
        ctx.fillRect(Math.max(x0, xc - 1), activeY, 2, bandH);
      }
    }

    for (var m = marks.length - 1; m >= 0; m--) {
      var mk = marks[m];
      mk.age++;
      if (mk.age > 40) { marks.splice(m, 1); continue; }
      ctx.globalAlpha = 1 - mk.age / 40;
      ctx.strokeStyle = lane(mk.mech + 1);
      ctx.lineWidth = mk.owner ? 2.4 : 1.6;
      ctx.beginPath();
      ctx.moveTo(px(mk.x), activeY - 8); ctx.lineTo(px(mk.x), activeY + bandH + 6);
      ctx.stroke();
      ctx.globalAlpha = 1;
    }

    var legendY = activeY + bandH + 32;
    ctx.textAlign = "left"; ctx.fillStyle = css("--ink3");
    var items = [["fill", css("--accent"), "listed"], ["bar", css("--ink3"), "range finished"],
                 ["bar", css("--alert"), "range failed"], ["rect", css("--ink3"), "not yet claimed"],
                 ["tick", css("--signal"), "split pivot"]];
    var cx = px(0), right = px(1);
    items.forEach(function (it) {
      var wide = 22 + ctx.measureText(it[2]).width;
      if (cx + wide > right) { cx = px(0); legendY += 15; }
      if (it[0] === "fill") { ctx.fillStyle = it[1]; ctx.globalAlpha = 0.45; ctx.fillRect(cx, legendY - 7, 16, 8); ctx.globalAlpha = 1; }
      else if (it[0] === "bar") { ctx.fillStyle = it[1]; ctx.globalAlpha = 0.5; ctx.fillRect(cx, legendY - 5, 16, 4); ctx.globalAlpha = 1; }
      else if (it[0] === "rect") { ctx.strokeStyle = it[1]; ctx.lineWidth = 1; ctx.setLineDash([3, 2]); ctx.strokeRect(cx + 0.5, legendY - 7.5, 16, 9); ctx.setLineDash([]); }
      else { ctx.strokeStyle = it[1]; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(cx + 7, legendY - 9); ctx.lineTo(cx + 7, legendY + 1); ctx.stroke(); }
      ctx.fillStyle = css("--ink3");
      ctx.fillText(it[2], cx + 22, legendY);
      cx += wide + 20;
    });
  }

  function readout() {
    document.getElementById("m-keys").textContent = fmt(stats.keys);
    document.getElementById("m-pages").textContent = fmt(stats.pages);
    document.getElementById("m-live").textContent = fmt(live);
    document.getElementById("m-busy").textContent = fmt(Object.keys(stats.busy).length) + " / " + meta.workers;
    document.getElementById("m-splits").textContent = fmt(stats.splits) + " / " + fmt(splitTotal);
    document.getElementById("m-steals").textContent = fmt(stats.steals) + " / " + fmt(stealTotal);
    document.getElementById("clock").textContent =
      (clockMs / 1000).toFixed(1) + "s / " + (meta.durationMs / 1000).toFixed(1) + "s";
    scrub.value = String(Math.round(1000 * clockMs / Math.max(1, meta.durationMs)));
  }

  // ---- player -------------------------------------------------------------
  var clockMs = 0, running = true, rate = 1, last = 0;
  var SPEEDS = [1, 4, 16, 64], speedIx = 0;
  var playBtn = document.getElementById("play");
  var speedBtn = document.getElementById("speed");
  var scrub = document.getElementById("scrub");
  var reduce = window.matchMedia("(prefers-reduced-motion: reduce)");

  // Compress the run so a multi-hour trace is watchable; SPEEDS multiplies on top.
  var BASE = Math.max(1, meta.durationMs / 30000);

  function frame(ts) {
    requestAnimationFrame(frame);
    if (!last) { last = ts; }
    var dt = ts - last; last = ts;
    if (running) {
      clockMs = Math.min(meta.durationMs, clockMs + dt * BASE * rate);
      seekTo(clockMs);
      if (clockMs >= meta.durationMs) { running = false; playBtn.textContent = "Replay"; }
    }
    draw(); readout();
  }

  playBtn.addEventListener("click", function () {
    if (clockMs >= meta.durationMs) { clockMs = 0; reset(); seekTo(0); }
    running = !running;
    playBtn.textContent = running ? "Pause" : "Play";
  });
  document.getElementById("restart").addEventListener("click", function () {
    clockMs = 0; reset(); seekTo(0); running = true; playBtn.textContent = "Pause";
  });
  speedBtn.addEventListener("click", function () {
    speedIx = (speedIx + 1) % SPEEDS.length;
    rate = SPEEDS[speedIx];
    speedBtn.innerHTML = rate + "&times;";
  });
  scrub.addEventListener("input", function () {
    running = false; playBtn.textContent = "Play";
    clockMs = (Number(scrub.value) / 1000) * meta.durationMs;
    reset(); seekTo(clockMs);
    draw(); readout();
  });

  // ---- static keyspace x time map ----------------------------------------
  // Time as an AXIS, not a playhead: the animated strip above shows how the carving
  // unfolds, this shows the finished shape of it. A serial tail is a tall thin column.
  var sv = document.getElementById("static"), sctx = sv.getContext("2d");
  var SPAD_L = 44, SPAD_R = 14, SPAD_T = 14, SPAD_B = 24;

  function drawStatic() {
    var dpr = window.devicePixelRatio || 1, rect = sv.getBoundingClientRect();
    var w = Math.max(320, rect.width), h = Math.min(520, Math.max(240, w * 0.46));
    sv.style.height = h + "px";
    sv.width = Math.round(w * dpr); sv.height = Math.round(h * dpr);
    sctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    sctx.clearRect(0, 0, w, h);

    var iw = w - SPAD_L - SPAD_R, ih = h - SPAD_T - SPAD_B;
    var dur = Math.max(1, meta.durationMs);
    function sx(p) { return SPAD_L + p * iw; }
    function sy(t) { return SPAD_T + (t / dur) * ih; }

    sctx.font = "10px " + css("--mono");
    sctx.strokeStyle = css("--rule"); sctx.lineWidth = 1;
    sctx.fillStyle = css("--ink3");

    var ticks = 6, i, dp = dur < 10000 ? 2 : (dur < 60000 ? 1 : 0);
    for (i = 0; i <= ticks; i++) {
      var t = (dur / ticks) * i, y = sy(t);
      sctx.beginPath(); sctx.moveTo(SPAD_L, y); sctx.lineTo(SPAD_L + iw, y); sctx.stroke();
      sctx.textAlign = "right";
      sctx.fillText((t / 1000).toFixed(dp) + "s", SPAD_L - 6, y + 3);
    }
    sctx.textAlign = "center";
    for (i = 0; i <= 4; i++) {
      sctx.fillText((i * 25) + "%", sx(i / 4), SPAD_T + ih + 15);
    }

    MODEL.segments.forEach(function (s) {
      var x0 = sx(s[0]), x1 = sx(s[1]), y0 = sy(s[2]), y1 = sy(s[3]);
      var bw = Math.max(1, x1 - x0), bh = Math.max(1, y1 - y0);
      var col = lane(s[4]);
      sctx.globalAlpha = 0.3; sctx.fillStyle = col; sctx.fillRect(x0, y0, bw, bh);
      sctx.globalAlpha = 1; sctx.strokeStyle = col; sctx.lineWidth = 0.6;
      sctx.strokeRect(x0 + 0.25, y0 + 0.25, bw, bh);
    });

    stream.forEach(function (e) {
      if (e[1] !== K_SPLIT && e[1] !== K_OWNER) { return; }
      sctx.beginPath();
      sctx.arc(sx(e[5]), sy(e[0]), e[1] === K_OWNER ? 3.6 : 2.8, 0, Math.PI * 2);
      sctx.fillStyle = lane(e[7] + 1); sctx.fill();
      sctx.lineWidth = 1.2; sctx.strokeStyle = css("--panel"); sctx.stroke();
    });

    if (!MODEL.segments.length) {
      sctx.textAlign = "center"; sctx.fillStyle = css("--ink3");
      sctx.fillText("no completed range in this trace", w / 2, SPAD_T + ih / 2);
    }
  }

  window.addEventListener("resize", function () { resize(); draw(); drawStatic(); });
  if (window.matchMedia) {
    var mq = window.matchMedia("(prefers-color-scheme: dark)");
    if (mq.addEventListener) {
      mq.addEventListener("change", function () { draw(); drawStatic(); });
    }
  }

  resize(); reset(); drawStatic();
  if (reduce.matches) {
    running = false; playBtn.textContent = "Play";
    clockMs = meta.durationMs; seekTo(clockMs);
  }
  draw(); readout();
  requestAnimationFrame(frame);
})();
</script>
</body>
</html>
"""


# --------------------------------------------------------------------------- self-test


SELF_TEST_TRACE = [
    {"v": 1, "ts_ns": 1000, "ts_ms": 1, "event": "seeded", "worker_id": -1, "node_id": 1,
     "lo": None, "hi": "b/"},
    {"v": 1, "ts_ns": 1100, "ts_ms": 1, "event": "seeded", "worker_id": -1, "node_id": 2,
     "lo": "b/", "hi": None},
    {"v": 1, "ts_ns": 2000, "ts_ms": 2, "event": "claimed", "worker_id": 0, "node_id": 1,
     "lo": "a/", "cursor": "a/", "hi": "b/"},
    {"v": 1, "ts_ns": 3000, "ts_ms": 3, "event": "page_committed", "worker_id": 0, "node_id": 1,
     "keys": 1000, "cursor": "a/m", "completed": False},
    {"v": 1, "ts_ns": 4000, "ts_ms": 4, "event": "split", "worker_id": 1, "node_id": 1,
     "child_node_id": 3, "mechanism": "byte_midpoint", "pivot": "a/q", "hi": "b/"},
    {"v": 1, "ts_ns": 5000, "ts_ms": 5, "event": "steal_attempt", "worker_id": 1,
     "outcome": "RETRY", "reason": "probe_empty"},
    {"v": 1, "ts_ns": 6000, "ts_ms": 6, "event": "page_committed", "worker_id": 0, "node_id": 1,
     "keys": 400, "cursor": "a/q", "completed": True},
    {"v": 1, "ts_ns": 7000, "ts_ms": 7, "event": "completed", "worker_id": 0, "node_id": 1},
    {"v": 1, "ts_ns": 8000, "ts_ms": 8, "event": "future_event_kind", "worker_id": 0, "node_id": 9},
]


def self_test():
    """Exercise the reductions that are easy to get quietly wrong."""
    failures = []

    def check(label, got, want):
        if got != want:
            failures.append("%s: got %r, want %r" % (label, got, want))

    check("unescape", unescape(r"a\x09b"), "a\tb")
    check("unescape untouched", unescape("plain/key"), "plain/key")
    check("common prefix", common_prefix([b"crawl-a", b"crawl-b", b"crawl-c"]), b"crawl-")
    check("common prefix disjoint", common_prefix([b"a", b"z"]), b"")

    axis = Axis({b"a/a", b"a/m", b"a/z"})
    check("axis floor", round(axis.pos(b"a/a"), 6), 0.0)
    check("axis ceiling", round(axis.pos(b"a/z"), 6), 1.0)
    check("axis anchor is exact", round(axis.pos(b"a/m"), 6), 0.5)
    check("axis null default", axis.pos(None, 0.5), 0.5)
    check("axis is monotone", axis.pos(b"a/b") < axis.pos(b"a/c"), True)
    check("axis clamps below", axis.pos(b"a/"), 0.0)
    check("axis clamps above", axis.pos(b"b"), 1.0)
    # The failure that motivated the anchored axis: a deep shared prefix must still spread out.
    deep = Axis({b"crawl-data/CC-MAIN-2014-6/x", b"crawl-data/CC-MAIN-2019-3/x",
                 b"crawl-data/CC-MAIN-2024-9/x"})
    check("deep prefix still spreads", round(deep.pos(b"crawl-data/CC-MAIN-2019-3/x"), 6), 0.5)

    model = build_model(list(SELF_TEST_TRACE), 0, "self-test", False)
    check("keys summed", model["meta"]["totalKeys"], 1400)
    check("pages counted", model["meta"]["pagesSeen"], 2)
    check("workers counted", model["meta"]["workers"], 2)
    check("mechanism captured", model["mechanisms"], ["byte_midpoint"])
    check("steal outcome captured", model["outcomes"], ["RETRY / probe_empty"])
    check("unknown kind ignored",
          any(e[1] not in range(8) for e in model["stream"]), False)
    check("mass is non-empty", any(v > 0 for v in model["profile"]), True)

    # A torn final line must not sink the parse.
    torn = [json.dumps(e) for e in SELF_TEST_TRACE]
    torn.append('{"v":1,"ts_ns":9000,"event":"page_comm')
    tmp = Path(".trace-viz-selftest.jsonl")
    try:
        tmp.write_text("\n".join(torn), encoding="utf-8")
        events, skipped = load_events(tmp)
        check("torn line skipped", skipped, 1)
        check("intact lines kept", len(events), len(SELF_TEST_TRACE))
    finally:
        tmp.unlink(missing_ok=True)

    page = render(build_model(list(SELF_TEST_TRACE), 0, "self-test", True))
    check("model injected", "/*__MODEL__*/null" in page, False)
    check("anonymized page withholds keys", "a/q" in page, False)
    check("page is standalone", "http://" in page or "https://" in page, False)

    for line in failures:
        print("FAIL " + line, file=sys.stderr)
    print("self-test: %d checks, %d failures" % (24, len(failures)), file=sys.stderr)
    return 1 if failures else 0


# --------------------------------------------------------------------------- cli


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="trace-viz.py",
        description="Render a swath --trace JSONL run as a self-contained HTML page.")
    parser.add_argument("trace", nargs="?", type=Path, help="the --trace JSONL file to render")
    parser.add_argument("-o", "--out", type=Path,
                        help="output HTML path (default: alongside the trace, .html)")
    parser.add_argument("--title", help="page heading (default: the trace file's stem)")
    parser.add_argument("--anonymize", action="store_true",
                        help="withhold every key name; emit positions and counts only")
    parser.add_argument("--self-test", action="store_true", help="run internal checks and exit")
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()
    if args.trace is None:
        parser.error("a trace file is required (or --self-test)")
    if not args.trace.is_file():
        print("trace-viz: no such file: %s" % args.trace, file=sys.stderr)
        return 2

    events, skipped = load_events(args.trace)
    if not events:
        print("trace-viz: %s held no parseable trace events" % args.trace, file=sys.stderr)
        return 1

    versions = {e.get("v") for e in events if "v" in e}
    unknown = {v for v in versions if v != SCHEMA_VERSION}
    if unknown:
        print("trace-viz: warning: unrecognized schema version(s) %s — this reader "
              "understands v%d; fields may be missing"
              % (sorted(unknown), SCHEMA_VERSION), file=sys.stderr)

    model = build_model(events, skipped, args.title or args.trace.stem, args.anonymize)
    out = args.out or args.trace.with_suffix(".html")
    out.write_text(render(model), encoding="utf-8")

    meta = model["meta"]
    print("trace-viz: %s -> %s" % (args.trace, out), file=sys.stderr)
    print("  %s events, %s keys over %s pages, %d workers, %.1fs"
          % (fmt_int(meta["events"]), fmt_int(meta["totalKeys"]), fmt_int(meta["pagesSeen"]),
             meta["workers"], meta["durationMs"] / 1000.0), file=sys.stderr)
    if skipped:
        print("  %d unparseable line(s) skipped" % skipped, file=sys.stderr)
    if meta["keepEvery"] > 1:
        print("  page events downsampled %d:1 for playback (%s of %s plotted); key counts and "
              "the density profile use every page"
              % (meta["keepEvery"], fmt_int(meta["pagesPlotted"]), fmt_int(meta["pagesSeen"])),
              file=sys.stderr)
    return 0


def fmt_int(n):
    return "{:,}".format(n)


if __name__ == "__main__":
    sys.exit(main())
