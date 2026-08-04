#!/usr/bin/env python3
"""trace-viz — turn a swath ``--trace`` JSONL run into a self-contained HTML explainer.

A counter summary tells you what a run did. A trace can tell you the *story* of it: which
of the seed's guesses turned out to be wrong, how wrong, when the engine noticed, and what
it did about it. This reads the trace and writes that story out — the prose findings as
well as the figures, all computed from the events, so the page reads like an explainer of
whichever run it was pointed at rather than a dashboard you have to interpret.

Self-contained (stdlib only), matching ``scripts/ci/check-instrumentation-drift.py`` — the
public repo carries no external Python dependency. The generated page is likewise
self-contained: no CDN, no external images, safe to open from a file:// URL.

Usage:
    scripts/trace/trace-viz.py TRACE.jsonl [-o OUT.html] [--title NAME] [--anonymize]
    scripts/trace/trace-viz.py --self-test

Exit codes: 0 = page written, 1 = the trace yielded nothing renderable, 2 = script error.

The keyspace axis — equal width means equal keys
------------------------------------------------
Keys are byte strings, so "position in the keyspace" needs a projection, and the obvious
ones fail on real data. A raw byte-value axis collapses a deep-prefix bucket onto a
hairline. An axis anchored on range boundaries gives every boundary equal width, so a run
that started from 257 seed ranges renders as 257 identical slivers and width encodes
nothing.

So the axis is the **measured key-mass CDF**: every ``page_committed`` event carries a
cursor and a key count, and sorting those by cursor gives the empirical distribution of
keys across the keyspace. Position is that distribution. Equal screen width therefore means
an equal number of objects, so a range's **area is proportional to the work it did** — a
region holding two thirds of a bucket gets two thirds of the picture, which is exactly the
region a reader needs to see. The cost, stated on the page too: distance on screen is not
byte distance, and an empty stretch of keyspace takes no width at all.

Colour is **seed lineage**, not worker id. Every range traces back through the split
genealogy to the seed range it was carved out of; a reader does not care which of 64
threads did the work, they care which original guess the work descends from.

Sensitivity
-----------
A trace carries real key names on nearly every event (metrics-internals.md §7) — the same
sensitivity class as the listing output itself, and the rendered page inherits it: seed
bounds and split pivots are shown verbatim, because they are most of the value. Pass
``--anonymize`` to emit positions and counts only, with every key name withheld, when the
picture needs to travel further than the trace does.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from pathlib import Path

SCHEMA_VERSION = 1
POS_BYTES = 8
MAX_PAGE_EVENTS = 9000
TIME_BINS = 120

# Event kinds as emitted into the page's compact event stream.
K_SEED, K_CLAIM, K_PAGE, K_SPLIT, K_OWNER_SPLIT, K_DONE, K_FAIL, K_STEAL = range(8)

_ESCAPE_RE = re.compile(r"\\x([0-9a-fA-F]{2})")


# --------------------------------------------------------------------------- keys


def unescape(text):
    """Reverse ``ControlCharEscaper``: ``\\xHH`` back to the character it stands for.

    Exact for every key that does not itself contain the literal sequence
    backslash-x-hexdigit-hexdigit — an ambiguity inherent to the trace format, not
    introduced here.
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
    value, scale = 0.0, 1.0
    for byte in kb[skip:skip + POS_BYTES]:
        scale /= 256.0
        value += byte * scale
    return value


class MassAxis:
    """The measured key-mass CDF, as a projection of key bytes onto [0, 1].

    Built from ``(cursor, keys)`` pairs — one per committed page. Sorting them by cursor
    and accumulating the key counts gives the empirical distribution of objects across the
    keyspace; a key's position is its share of the bucket below it. Equal width is
    therefore equal objects, which is what makes a dense region legible instead of a
    hairline.

    A key falling between two measured cursors is interpolated by byte value inside that
    gap, so the projection stays monotone for the synthesized pivots and seed bounds that
    were never themselves a page cursor.
    """

    def __init__(self, points):
        merged = collections.OrderedDict()
        for cursor, keys in sorted(points):
            merged[cursor] = merged.get(cursor, 0) + keys
        self.keys = list(merged.keys())
        total = float(sum(merged.values())) or 1.0
        self.cum, running = [], 0.0
        for k in self.keys:
            running += merged[k]
            self.cum.append(running / total)
        self.count = len(self.keys)

    def pos(self, kb, default=0.0):
        """Position of a key in [0, 1] — the share of listed objects at or below it."""
        if kb is None or self.count == 0:
            return default
        lo, hi = 0, self.count
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] < kb:
                lo = mid + 1
            else:
                hi = mid
        idx = lo
        if idx >= self.count:
            return 1.0
        if self.keys[idx] == kb:
            return self.cum[idx]
        below = self.cum[idx - 1] if idx > 0 else 0.0
        above = self.cum[idx]
        left = self.keys[idx - 1] if idx > 0 else b""
        right = self.keys[idx]
        skip = len(common_prefix([left, right])) if idx > 0 else 0
        a, b, k = _fraction(left, skip), _fraction(right, skip), _fraction(kb, skip)
        frac = 0.0 if b <= a else min(1.0, max(0.0, (k - a) / (b - a)))
        return below + (above - below) * frac


# --------------------------------------------------------------------------- input


def load_events(path):
    """Parse a trace file leniently.

    Tolerates the two things metrics-internals.md §7 requires a consumer to tolerate: a
    torn final line after a hard kill, and event kinds this reader does not know.
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


def family_of(key):
    """Collapse a key to its top-level directory, generalizing long digit runs.

    ``estofs.20210101/x`` and ``estofs.20210102/y`` become one family. A presentation
    heuristic for grouping the object-count table, not a claim about the bucket's schema —
    the page says as much.
    """
    seg = (key or "").split("/")[0]
    return re.sub(r"\d{6,}", "N", seg) or "(root)"


# --------------------------------------------------------------------------- model


def build_model(events, skipped, title, anonymize):
    """Reduce a trace to the figures AND the findings the page states in words.

    Two passes. The first builds the mass axis, because positions cannot be assigned until
    the whole key distribution is known. The second walks the events in order, replaying
    the run to reconstruct range lifetimes, seed lineage, and the timeline.
    """
    events.sort(key=lambda e: e["ts_ns"])
    t0 = events[0]["ts_ns"]
    duration = max(1.0, (events[-1]["ts_ns"] - t0) / 1e6)

    # ---- pass 1: the key distribution ------------------------------------------
    points = []
    for e in events:
        if e.get("event") == "page_committed" and e.get("cursor"):
            points.append((key_bytes(e["cursor"]), int(e.get("keys", 0) or 0)))
    axis = MassAxis(points)

    def ms(event):
        return round((event["ts_ns"] - t0) / 1e6, 1)

    def at(event, field, default=0.0):
        value = event.get(field)
        return round(axis.pos(key_bytes(value) if value else None, default), 5)

    def show(value):
        return None if (value is None or anonymize) else value

    # ---- pass 2: replay ---------------------------------------------------------
    bounds, open_seg, root_of, seed_info = {}, {}, {}, {}
    segments, stream = [], []
    mechanisms = []
    mech_count, outcome_count, gate_count, families = (collections.Counter(),
                                                       collections.Counter(),
                                                       collections.Counter(),
                                                       collections.Counter())
    mech_pivots = collections.defaultdict(list)
    family_dirs = collections.defaultdict(set)
    family_span = {}
    seed_keys, seed_splits, seed_workers = (collections.Counter(),
                                            collections.Counter(),
                                            collections.defaultdict(set))
    lineage_order = []
    keys_bin = [0.0] * TIME_BINS
    busy_bin = [0] * TIME_BINS
    workers = set()
    claimed = completed = failed = 0
    pages_seen = pages_kept = total_keys = pending_keys = pending_pages = 0
    first_steal_ms = first_split_ms = None
    live = peak_live = 0
    peak_live_ms = 0.0

    page_total = sum(1 for e in events if e.get("event") == "page_committed")
    keep_every = (page_total // MAX_PAGE_EVENTS) + 1 if page_total > MAX_PAGE_EVENTS else 1

    def index(table, value):
        if value not in table:
            table.append(value)
        return table.index(value)

    def close_segment(node, t):
        seg = open_seg.pop(node, None)
        if seg and t > seg[2]:
            segments.append([seg[0], seg[1], seg[2], t, seg[3]])
        return seg

    def tbin(t):
        return min(TIME_BINS - 1, max(0, int(TIME_BINS * t / duration)))

    for event in events:
        kind = event.get("event")
        node = event.get("node_id", -1)
        worker = event.get("worker_id", -1)
        if worker >= 0:
            workers.add(worker)
        t = ms(event)
        b = tbin(t)

        if kind == "seeded":
            lo, hi = at(event, "lo", 0.0), at(event, "hi", 1.0)
            bounds[node] = (lo, hi)
            root_of[node] = node
            lineage_order.append(node)
            seed_info[node] = {"lo": show(event.get("lo")), "hi": show(event.get("hi")),
                               "x0": lo, "x1": hi, "open": event.get("hi") is None}
            stream.append([t, K_SEED, node, lo, hi])

        elif kind == "claimed":
            claimed += 1
            lo, hi = at(event, "lo", 0.0), at(event, "hi", 1.0)
            cur = at(event, "cursor", lo)
            bounds[node] = (lo, hi)
            root_of.setdefault(node, node)
            open_seg[node] = [lo, hi, t, root_of[node]]
            seed_workers[root_of[node]].add(worker)
            live += 1
            if live > peak_live:
                peak_live, peak_live_ms = live, t
            stream.append([t, K_CLAIM, node, worker, lo, cur, hi, root_of[node]])

        elif kind == "page_committed":
            pages_seen += 1
            keys = int(event.get("keys", 0) or 0)
            total_keys += keys
            keys_bin[b] += keys
            busy_bin[b] = max(busy_bin[b], live)
            new = at(event, "cursor", 0.0)
            seed_keys[root_of.get(node, node)] += keys
            if not anonymize and event.get("cursor"):
                fam = family_of(event["cursor"])
                families[fam] += keys
                family_dirs[fam].add(event["cursor"].split("/")[0])
                # Where this family sits on the axis, so a label can be drawn over its own
                # objects rather than in a legend the reader has to cross-reference.
                span = family_span.get(fam)
                family_span[fam] = (min(span[0], new), max(span[1], new)) if span else (new, new)
            pending_keys += keys
            pending_pages += 1
            if pages_seen % keep_every == 0 or event.get("completed"):
                # Carry BOTH the keys and the pages the dropped events stood for, so a
                # downsampled replay still ends on the run's true totals.
                stream.append([t, K_PAGE, node, worker, new, pending_keys, pending_pages])
                pending_keys = pending_pages = 0
                pages_kept += 1

        elif kind in ("split", "owner_split"):
            mech = event.get("mechanism") or "?"
            mech_count[(kind, mech)] += 1
            if len(mech_pivots[mech]) < 3 and event.get("pivot"):
                mech_pivots[mech].append(show(event["pivot"]))
            if first_split_ms is None:
                first_split_ms = t
            child = event.get("child_node_id", -1)
            pivot, hi = at(event, "pivot", 0.0), at(event, "hi", 1.0)
            bounds[child] = (pivot, hi)
            root = root_of.get(node, node)
            root_of[child] = root
            seed_splits[root] += 1
            prior = close_segment(node, t)
            if prior:
                open_seg[node] = [prior[0], pivot, t, prior[3]]
            stream.append([t, K_SPLIT if kind == "split" else K_OWNER_SPLIT,
                           node, child, worker, pivot, hi, index(mechanisms, mech), root])

        elif kind in ("completed", "failed"):
            if kind == "completed":
                completed += 1
            else:
                failed += 1
            close_segment(node, t)
            live = max(0, live - 1)
            stream.append([t, K_DONE if kind == "completed" else K_FAIL, node])

        elif kind == "steal_attempt":
            outcome_count["%s / %s" % (event.get("outcome") or "?",
                                       event.get("reason") or "?")] += 1
            if first_steal_ms is None:
                first_steal_ms = t
            stream.append([t, K_STEAL, worker, 0])

        elif kind == "owner_split_decision":
            gate_count[event.get("reason") or "?"] += 1

    if pending_keys or pending_pages:
        stream.append([ms(events[-1]), K_PAGE, -1, -1, 1.0, pending_keys, pending_pages])
    end = ms(events[-1])
    for node in list(open_seg):
        close_segment(node, end)

    # ---- seed lineage: the figure the page is built around ----------------------
    seeds = []
    for node in lineage_order:
        info = seed_info[node]
        seeds.append({"id": node, "lo": info["lo"], "hi": info["hi"],
                      "open": info["open"], "x0": info["x0"], "x1": info["x1"],
                      "keys": int(seed_keys.get(node, 0)),
                      "splits": int(seed_splits.get(node, 0)),
                      "workers": len(seed_workers.get(node, ()))})
    by_mass = sorted(seeds, key=lambda s: -s["keys"])
    seeded_total = sum(s["keys"] for s in seeds) or 1
    top = by_mass[0] if by_mass else None
    tidy = [s["keys"] for s in by_mass[1:] if s["keys"] > 0]
    median = sorted(tidy)[len(tidy) // 2] if tidy else 0

    # Lineage colour slots: the heaviest few get their own, everything else shares one.
    palette_ids = [s["id"] for s in by_mass[:5]]
    lineage_slot = {nid: i for i, nid in enumerate(palette_ids)}

    total_splits = sum(mech_count.values())
    owner_splits = sum(n for (k, m), n in mech_count.items() if k == "owner_split")
    uniform = sum(n for (k, m), n in mech_count.items()
                  if m in ("midpoint", "byte_midpoint"))
    blocked = sum(gate_count.values()) - sum(
        n for r, n in gate_count.items() if r in ("self_published",))

    findings = {
        "seedCount": len(seeds),
        "topKeys": top["keys"] if top else 0,
        "topShare": round(100.0 * top["keys"] / seeded_total, 1) if top else 0.0,
        "topSplits": top["splits"] if top else 0,
        "topWorkers": top["workers"] if top else 0,
        "topLo": top["lo"] if top else None,
        "topHi": top["hi"] if top else None,
        "medianKeys": median,
        "ratio": int(top["keys"] / median) if (top and median) else 0,
        "emptySeeds": sum(1 for s in seeds if s["keys"] == 0),
        "firstStealMs": first_steal_ms,
        "firstSplitMs": first_split_ms,
        "peakLive": peak_live, "peakLiveMs": peak_live_ms,
        "totalSplits": total_splits, "ownerSplits": owner_splits,
        "uniformSplits": uniform,
        "gateBlocked": blocked, "gateTotal": sum(gate_count.values()),
        "claimed": claimed, "completed": completed, "failed": failed,
        "ledgerOk": (len(seeds) + total_splits == claimed == completed) and failed == 0,
        "reqPer1k": round(1000.0 * pages_seen / total_keys, 3) if total_keys else None,
        "floorPages": (total_keys + 999) // 1000 if total_keys else 0,
    }

    peak_rate = max(keys_bin) or 1.0
    return {
        "meta": {"title": title, "events": len(events), "skippedLines": skipped,
                 "pagesSeen": pages_seen, "pagesPlotted": pages_kept,
                 "keepEvery": keep_every, "totalKeys": total_keys,
                 "workers": len(workers), "durationMs": round(duration, 1),
                 "cdfPoints": axis.count, "anonymized": anonymize},
        "findings": findings,
        "seeds": by_mass[:14],
        "lineage": lineage_slot,
        "families": [{"name": k, "dirs": len(family_dirs[k]), "keys": int(v),
                      "x0": round(family_span.get(k, (0.0, 0.0))[0], 5),
                      "x1": round(family_span.get(k, (0.0, 0.0))[1], 5)}
                     for k, v in families.most_common(10)],
        "rate": [round(v / peak_rate, 4) for v in keys_bin],
        "busy": busy_bin,
        "peakRate": round(peak_rate * TIME_BINS / (duration / 1000.0), 0) if duration else 0,
        "mechanisms": mechanisms,
        "mechRows": sorted([{"kind": k, "name": m, "n": n, "pivots": mech_pivots.get(m, [])}
                            for (k, m), n in mech_count.items()], key=lambda r: -r["n"]),
        "outcomeRows": sorted([{"name": k, "n": v} for k, v in outcome_count.items()],
                              key=lambda r: -r["n"]),
        "gateRows": sorted([{"name": k, "n": v} for k, v in gate_count.items()],
                           key=lambda r: -r["n"])[:6],
        "stream": stream,
        "segments": segments,
    }


# --------------------------------------------------------------------------- output


def render(model):
    return TEMPLATE.replace("/*__MODEL__*/null",
                            json.dumps(model, separators=(",", ":")))


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
    --accent-sub:#0D6E6014; --signal-sub:#9A671018; --alert-sub:#A33F2814;
    --mono:ui-monospace,"SFMono-Regular","Cascadia Mono",Menlo,Consolas,monospace;
    --serif:Charter,"Bitstream Charter","Sitka Text",Cambria,Georgia,serif;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --paper:#0E1312; --panel:#161D1B; --deep:#1E2724;
      --ink:#E7EBE3; --ink2:#9BA69F; --ink3:#74807A; --rule:#29332F;
      --accent:#56C9B1; --signal:#E0AC55; --alert:#E4826A;
      --accent-sub:#56C9B11A; --signal-sub:#E0AC551A; --alert-sub:#E4826A1A;
    }
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--paper); color:var(--ink); font-family:var(--serif);
         font-size:16.5px; line-height:1.6; overflow-x:hidden; }
  .wrap { max-width:64rem; margin:0 auto; padding:2.4rem 1.5rem 5rem; }
  .col { max-width:36rem; }
  h1 { font-family:var(--mono); font-weight:500; font-size:clamp(1.55rem,4vw,2.25rem);
       letter-spacing:-.035em; margin:0 0 .35rem; text-wrap:balance; }
  .sub { font-family:var(--mono); font-size:.72rem; color:var(--ink3); margin-bottom:1.5rem; }
  h2 { font-family:var(--mono); font-weight:500; font-size:1.22rem; letter-spacing:-.025em;
       margin:0; text-wrap:balance; }
  .sec { padding-top:2.6rem; }
  .sec-head { display:flex; align-items:baseline; gap:.9rem; border-top:2px solid var(--ink);
              padding-top:.6rem; margin-bottom:1rem; }
  .eyebrow { font-family:var(--mono); font-size:.63rem; letter-spacing:.15em;
             text-transform:uppercase; color:var(--ink3); white-space:nowrap; }
  p { margin:0 0 1em; }
  code { font-family:var(--mono); font-size:.85em; background:var(--panel); padding:.08em .32em;
         border-radius:3px; border:1px solid var(--rule); word-break:break-all; }
  b.hl { box-shadow: inset 0 -.4em 0 var(--accent-sub); font-weight:600; }

  .tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(8.2rem,1fr));
           gap:1px; background:var(--rule); border:1px solid var(--rule); margin-bottom:1.5rem; }
  .tiles > div { background:var(--paper); padding:.6rem .85rem; }
  .tiles dt { font-family:var(--mono); font-size:.59rem; letter-spacing:.13em;
              text-transform:uppercase; color:var(--ink3); margin-bottom:.12rem; }
  .tiles dd { margin:0; font-family:var(--mono); font-size:1.05rem;
              font-variant-numeric:tabular-nums; letter-spacing:-.02em; }

  .finding { border-left:3px solid var(--alert); background:var(--alert-sub);
             padding:.95rem 1.15rem; margin:1.2rem 0; }
  .finding .lbl { font-family:var(--mono); font-size:.62rem; letter-spacing:.14em;
                  text-transform:uppercase; color:var(--alert); display:block; margin-bottom:.3rem; }
  .finding p { margin:0 0 .55em; max-width:46rem; } .finding p:last-child { margin-bottom:0; }
  .finding.calm { border-left-color:var(--accent); background:var(--accent-sub); }
  .finding.calm .lbl { color:var(--accent); }

  .card { background:var(--panel); border:1px solid var(--rule); border-radius:2px;
          padding:1rem 1.15rem; margin:1.1rem 0; overflow-x:auto; }
  .card h3 { font-family:var(--mono); font-size:.68rem; letter-spacing:.13em;
             text-transform:uppercase; color:var(--ink3); margin:0 0 .6rem; font-weight:600; }
  canvas { display:block; width:100%; }
  .row { display:grid; grid-template-columns:minmax(8rem,17rem) 1fr auto; gap:.7rem;
         align-items:center; font-family:var(--mono); font-size:.74rem; padding:.2rem 0; }
  .row .nm { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; color:var(--ink2); }
  .row .tr { background:var(--deep); height:13px; position:relative; }
  .row .fl { position:absolute; inset:0 auto 0 0; }
  .row .vl { font-variant-numeric:tabular-nums; color:var(--ink2); white-space:nowrap; }

  table { border-collapse:collapse; width:100%; font-size:.82rem; }
  th,td { text-align:left; padding:.38rem .8rem .38rem 0; border-bottom:1px solid var(--rule);
          vertical-align:top; }
  th { font-family:var(--mono); font-size:.59rem; letter-spacing:.12em; text-transform:uppercase;
       color:var(--ink3); }
  td.k { font-family:var(--mono); font-size:.75rem; white-space:nowrap; }
  td.n { font-family:var(--mono); text-align:right; font-variant-numeric:tabular-nums; }
  td.pv { font-family:var(--mono); font-size:.71rem; color:var(--ink2); word-break:break-all; }
  td.pv div { padding:.05rem 0; }
  tr:last-child td { border-bottom:0; }

  .bar { display:flex; align-items:center; gap:.75rem; flex-wrap:wrap; padding:.5rem 0 .7rem; }
  button { font-family:var(--mono); font-size:.7rem; padding:.28rem .68rem; background:var(--paper);
           color:var(--ink); border:1px solid var(--rule); border-radius:2px; cursor:pointer; }
  button:hover { border-color:var(--ink2); }
  button:focus-visible { outline:2px solid var(--accent); outline-offset:2px; }
  input[type=range] { flex:1 1 9rem; min-width:6rem; accent-color:var(--accent); }
  .clock { font-family:var(--mono); font-size:.71rem; color:var(--ink2);
           font-variant-numeric:tabular-nums; white-space:nowrap; }
  .cap { font-family:var(--mono); font-size:.7rem; color:var(--ink2); line-height:1.55;
         max-width:60ch; margin:.6rem 0 0; }
  .key { display:flex; flex-wrap:wrap; gap:.9rem; font-family:var(--mono); font-size:.7rem;
         color:var(--ink2); margin-top:.6rem; }
  .key span { display:inline-flex; align-items:center; gap:.35rem; }
  .key i { width:.7rem; height:.7rem; display:inline-block; border-radius:2px; }
  footer { margin-top:3rem; padding-top:1.3rem; border-top:2px solid var(--ink);
           font-family:var(--mono); font-size:.69rem; color:var(--ink2); line-height:1.7; }
  @media (prefers-reduced-motion: reduce) { * { animation-duration:.001ms !important; } }
</style>
</head>
<body>
<div class="wrap">
  <h1 id="title">swath run trace</h1>
  <div class="sub" id="subtitle"></div>
  <dl class="tiles" id="tiles"></dl>
  <div id="findings"></div>

  <div class="sec">
    <div class="sec-head"><span class="eyebrow">§1</span><h2>The guesses, and how they turned out</h2></div>
    <div class="col">
      <p>Before any worker starts, swath spends one <code>delimiter=/</code> request asking S3
      where the directories are, and turns the answer into starting ranges. A directory name says
      nothing about how many objects are behind it, so this is a guess — and it is the only free
      information the engine will ever get.</p>
      <p id="seed-lede"></p>
    </div>
    <div class="card">
      <h3>Seed ranges, by objects actually found behind them</h3>
      <div id="seed-bars"></div>
      <p class="cap" id="seed-cap"></p>
    </div>
    <div class="card" id="fam-card">
      <h3>Objects by top-level directory</h3>
      <div id="fam-bars"></div>
      <p class="cap">Grouped by first path segment, with runs of six or more digits generalized so
      dated sibling directories collapse into one row — a grouping choice for this table only.</p>
    </div>
  </div>

  <div class="sec">
    <div class="sec-head"><span class="eyebrow">§2</span><h2>What the run actually felt like</h2></div>
    <div class="col"><p id="time-lede"></p></div>
    <div class="card">
      <h3>Objects listed per second, and live ranges, over the run</h3>
      <canvas id="rate" role="img" aria-label="Two series over the run's duration: objects listed per second, and the number of live ranges."></canvas>
      <p class="cap" id="rate-cap"></p>
    </div>
    <div class="card" id="gate-card">
      <h3>Owner-split gate decisions</h3>
      <table><thead><tr><th>Gate outcome</th><th style="text-align:right">Times</th></tr></thead>
      <tbody id="gate-table"></tbody></table>
      <p class="cap" id="gate-cap"></p>
    </div>
  </div>

  <div class="sec">
    <div class="sec-head"><span class="eyebrow">§3</span><h2>Most of these keys do not exist</h2></div>
    <div class="col">
      <p id="mech-lede"></p>
      <p>To split a range swath needs a boundary key, and it does <b>not</b> need one that exists —
      <code>start-after</code> accepts any byte string. The pivots below are verbatim from this
      run. The odd-looking ones are synthesized coordinates, not object names: a trailing
      <code>:</code> is one byte past <code>9</code>, so it means "past every key ending in a
      digit here".</p>
    </div>
    <div class="card">
      <table><thead><tr><th>Mechanism</th><th style="text-align:right">Splits</th><th>Sample pivots, verbatim</th></tr></thead>
      <tbody id="mech-table"></tbody></table>
      <p class="cap" id="mech-cap"></p>
    </div>
    <div class="card" id="steal-card">
      <h3>How each thief attempt ended</h3>
      <table><tbody id="steal-table"></tbody></table>
      <p class="cap">Many retries against few committed children means idle workers were probing
      without finding anything splittable.</p>
    </div>
  </div>

  <div class="sec">
    <div class="sec-head"><span class="eyebrow">§4</span><h2>The whole run, on one map</h2></div>
    <div class="col">
      <p>Horizontal is the keyspace weighted by objects — <b class="hl">equal width means an equal
      number of objects</b> — and vertical is time running downward. Each rectangle is one range
      over the slice it owned and the time it owned it, coloured by which original seed guess it
      descends from. Because width is objects, a rectangle's <em>area</em> is roughly the work it
      did.</p>
      <p><b>Wide and short</b> is a big slice eaten fast. <b>A tall thin column</b> is a serial
      tail — a dense region that could not be divided, outliving everything around it.</p>
    </div>
    <div class="card">
      <canvas id="static" role="img" aria-label="Keyspace over time: each range drawn as a rectangle whose width is the share of objects it owned and whose height is how long it owned them, coloured by seed lineage."></canvas>
      <div class="key" id="static-key"></div>
      <p class="cap" id="static-cap"></p>
    </div>
  </div>

  <div class="sec">
    <div class="sec-head"><span class="eyebrow">§5</span><h2>Watch it happen</h2></div>
    <div class="col"><p>The same events replayed in order, on the same axis. Bands are live ranges,
    filled to each worker's cursor; ticks are split pivots as they commit.</p></div>
    <div class="card">
      <div class="bar">
        <button id="play" type="button">Pause</button>
        <button id="restart" type="button">Restart</button>
        <button id="speed" type="button">1&times;</button>
        <input id="scrub" type="range" min="0" max="1000" value="0" aria-label="Scrub through the run">
        <span class="clock" id="clock">0s</span>
      </div>
      <canvas id="map" role="img" aria-label="Replay of the run: live ranges across the object-weighted keyspace, with split pivots marked as they commit."></canvas>
      <dl class="tiles" id="live-tiles" style="margin-top:.8rem"></dl>
    </div>
  </div>

  <div class="sec">
    <div class="sec-head"><span class="eyebrow">§6</span><h2>The ledger</h2></div>
    <div class="col"><p id="ledger-lede"></p></div>
  </div>

  <footer id="provenance"></footer>
</div>

<script>
(function () {
  "use strict";
  var M = /*__MODEL__*/null;
  if (!M) { return; }
  var meta=M.meta, F=M.findings, stream=M.stream;
  var K_SEED=0,K_CLAIM=1,K_PAGE=2,K_SPLIT=3,K_OWNER=4,K_DONE=5,K_FAIL=6,K_STEAL=7;

  var LIN_L=["#A33F28","#0D6E60","#2E7C86","#6E7B3C","#38617E"], REST_L="#9AA79F";
  var LIN_D=["#E4826A","#56C9B1","#6FBBD0","#BCC076","#7FA8CE"], REST_D="#5E6B65";
  function css(n){ return getComputedStyle(document.documentElement).getPropertyValue(n).trim(); }
  function dark(){ return window.matchMedia&&window.matchMedia("(prefers-color-scheme: dark)").matches; }
  function lineage(root){
    var slot=M.lineage[root]; var t=dark()?LIN_D:LIN_L;
    return slot===undefined ? (dark()?REST_D:REST_L) : t[slot];
  }
  function fmt(n){ return Math.round(n).toLocaleString("en-US"); }
  function compact(n){ return n>=1e6 ? (n/1e6).toFixed(1)+"M"
                     : n>=1e3 ? Math.round(n/1e3)+"k" : String(Math.round(n)); }
  function secs(ms){ return ms>=60000 ? Math.floor(ms/60000)+"m"+String(Math.round(ms%60000/1000)).padStart(2,"0")+"s"
                                      : (ms/1000).toFixed(ms<10000?1:0)+"s"; }
  function el(t,c,x){ var e=document.createElement(t); if(c)e.className=c;
    if(x!==undefined)e.textContent=x; return e; }
  // Never round a near-miss up to a flat 100% — "100%" next to a nonzero remainder reads as a bug.
  function pct(a,b){ if(!b) return 0; var v=100*a/b;
    return (v>99.5&&a<b)||(v<0.5&&a>0) ? v.toFixed(1) : Math.round(v); }

  document.getElementById("title").textContent = meta.title||"swath run trace";
  document.getElementById("subtitle").textContent =
    [fmt(meta.totalKeys)+" objects", fmt(meta.pagesSeen)+" LIST requests",
     meta.workers+" workers", secs(meta.durationMs)].join("  ·  ");

  var tw=document.getElementById("tiles");
  [["Objects",fmt(meta.totalKeys)],["Wall time",secs(meta.durationMs)],
   ["LIST requests",fmt(meta.pagesSeen)],
   ["Requests / 1k",F.reqPer1k===null?"—":F.reqPer1k.toFixed(2)],
   ["Workers",fmt(meta.workers)],["Splits",fmt(F.totalSplits)]].forEach(function(t){
    var d=el("div"); d.appendChild(el("dt",null,t[0])); d.appendChild(el("dd",null,t[1])); tw.appendChild(d); });

  // ---- findings, written from the data -----------------------------------
  var fw=document.getElementById("findings");
  function finding(cls,label,paras){ var d=el("div","finding"+(cls?" "+cls:""));
    d.appendChild(el("span","lbl",label));
    paras.forEach(function(p){ d.appendChild(el("p",null,p)); }); fw.appendChild(d); }

  if (F.topShare>=25 && F.seedCount>1) {
    finding("","The guess that was wrong",[
      "The seed produced "+fmt(F.seedCount)+" ranges. One of them held "+fmt(F.topKeys)+
      " objects — "+F.topShare+"% of everything this run listed"+
      (F.ratio>4 ? ", about "+fmt(F.ratio)+"× the median of the rest" : "")+
      ". Nothing about its bounds said so; the only way to find out was to list it.",
      F.topSplits>0
        ? "Work-stealing took it apart at runtime: "+fmt(F.topSplits)+" of the run's "+
          fmt(F.totalSplits)+" splits ("+pct(F.topSplits,F.totalSplits)+
          "%) were carved out of that one range and its descendants, and "+fmt(F.topWorkers)+
          " of the "+fmt(meta.workers)+" workers ended up inside it."
        : "No split ever divided it, so it drained on a single worker for as long as it took."
    ]);
  }
  if (F.firstStealMs!==null && F.peakLive>0) {
    finding("calm","Seeding alone carried the start",[
      "Every worker was busy within "+secs(F.peakLiveMs)+" ("+fmt(F.peakLive)+
      " ranges live at once), and the first steal attempt did not happen until "+
      secs(F.firstStealMs)+". For that whole stretch there was nothing to steal — every worker "+
      "had its own structurally disjoint slice, bought with a single request."]);
  }
  if (F.totalSplits>0) {
    finding("calm","Pivots came from evidence, not from the middle",[
      fmt(F.totalSplits-F.uniformSplits)+" of "+fmt(F.totalSplits)+" splits ("+
      pct(F.totalSplits-F.uniformSplits,F.totalSplits)+"%) placed their pivot from something the "+
      "run had observed — drained density, a real directory boundary, the alphabet seen in "+
      "returned keys. Only "+fmt(F.uniformSplits)+" used the plain byte midpoint."]);
  }

  // ---- bar lists ----------------------------------------------------------
  function bars(host, rows, total, colorFn){
    var max=rows.reduce(function(a,r){return Math.max(a,r.value);},0)||1;
    rows.forEach(function(r,i){
      var row=el("div","row"); row.appendChild(el("div","nm",r.label));
      var tr=el("div","tr"), fl=el("div","fl");
      fl.style.width=(100*r.value/max).toFixed(2)+"%";
      fl.style.background=colorFn?colorFn(r,i):css("--accent");
      fl.style.opacity=".72"; tr.appendChild(fl); row.appendChild(tr);
      row.appendChild(el("div","vl",fmt(r.value)+(total?"  ·  "+(100*r.value/total).toFixed(1)+"%":"")));
      host.appendChild(row);
    });
  }
  var seedTotal=meta.totalKeys||1;
  bars(document.getElementById("seed-bars"),
       M.seeds.map(function(s){
         var nm = s.lo===null ? ("range at "+(100*s.x0).toFixed(1)+"%")
                : ((s.lo===""?"⊥":s.lo)+" → "+(s.open?"∅ (open frontier)":(s.hi===null?"?":s.hi)));
         return {label:nm, value:s.keys, id:s.id};
       }), seedTotal, function(r){ return lineage(r.id); });
  document.getElementById("seed-cap").textContent =
    "The "+M.seeds.length+" heaviest of "+fmt(F.seedCount)+" seed ranges, labelled by their "+
    "bounds. Weight includes every range later split off it, traced through the split "+
    "genealogy — so this is the work each original guess was really responsible for. "+
    fmt(F.emptySeeds)+" seed ranges held no objects at all.";
  document.getElementById("seed-lede").textContent =
    "This run's seed produced "+fmt(F.seedCount)+" ranges. They were not equal: the heaviest held "+
    F.topShare+"% of the objects"+(F.medianKeys? ", against a median of "+fmt(F.medianKeys)+
    " for the rest":"")+", and "+fmt(F.emptySeeds)+" were empty.";

  if (M.families.length && !meta.anonymized) {
    var famTotal=M.families.reduce(function(a,f){return a+f.keys;},0)||1;
    bars(document.getElementById("fam-bars"), M.families.map(function(f){
      return {label:f.name+(f.dirs>1?"  ("+fmt(f.dirs)+" dirs)":""), value:f.keys};
    }), famTotal, null);
  } else { document.getElementById("fam-card").hidden=true; }

  // ---- rate + concurrency chart ------------------------------------------
  var rc=document.getElementById("rate"), rx=rc.getContext("2d");
  function drawRate(){
    var dpr=window.devicePixelRatio||1, w=Math.max(300,rc.getBoundingClientRect().width), h=150;
    rc.style.height=h+"px"; rc.width=Math.round(w*dpr); rc.height=Math.round(h*dpr);
    rx.setTransform(dpr,0,0,dpr,0,0); rx.clearRect(0,0,w,h);
    var L=58,R=12,T=22,B=22, iw=w-L-R, ih=h-T-B, n=M.rate.length;
    rx.font="10px "+css("--mono"); rx.strokeStyle=css("--rule"); rx.lineWidth=1;
    rx.beginPath(); rx.moveTo(L,T+ih); rx.lineTo(L+iw,T+ih); rx.stroke();
    rx.beginPath(); rx.moveTo(L,T+ih);
    for(var i=0;i<n;i++){ rx.lineTo(L+iw*(i+.5)/n, T+ih-M.rate[i]*ih*0.92); }
    rx.lineTo(L+iw,T+ih); rx.closePath();
    rx.fillStyle=css("--accent"); rx.globalAlpha=.22; rx.fill(); rx.globalAlpha=1;
    rx.strokeStyle=css("--accent"); rx.lineWidth=1.4; rx.beginPath();
    for(i=0;i<n;i++){ var x=L+iw*(i+.5)/n, y=T+ih-M.rate[i]*ih*0.92;
      if(i===0) rx.moveTo(x,y); else rx.lineTo(x,y); } rx.stroke();
    var mb=Math.max.apply(null,M.busy)||1;
    rx.strokeStyle=css("--signal"); rx.lineWidth=1.2; rx.setLineDash([3,2]); rx.beginPath();
    for(i=0;i<n;i++){ x=L+iw*(i+.5)/n; y=T+ih-(M.busy[i]/mb)*ih*0.92;
      if(i===0) rx.moveTo(x,y); else rx.lineTo(x,y); } rx.stroke(); rx.setLineDash([]);
    rx.fillStyle=css("--ink3"); rx.textAlign="right";
    rx.fillText(compact(M.peakRate)+"/s", L-6, T+8); rx.fillText("0", L-6, T+ih+3);
    rx.textAlign="left"; rx.fillText("0s", L, T+ih+16);
    rx.textAlign="right"; rx.fillText(secs(meta.durationMs), L+iw, T+ih+16);
    rx.textAlign="left"; rx.fillStyle=css("--accent"); rx.fillText("objects/s", L, T-9);
    rx.fillStyle=css("--signal"); rx.fillText("live ranges (peak "+fmt(mb)+")", L+66, T-9);
  }
  document.getElementById("rate-cap").textContent =
    "Binned into "+M.rate.length+" intervals across the run. Peak observed rate ~"+
    fmt(M.peakRate)+" objects/s.";
  document.getElementById("time-lede").textContent =
    "Parallelism is not constant. Watch for a trough: it is what a run looks like when the easy, "+
    "well-guessed ranges have all finished and one badly-guessed one is still going — the moment "+
    "stealing has to earn its keep."+
    (F.firstSplitMs!==null? " The first split of this run committed at "+secs(F.firstSplitMs)+".":"");

  var gt=document.getElementById("gate-table");
  M.gateRows.forEach(function(r){ var tr=el("tr");
    tr.appendChild(el("td","k",r.name)); tr.appendChild(el("td","n",fmt(r.n))); gt.appendChild(tr); });
  if (!M.gateRows.length) { document.getElementById("gate-card").hidden=true; }
  else {
    document.getElementById("gate-cap").textContent =
      "The owner-side split gate was consulted "+fmt(F.gateTotal)+" times and mostly said no. "+
      "That is the design working: the split mechanism is global, but its trigger is local and "+
      "demand-driven, so while every worker is busy there is no reason to shed work and the gate "+
      "stays shut.";
  }

  // ---- mechanisms ---------------------------------------------------------
  var mt=document.getElementById("mech-table");
  M.mechRows.forEach(function(r){
    var tr=el("tr"), td=el("td","k"); td.textContent=r.name;
    if(r.kind==="owner_split"){ td.appendChild(el("div","cap","owner-side")); }
    tr.appendChild(td); tr.appendChild(el("td","n",fmt(r.n)));
    var pv=el("td","pv");
    (r.pivots||[]).forEach(function(p){ if(p!==null) pv.appendChild(el("div",null,p)); });
    if(!pv.childNodes.length) pv.textContent = meta.anonymized?"(withheld)":"—";
    tr.appendChild(pv); mt.appendChild(tr);
  });
  document.getElementById("mech-cap").textContent =
    "Ordered by how often each rung of the pivot cascade fired. Owner-side splits are placed by a "+
    "range's own worker ahead of its cursor; the rest are taken by idle thieves.";
  document.getElementById("mech-lede").textContent =
    "This run committed "+fmt(F.totalSplits)+" splits. "+fmt(F.ownerSplits)+
    " were owner-side — a range's own worker carving ahead of its cursor, because on a dense "+
    "fast-draining range a thief loses the race to the pivot — and "+fmt(F.totalSplits-F.ownerSplits)+
    " were taken by idle thieves.";

  var st=document.getElementById("steal-table");
  M.outcomeRows.forEach(function(r){ var tr=el("tr");
    tr.appendChild(el("td","k",r.name)); tr.appendChild(el("td","n",fmt(r.n))); st.appendChild(tr); });
  if(!M.outcomeRows.length) document.getElementById("steal-card").hidden=true;

  // ---- static map ---------------------------------------------------------
  var sv=document.getElementById("static"), sc=sv.getContext("2d");
  var PL=48,PR=14,PT=14,PB=26;
  function drawStatic(){
    var dpr=window.devicePixelRatio||1, w=Math.max(320,sv.getBoundingClientRect().width);
    var h=Math.min(560,Math.max(260,w*0.52));
    sv.style.height=h+"px"; sv.width=Math.round(w*dpr); sv.height=Math.round(h*dpr);
    sc.setTransform(dpr,0,0,dpr,0,0); sc.clearRect(0,0,w,h);
    var iw=w-PL-PR, ih=h-PT-PB, dur=Math.max(1,meta.durationMs);
    function sx(p){ return PL+p*iw; } function sy(t){ return PT+(t/dur)*ih; }
    sc.font="10px "+css("--mono"); sc.strokeStyle=css("--rule"); sc.lineWidth=1; sc.fillStyle=css("--ink3");
    for(var i=0;i<=6;i++){ var t=(dur/6)*i, y=sy(t);
      sc.beginPath(); sc.moveTo(PL,y); sc.lineTo(PL+iw,y); sc.stroke();
      sc.textAlign="right"; sc.fillText(secs(t),PL-6,y+3); }
    sc.textAlign="center";
    for(i=0;i<=4;i++){ sc.fillText((i*25)+"%",sx(i/4),PT+ih+16); }
    M.segments.forEach(function(s){
      var x0=sx(s[0]),x1=sx(s[1]),y0=sy(s[2]),y1=sy(s[3]);
      var bw=Math.max(1,x1-x0), bh=Math.max(1,y1-y0), col=lineage(s[4]);
      sc.globalAlpha=.34; sc.fillStyle=col; sc.fillRect(x0,y0,bw,bh);
      sc.globalAlpha=1; sc.strokeStyle=col; sc.lineWidth=.5; sc.strokeRect(x0+.25,y0+.25,bw,bh);
    });
    stream.forEach(function(e){
      if(e[1]!==K_SPLIT&&e[1]!==K_OWNER) return;
      sc.beginPath(); sc.arc(sx(e[5]),sy(e[0]),e[1]===K_OWNER?2.6:2.0,0,6.284);
      sc.fillStyle=css("--signal"); sc.globalAlpha=.75; sc.fill(); sc.globalAlpha=1;
    });
    if(!M.segments.length){ sc.textAlign="center"; sc.fillStyle=css("--ink3");
      sc.fillText("no completed range in this trace",w/2,PT+ih/2); }
  }
  var ky=document.getElementById("static-key");
  M.seeds.slice(0,5).forEach(function(s){
    if(M.lineage[s.id]===undefined) return;
    var sp=el("span"); var sw=el("i"); sw.style.background=lineage(s.id); sp.appendChild(sw);
    sp.appendChild(document.createTextNode(
      (s.lo===null? "range at "+(100*s.x0).toFixed(0)+"%" : (s.lo===""?"⊥":s.lo))+
      "  ·  "+(100*s.keys/(meta.totalKeys||1)).toFixed(1)+"%"));
    ky.appendChild(sp);
  });
  var rest=el("span"); var rw=el("i"); rw.style.background=dark()?REST_D:REST_L;
  rest.appendChild(rw); rest.appendChild(document.createTextNode("all other seed lineages"));
  ky.appendChild(rest);
  document.getElementById("static-cap").textContent =
    fmt(M.segments.length)+" range-lifetimes drawn, coloured by seed lineage; ochre dots are split "+
    "pivots. Horizontal position is the share of objects below a key, measured from this run's "+
    fmt(meta.cdfPoints)+" page commits — so equal width is equal objects, and byte distance is "+
    "NOT preserved.";

  // ---- replay -------------------------------------------------------------
  var nodes,live,marks,cursor,stats,clockMs=0,running=true,rate=1,last=0;
  function reset(){ nodes={}; live=0; marks=[]; cursor=0;
    stats={keys:0,pages:0,splits:0,steals:0,busy:{}}; }
  function apply(e){
    var k=e[1],n;
    if(k===K_SEED){ nodes[e[2]]={lo:e[3],hi:e[4],cur:e[3],root:e[2],state:"pending"}; }
    else if(k===K_CLAIM){ n=nodes[e[2]]||(nodes[e[2]]={});
      n.lo=e[4]; n.hi=e[6]; n.cur=e[5]; n.worker=e[3]; n.root=e[7]; n.state="live";
      stats.busy[e[3]]=(stats.busy[e[3]]||0)+1; }
    else if(k===K_PAGE){ stats.keys+=e[5]; stats.pages+=(e[6]||1); n=nodes[e[2]]; if(n){ n.cur=e[4]; } }
    else if(k===K_SPLIT||k===K_OWNER){ n=nodes[e[2]]; if(n) n.hi=e[5];
      nodes[e[3]]={lo:e[5],hi:e[6],cur:e[5],root:e[8],state:"pending"};
      marks.push({x:e[5],age:0,owner:k===K_OWNER}); stats.splits++; }
    else if(k===K_DONE||k===K_FAIL){ n=nodes[e[2]];
      if(n){ n.state=k===K_DONE?"done":"failed";
        if(n.worker>=0){ stats.busy[n.worker]--; if(stats.busy[n.worker]<=0) delete stats.busy[n.worker]; } } }
    else if(k===K_STEAL){ stats.steals++; }
  }
  function seekTo(t){ if(t<clockMs) reset();
    while(cursor<stream.length && stream[cursor][0]<=t) apply(stream[cursor++]);
    live=0; for(var id in nodes){ var s=nodes[id].state; if(s==="live"||s==="pending") live++; } }

  var cv=document.getElementById("map"), ctx=cv.getContext("2d");
  var W=0,H=0,PAD=22;
  function resize(){ var dpr=window.devicePixelRatio||1,r=cv.getBoundingClientRect();
    W=Math.max(300,r.width); H=W<640?190:160; cv.style.height=H+"px";
    cv.width=Math.round(W*dpr); cv.height=Math.round(H*dpr); ctx.setTransform(dpr,0,0,dpr,0,0); }
  function px(x){ return PAD+x*(W-PAD*2); }
  function draw(){
    ctx.clearRect(0,0,W,H); ctx.font="10px "+css("--mono");
    ctx.fillStyle=css("--ink3"); ctx.textAlign="left";
    ctx.fillText("keyspace, weighted by objects →",px(0),12);
    var top=34,bh=30,doneY=top,actY=top+8;
    ctx.strokeStyle=css("--rule");
    ctx.beginPath(); ctx.moveTo(px(0),top-6); ctx.lineTo(px(1),top-6); ctx.stroke();
    for(var id in nodes){
      var n=nodes[id],x0=px(n.lo),x1=px(n.hi),w=Math.max(1,x1-x0);
      if(n.state==="done"||n.state==="failed"){
        ctx.globalAlpha=.4; ctx.fillStyle=n.state==="done"?lineage(n.root):css("--alert");
        ctx.fillRect(x0,doneY,w-.5,4); ctx.globalAlpha=1;
      } else if(n.state==="pending"){
        ctx.strokeStyle=css("--ink3"); ctx.lineWidth=1; ctx.setLineDash([3,2]);
        ctx.strokeRect(x0+.5,actY+.5,Math.max(2,w)-1,bh-1); ctx.setLineDash([]);
      } else {
        var c=lineage(n.root),xc=px(n.cur);
        ctx.strokeStyle=c; ctx.lineWidth=1.1; ctx.strokeRect(x0+.5,actY+.5,Math.max(2,w)-1,bh-1);
        ctx.fillStyle=c; ctx.globalAlpha=.32; ctx.fillRect(x0+1,actY+1,Math.max(0,xc-x0)-1,bh-2);
        ctx.globalAlpha=1; ctx.fillRect(Math.max(x0,xc-1),actY,2,bh);
      }
    }
    for(var m=marks.length-1;m>=0;m--){ var k=marks[m]; k.age++;
      if(k.age>34){ marks.splice(m,1); continue; }
      ctx.globalAlpha=1-k.age/34; ctx.strokeStyle=css("--signal"); ctx.lineWidth=k.owner?2:1.3;
      ctx.beginPath(); ctx.moveTo(px(k.x),actY-6); ctx.lineTo(px(k.x),actY+bh+5); ctx.stroke();
      ctx.globalAlpha=1; }
  }
  var lt=document.getElementById("live-tiles");
  var liveDd=[["Objects"],["Requests"],["Live ranges"],["Busy"],["Splits"],["Steals"]].map(function(d){
    var w=el("div"); w.appendChild(el("dt",null,d[0]));
    var dd=el("dd",null,"0"); w.appendChild(dd); lt.appendChild(w); return dd; });
  var scrub=document.getElementById("scrub"), playBtn=document.getElementById("play");
  function readout(){
    liveDd[0].textContent=fmt(stats.keys); liveDd[1].textContent=fmt(stats.pages);
    liveDd[2].textContent=fmt(live);
    liveDd[3].textContent=fmt(Object.keys(stats.busy).length)+" / "+meta.workers;
    liveDd[4].textContent=fmt(stats.splits); liveDd[5].textContent=fmt(stats.steals);
    document.getElementById("clock").textContent=secs(clockMs)+" / "+secs(meta.durationMs);
    scrub.value=String(Math.round(1000*clockMs/Math.max(1,meta.durationMs)));
  }
  var BASE=Math.max(1,meta.durationMs/30000), SPEEDS=[1,4,16,64], si=0;
  function frame(ts){ requestAnimationFrame(frame); if(!last) last=ts; var dt=ts-last; last=ts;
    if(running){ clockMs=Math.min(meta.durationMs,clockMs+dt*BASE*rate); seekTo(clockMs);
      if(clockMs>=meta.durationMs){ running=false; playBtn.textContent="Replay"; } }
    draw(); readout(); }
  playBtn.addEventListener("click",function(){
    if(clockMs>=meta.durationMs){ clockMs=0; reset(); seekTo(0); }
    running=!running; playBtn.textContent=running?"Pause":"Play"; });
  document.getElementById("restart").addEventListener("click",function(){
    clockMs=0; reset(); seekTo(0); running=true; playBtn.textContent="Pause"; });
  document.getElementById("speed").addEventListener("click",function(){
    si=(si+1)%SPEEDS.length; rate=SPEEDS[si];
    document.getElementById("speed").innerHTML=rate+"&times;"; });
  scrub.addEventListener("input",function(){ running=false; playBtn.textContent="Play";
    clockMs=(Number(scrub.value)/1000)*meta.durationMs; reset(); seekTo(clockMs); draw(); readout(); });

  // ---- ledger + provenance ------------------------------------------------
  document.getElementById("ledger-lede").textContent =
    fmt(F.seedCount)+" seed ranges plus "+fmt(F.totalSplits)+" splits is "+
    fmt(F.seedCount+F.totalSplits)+" ranges; the trace records "+fmt(F.claimed)+" claimed and "+
    fmt(F.completed)+" completed, with "+fmt(F.failed)+" failed."+
    (F.ledgerOk ? " Those balance exactly — every range that was created was claimed, and every "+
      "range that was claimed finished. No gap, no overlap, nothing lost."
                : " Those do not balance, which is expected if the run was interrupted or the "+
      "trace is a partial prefix.")+
    (F.floorPages ? " The run spent "+fmt(meta.pagesSeen)+" LIST requests against a theoretical "+
      "floor of "+fmt(F.floorPages)+" for this many objects — "+
      (100*meta.pagesSeen/F.floorPages-100).toFixed(1)+"% overhead for the parallelism." : "");

  var prov=["Rendered from a swath --trace JSONL file by scripts/trace/trace-viz.py",
            fmt(meta.events)+" events parsed"];
  if(meta.skippedLines) prov.push(meta.skippedLines+" unparseable line(s) skipped");
  if(meta.keepEvery>1) prov.push("the replay is downsampled "+meta.keepEvery+":1 ("+
    fmt(meta.pagesPlotted)+" of "+fmt(meta.pagesSeen)+" page events); every figure, count and "+
    "finding above uses all of them");
  if(meta.anonymized) prov.push("ANONYMIZED — key names withheld");
  prov.push("Two stated aggregation choices: the horizontal axis is the measured key-mass CDF "+
            "(equal width = equal objects, not equal bytes), and colour is seed lineage, not worker");
  prov.push("Where this page and the trace disagree, the trace wins");
  document.getElementById("provenance").textContent=prov.join(". ")+".";

  window.addEventListener("resize",function(){ resize(); draw(); drawStatic(); drawRate(); });
  if(window.matchMedia){ var mq=window.matchMedia("(prefers-color-scheme: dark)");
    if(mq.addEventListener) mq.addEventListener("change",function(){ draw(); drawStatic(); drawRate(); }); }
  resize(); reset(); drawStatic(); drawRate();
  if(window.matchMedia("(prefers-reduced-motion: reduce)").matches){
    running=false; playBtn.textContent="Play"; clockMs=meta.durationMs; seekTo(clockMs); }
  draw(); readout(); requestAnimationFrame(frame);
})();
</script>
</body>
</html>
"""


# --------------------------------------------------------------------------- video


def render_video(model):
    """A capture-oriented page: fixed 1080x1350, big type, one deterministic seek hook.

    LinkedIn's feed gives a 4:5 portrait the most vertical real estate of any ratio it
    accepts, and it autoplays muted — so the frame has to carry its own narration. The page
    exposes ``window.__seek(ms)`` and draws synchronously, which lets a screenshot driver
    step it frame by frame instead of racing a live animation.
    """
    return VIDEO_TEMPLATE.replace("/*__MODEL__*/null",
                                  json.dumps(model, separators=(",", ":")))


VIDEO_TEMPLATE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>swath</title>
<style>
  :root { --paper:#0E1312; --panel:#161D1B; --deep:#1E2724; --ink:#E7EBE3; --ink2:#9BA69F;
          --ink3:#74807A; --rule:#29332F; --accent:#56C9B1; --signal:#E0AC55; --alert:#E4826A;
          --mono:ui-monospace,"SFMono-Regular","Cascadia Mono",Menlo,Consolas,monospace; }
  * { box-sizing:border-box; margin:0; }
  html,body { width:1080px; height:1350px; overflow:hidden; background:var(--paper);
              color:var(--ink); font-family:var(--mono); }
  .stage { width:1080px; height:1350px; padding:44px 52px; display:flex; flex-direction:column; }
  .top { display:flex; justify-content:space-between; align-items:flex-start; }
  h1 { font-size:44px; font-weight:500; letter-spacing:-.035em; line-height:1.05; }
  .bkt { color:var(--accent); }
  .clock { font-size:26px; color:var(--ink3); font-variant-numeric:tabular-nums; }
  .act { margin-top:26px; min-height:104px; }
  .act .n { font-size:15px; letter-spacing:.22em; text-transform:uppercase; color:var(--signal); }
  .act .t { font-size:31px; line-height:1.25; margin-top:9px; max-width:960px; letter-spacing:-.02em; }
  .act .t b { color:var(--accent); font-weight:600; }
  .act .t i { font-style:normal; color:var(--alert); font-weight:600; }
  .ribbon { margin-top:14px; height:76px; position:relative; }
  .ribbon .seg { position:absolute; top:0; height:26px; border-radius:2px; opacity:.55; }
  .ribbon .lab { position:absolute; top:30px; font-size:15px; color:var(--ink2);
                 white-space:nowrap; overflow:hidden; }
  canvas { display:block; margin-top:12px; border:1px solid var(--rule); background:var(--panel); }
  .stats { margin-top:auto; display:grid; grid-template-columns:repeat(4,1fr); gap:1px;
           background:var(--rule); border:1px solid var(--rule); }
  .stats div { background:var(--paper); padding:14px 18px; }
  .stats dt { font-size:14px; letter-spacing:.16em; text-transform:uppercase; color:var(--ink3); }
  .stats dd { font-size:34px; font-variant-numeric:tabular-nums; letter-spacing:-.02em; margin-top:4px; }
  .foot { margin-top:16px; font-size:15px; color:var(--ink3); line-height:1.5; }
</style></head>
<body><div class="stage">
  <div class="top"><h1 id="h1"></h1><div class="clock" id="clock"></div></div>
  <div class="act"><div class="n" id="actn"></div><div class="t" id="actt"></div></div>
  <div class="ribbon" id="ribbon"></div>
  <canvas id="map" width="976" height="620"></canvas>
  <dl class="stats" id="stats"></dl>
  <div class="foot" id="foot"></div>
</div>
<script>
(function(){
  "use strict";
  var M = /*__MODEL__*/null; if(!M) return;
  var meta=M.meta, F=M.findings, stream=M.stream;
  var K_PAGE=2,K_SPLIT=3,K_OWNER=4,K_CLAIM=1,K_DONE=5,K_FAIL=6,K_STEAL=7;
  var LIN=["#E4826A","#56C9B1","#6FBBD0","#BCC076","#7FA8CE"], REST="#5E6B65";
  function lin(r){ var s=M.lineage[r]; return s===undefined?REST:LIN[s]; }
  function fmt(n){ return Math.round(n).toLocaleString("en-US"); }
  function secs(ms){ return ms>=60000 ? Math.floor(ms/60000)+"m"+String(Math.round(ms%60000/1000)).padStart(2,"0")+"s"
                                      : (ms/1000).toFixed(1)+"s"; }

  document.getElementById("h1").innerHTML =
    '<span class="bkt">'+(meta.title||"swath")+'</span>';

  // ---- prefix ribbon: labels sit over the objects they describe ----------
  var rb=document.getElementById("ribbon"), W=976;
  var row=0;
  (M.families||[]).filter(function(f){ return f.x1-f.x0 > 0.035; }).slice(0,7).forEach(function(f){
    var x0=f.x0*W, w=Math.max(4,(f.x1-f.x0)*W);
    var seg=document.createElement("div"); seg.className="seg";
    seg.style.left=x0+"px"; seg.style.width=w+"px"; seg.style.background=LIN[1];
    seg.style.opacity = String(0.22+0.5*Math.min(1,f.keys/(meta.totalKeys||1)*2));
    rb.appendChild(seg);
    var txt=f.name+"  "+(100*f.keys/(meta.totalKeys||1)).toFixed(0)+"%";
    if (w < txt.length*8.4) { return; }           // no room to say it honestly — say nothing
    var lab=document.createElement("div"); lab.className="lab";
    lab.style.left=x0+"px"; lab.style.maxWidth=w+"px";
    lab.style.top=(row++%2?52:30)+"px";
    lab.textContent=txt; rb.appendChild(lab);
  });

  // ---- acts, derived from the run's own findings -------------------------
  var D=meta.durationMs, firstSteal=F.firstStealMs===null?D*0.35:F.firstStealMs;
  var ACTS=[
    {at:0, n:"one request", t:"swath asks S3 where the directories are — <b>once</b> — and turns "+
      "the answer into <b>"+fmt(F.seedCount)+" ranges</b> to scan in parallel."},
    {at:Math.min(D*0.08,4000), n:"all hands", t:"Every one of <b>"+fmt(meta.workers)+
      " workers</b> gets its own slice of the keyspace. Nothing to coordinate. Nothing to steal."},
    {at:firstSteal*0.55, n:"draining", t:"A directory name says nothing about how many objects "+
      "are behind it. The engine is finding out the only way there is — by listing."},
    {at:firstSteal, n:"the guesses run out", t:"The well-guessed ranges finish. One did not: "+
      "<i>"+F.topShare+"% of the whole bucket</i> sat behind a single guess."},
    {at:firstSteal+(D-firstSteal)*0.25, n:"stealing", t:"Idle workers carve the survivor apart at "+
      "boundaries they invent — <b>keys that do not exist</b> are still valid fences."},
    {at:firstSteal+(D-firstSteal)*0.62, n:"converging", t:"<b>"+fmt(F.topSplits)+" splits</b> into "+
      "that one range. All <b>"+fmt(F.topWorkers)+"</b> workers end up inside it."},
    {at:D*0.985, n:"done", t:"<b>"+fmt(meta.totalKeys)+" objects</b> in "+secs(D)+
      ", at <b>"+F.reqPer1k.toFixed(2)+"</b> requests per thousand. No gaps. No overlaps."}
  ];

  // ---- accumulating keyspace x time map ---------------------------------
  var cv=document.getElementById("map"), g=cv.getContext("2d");
  var CW=cv.width, CH=cv.height, ML=8, MT=8, IW=CW-16, IH=CH-16;
  function sx(p){ return ML+p*IW; } function sy(t){ return MT+(t/Math.max(1,D))*IH; }

  var stats=document.getElementById("stats"), dds=[];
  [["objects"],["requests"],["live ranges"],["splits"]].forEach(function(l){
    var d=document.createElement("div"), dt=document.createElement("dt"), dd=document.createElement("dd");
    dt.textContent=l[0]; dd.textContent="0"; d.appendChild(dt); d.appendChild(dd);
    stats.appendChild(d); dds.push(dd); });
  document.getElementById("foot").textContent =
    "Generated from this run's own --trace event log by scripts/trace/trace-viz.py. "+
    "Horizontal = keyspace weighted by objects, so equal width is equal objects. "+
    "Vertical = time. Colour = which original guess the work descends from.";

  function seek(t){
    var i, live=0, keys=0, pages=0, splits=0;
    g.clearRect(0,0,CW,CH);
    for(i=0;i<M.segments.length;i++){
      var s=M.segments[i]; if(s[2]>t) continue;
      var y1=Math.min(s[3],t);
      var x0=sx(s[0]), w=Math.max(1,sx(s[1])-x0), y0=sy(s[2]), h=Math.max(1,sy(y1)-y0);
      g.globalAlpha=.34; g.fillStyle=lin(s[4]); g.fillRect(x0,y0,w,h);
      g.globalAlpha=1; g.strokeStyle=lin(s[4]); g.lineWidth=.5; g.strokeRect(x0+.25,y0+.25,w,h);
      if(s[3]>t) live++;
    }
    for(i=0;i<stream.length;i++){
      var e=stream[i]; if(e[0]>t) break;
      if(e[1]===K_PAGE){ keys+=e[5]; pages+=(e[6]||1); }
      else if(e[1]===K_SPLIT||e[1]===K_OWNER){ splits++;
        g.beginPath(); g.arc(sx(e[5]),sy(e[0]),e[1]===K_OWNER?2.4:1.9,0,6.284);
        g.fillStyle="#E0AC55"; g.globalAlpha=.8; g.fill(); g.globalAlpha=1; }
    }
    g.strokeStyle="#56C9B1"; g.lineWidth=2; g.globalAlpha=.9;
    g.beginPath(); g.moveTo(ML,sy(t)); g.lineTo(ML+IW,sy(t)); g.stroke(); g.globalAlpha=1;

    var act=ACTS[0];
    for(i=0;i<ACTS.length;i++){ if(t>=ACTS[i].at) act=ACTS[i]; }
    document.getElementById("actn").textContent=act.n;
    document.getElementById("actt").innerHTML=act.t;
    document.getElementById("clock").textContent=secs(t)+" / "+secs(D);
    dds[0].textContent=fmt(keys); dds[1].textContent=fmt(pages);
    dds[2].textContent=fmt(live); dds[3].textContent=fmt(splits);
  }
  window.__seek=seek;          // the capture driver's only entry point
  window.__duration=D;
  seek(0);
})();
</script></body></html>
"""


# --------------------------------------------------------------------------- self-test


SELF_TEST_TRACE = [
    {"v": 1, "ts_ns": 1000, "event": "seeded", "worker_id": -1, "node_id": 1, "lo": None, "hi": "b/"},
    {"v": 1, "ts_ns": 1100, "event": "seeded", "worker_id": -1, "node_id": 2, "lo": "b/", "hi": None},
    {"v": 1, "ts_ns": 2000, "event": "claimed", "worker_id": 0, "node_id": 1,
     "lo": "a/", "cursor": "a/", "hi": "b/"},
    {"v": 1, "ts_ns": 2100, "event": "claimed", "worker_id": 1, "node_id": 2,
     "lo": "b/", "cursor": "b/", "hi": None},
    {"v": 1, "ts_ns": 3000, "event": "page_committed", "worker_id": 0, "node_id": 1,
     "keys": 1000, "cursor": "a/m", "completed": False},
    {"v": 1, "ts_ns": 3500, "event": "page_committed", "worker_id": 1, "node_id": 2,
     "keys": 50, "cursor": "b/z", "completed": True},
    {"v": 1, "ts_ns": 4000, "event": "split", "worker_id": 1, "node_id": 1,
     "child_node_id": 3, "mechanism": "midpoint", "pivot": "a/q", "hi": "b/"},
    {"v": 1, "ts_ns": 4200, "event": "owner_split_decision", "worker_id": 0, "node_id": 1,
     "reason": "demand_gated"},
    {"v": 1, "ts_ns": 5000, "event": "steal_attempt", "worker_id": 1,
     "outcome": "RETRY", "reason": "probe_empty"},
    {"v": 1, "ts_ns": 5500, "event": "claimed", "worker_id": 1, "node_id": 3,
     "lo": "a/q", "cursor": "a/q", "hi": "b/"},
    {"v": 1, "ts_ns": 6000, "event": "page_committed", "worker_id": 1, "node_id": 3,
     "keys": 400, "cursor": "a/z", "completed": True},
    {"v": 1, "ts_ns": 6500, "event": "completed", "worker_id": 1, "node_id": 3},
    {"v": 1, "ts_ns": 7000, "event": "completed", "worker_id": 0, "node_id": 1},
    {"v": 1, "ts_ns": 7100, "event": "completed", "worker_id": 1, "node_id": 2},
    {"v": 1, "ts_ns": 8000, "event": "future_event_kind", "worker_id": 0, "node_id": 9},
]


def self_test():
    failures = []

    def check(label, got, want):
        if got != want:
            failures.append("%s: got %r, want %r" % (label, got, want))

    check("unescape", unescape(r"a\x09b"), "a\tb")
    check("unescape untouched", unescape("plain/key"), "plain/key")
    check("common prefix", common_prefix([b"crawl-a", b"crawl-b"]), b"crawl-")
    check("family collapses dates", family_of("estofs.20210101/x"), "estofs.N")
    check("family keeps plain", family_of("_post_processing/a/b"), "_post_processing")

    axis = MassAxis([(b"a", 10), (b"b", 90)])
    check("axis is a CDF", round(axis.pos(b"a"), 6), 0.1)
    check("axis tops out", round(axis.pos(b"b"), 6), 1.0)
    check("axis past the end", axis.pos(b"z"), 1.0)
    check("axis null default", axis.pos(None, 0.5), 0.5)
    check("axis is monotone", axis.pos(b"a") < axis.pos(b"ab") <= axis.pos(b"b"), True)
    # Mass weighting is the whole point: a heavy key must claim most of the width.
    heavy = MassAxis([(b"a", 1), (b"m", 998), (b"z", 1)])
    check("mass dominates width", heavy.pos(b"m") > 0.9, True)

    model = build_model(list(SELF_TEST_TRACE), 0, "self-test", False)
    F = model["findings"]
    check("keys summed", model["meta"]["totalKeys"], 1450)
    check("pages counted", model["meta"]["pagesSeen"], 3)
    check("splits counted", F["totalSplits"], 1)
    check("uniform split spotted", F["uniformSplits"], 1)
    check("gate decisions counted", F["gateTotal"], 1)
    check("ledger balances", F["ledgerOk"], True)
    check("claimed == completed", (F["claimed"], F["completed"], F["failed"]), (3, 3, 0))
    # Lineage: node 3 was split off node 1, so its keys belong to seed 1's weight.
    seed1 = [s for s in model["seeds"] if s["id"] == 1][0]
    check("lineage rolls up to the seed", seed1["keys"], 1400)
    check("lineage counts its splits", seed1["splits"], 1)
    check("heaviest seed identified", F["topKeys"], 1400)
    check("unknown kind ignored", any(e[1] not in range(8) for e in model["stream"]), False)

    torn = [json.dumps(e) for e in SELF_TEST_TRACE] + ['{"v":1,"ts_ns":9000,"event":"page_comm']
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
    print("self-test: 27 checks, %d failures" % len(failures), file=sys.stderr)
    return 1 if failures else 0


# --------------------------------------------------------------------------- cli


def fmt_int(n):
    return "{:,}".format(n)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="trace-viz.py",
        description="Turn a swath --trace JSONL run into a self-contained HTML explainer.")
    parser.add_argument("trace", nargs="?", type=Path, help="the --trace JSONL file to render")
    parser.add_argument("-o", "--out", type=Path,
                        help="output HTML path (default: alongside the trace, .html)")
    parser.add_argument("--title", help="page heading (default: the trace file's stem)")
    parser.add_argument("--anonymize", action="store_true",
                        help="withhold every key name; emit positions and counts only")
    parser.add_argument("--video", action="store_true",
                        help="emit a 1080x1350 capture page for recording a share-ready clip; "
                             "drive it via window.__seek(ms) and encode the frames")
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

    unknown = {e.get("v") for e in events if "v" in e} - {SCHEMA_VERSION}
    if unknown:
        print("trace-viz: warning: unrecognized schema version(s) %s — this reader understands "
              "v%d; fields may be missing" % (sorted(unknown), SCHEMA_VERSION), file=sys.stderr)

    model = build_model(events, skipped, args.title or args.trace.stem, args.anonymize)
    out = args.out or args.trace.with_suffix(".html")
    out.write_text((render_video if args.video else render)(model), encoding="utf-8")

    meta, F = model["meta"], model["findings"]
    print("trace-viz: %s -> %s" % (args.trace, out), file=sys.stderr)
    print("  %s events · %s objects · %s pages · %d workers · %.1fs"
          % (fmt_int(meta["events"]), fmt_int(meta["totalKeys"]), fmt_int(meta["pagesSeen"]),
             meta["workers"], meta["durationMs"] / 1000.0), file=sys.stderr)
    print("  headline: heaviest of %s seed ranges held %.1f%% of the objects; %s splits, %s "
          "owner-side" % (fmt_int(F["seedCount"]), F["topShare"], fmt_int(F["totalSplits"]),
                          fmt_int(F["ownerSplits"])), file=sys.stderr)
    if not F["ledgerOk"]:
        print("  note: range ledger does not balance (partial or interrupted run)", file=sys.stderr)
    if skipped:
        print("  %d unparseable line(s) skipped" % skipped, file=sys.stderr)
    if meta["keepEvery"] > 1:
        print("  replay downsampled %d:1 (%s of %s page events); figures use all of them"
              % (meta["keepEvery"], fmt_int(meta["pagesPlotted"]), fmt_int(meta["pagesSeen"])),
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
