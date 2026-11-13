"""Reducing a trace to the figures and the findings a page states in words."""

import collections

from .keys import MassAxis, key_bytes, key_text
from .trace import family_of, run_start

TIME_BINS = 120
MAX_PAGE_EVENTS = 9000

# Event kinds as emitted into the page's compact event stream:
#   [t, K_SEED,  id, lo, hi]
#   [t, K_CLAIM, id, worker, lo, cursor, hi, root]
#   [t, K_PAGE,  id, worker, last, rows, pages]
#   [t, K_SPLIT, id, lower, upper, worker, cursor, pivot, hi, mechanism, root]
#   [t, K_DONE,  id]
#   [t, K_FAIL,  id]
# Positions are on the mass axis, in [0, 1]. A split ends its range at the cursor;
# the two children list (cursor, pivot] and (pivot, hi].
K_SEED, K_CLAIM, K_PAGE, K_SPLIT, K_DONE, K_FAIL = range(6)

# The pivot that comes from no evidence: halfway between the cursor and the bound.
UNIFORM = ("midpoint",)


def tiles_scope(leaves, start=b""):
    """Whether intervals ``(lo, hi]`` (hi None: open, ⊤) tile the scope after ``start``
    (b"", ⊥, on a fresh run; a resume's key, whose first range may start at a prefix
    of it): sorted by lo, the first starts at or below start, each starts where the
    one before it ended, and only the last is open, so it reaches ⊤. A failed range
    is not a leaf, so it leaves a gap."""
    if not leaves:
        return False
    leaves = sorted(leaves, key=lambda l: l[0])
    if leaves[0][0] > start:
        return False
    for i, (lo, hi) in enumerate(leaves):
        if i and lo != leaves[i - 1][1]:
            return False
        if hi is None and i != len(leaves) - 1:
            return False
        if hi is not None and hi <= lo:
            return False
    return leaves[-1][1] is None


def build_model(events, skipped, title, anonymize):
    """Reduce a trace to the figures AND the findings the page states in words.

    Two passes. The first builds the mass axis, because positions cannot be assigned until
    the whole key distribution is known. The second walks the events in order, replaying
    the run to reconstruct range lifetimes, seed lineage, and the timeline.
    """
    start = run_start(events)
    t0 = start["ts_ns"]
    duration = max(1.0, (events[-1]["ts_ns"] - t0) / 1e6)

    # ---- pass 1: the key distribution ------------------------------------------
    points = []
    for e in events:
        if e.get("event") == "page" and e.get("last") is not None and e.get("rows"):
            points.append((key_bytes(e["last"]), int(e["rows"])))
    axis = MassAxis(points)

    def ms(event):
        return round(max(0, event["ts_ns"] - t0) / 1e6, 1)

    def at(event, field, default=0.0):
        return round(axis.pos(key_bytes(event.get(field)), default), 5)

    def show(value):
        return None if (value is None or anonymize) else key_text(value)

    def hi_of(event):
        return 1.0 if event.get("open") else at(event, "hi", 1.0)

    # ---- pass 2: replay ---------------------------------------------------------
    parent_of = {}
    for e in events:
        if e.get("event") == "split":
            for child in ("lower", "upper"):
                if e.get(child) is not None:
                    parent_of[e[child]] = e.get("id")

    def ancestor(node):
        seen, cur = set(), node
        while cur in parent_of and cur not in seen:
            seen.add(cur)
            cur = parent_of[cur]
        return cur

    open_seg, seed_info, claimed_bounds = {}, {}, {}
    segments, stream, mechanisms = [], [], []
    mech_count, reason_count, families = (collections.Counter(), collections.Counter(),
                                          collections.Counter())
    mech_pivots = collections.defaultdict(list)
    family_dirs = collections.defaultdict(set)
    family_span = {}
    seed_keys, seed_splits, seed_workers = (collections.Counter(), collections.Counter(),
                                            collections.defaultdict(set))
    lineage_order, leaves = [], []
    keys_bin = [0.0] * TIME_BINS
    busy_bin = [0] * TIME_BINS
    lanes = set()
    seeds_made = claimed = completed = failed = splits = 0
    pages_seen = pages_kept = total_keys = pending_keys = pending_pages = 0
    first_split_ms = None
    live = peak_live = 0
    peak_live_ms = 0.0
    seed = None
    end = None

    page_total = sum(1 for e in events if e.get("event") == "page")
    keep_every = (page_total // MAX_PAGE_EVENTS) + 1 if page_total > MAX_PAGE_EVENTS else 1

    def index(table, value):
        if value not in table:
            table.append(value)
        return table.index(value)

    def close_segment(node, t, x1=None):
        seg = open_seg.pop(node, None)
        if seg and t > seg[2]:
            segments.append([seg[0], seg[1] if x1 is None else x1, seg[2], t, seg[3]])
        return seg

    def tbin(t):
        return min(TIME_BINS - 1, max(0, int(TIME_BINS * t / duration)))

    for event in events:
        kind = event.get("event")
        node = event.get("id", -1)
        worker = event.get("worker", -1)
        if worker >= 0:
            lanes.add(worker)
        t = ms(event)
        b = tbin(t)

        if kind == "seed":
            seed = event
            if event.get("stop") == "one_page":
                # The seed's first data page held the whole scope: no range follows.
                total_keys += int(event.get("keys", 0) or 0)
                leaves.append((b"", None))

        elif kind == "seeded":
            lo, hi = at(event, "lo", 0.0), hi_of(event)
            if event.get("reused"):
                # A resume's reused span: replayed by the order stage, never listed.
                leaves.append((key_bytes(event.get("lo")),
                               None if event.get("open") else key_bytes(event.get("hi"))))
                continue
            seeds_made += 1
            lineage_order.append(node)
            seed_info[node] = {"lo": show(event.get("lo")), "hi": show(event.get("hi")),
                               "x0": lo, "x1": hi, "open": bool(event.get("open"))}
            stream.append([t, K_SEED, node, lo, hi])

        elif kind == "claimed":
            claimed += 1
            lo, hi = at(event, "lo", 0.0), hi_of(event)
            root = ancestor(node)
            claimed_bounds[node] = (key_bytes(event.get("lo")),
                                    None if event.get("open") else key_bytes(event.get("hi")))
            open_seg[node] = [lo, hi, t, root]
            seed_workers[root].add(worker)
            live += 1
            if live > peak_live:
                peak_live, peak_live_ms = live, t
            stream.append([t, K_CLAIM, node, worker, lo, lo, hi, root])

        elif kind == "page":
            pages_seen += 1
            rows = int(event.get("rows", 0) or 0)
            total_keys += rows
            keys_bin[b] += rows
            busy_bin[b] = max(busy_bin[b], live)
            new = at(event, "last", 0.0)
            seed_keys[ancestor(node)] += rows
            last = show(event.get("last"))
            if last and rows:
                fam = family_of(last)
                families[fam] += rows
                family_dirs[fam].add(last.split("/")[0])
                # Where this family sits on the axis, so a label can be drawn over its own
                # objects rather than in a legend the reader has to cross-reference.
                span = family_span.get(fam)
                family_span[fam] = (min(span[0], new), max(span[1], new)) if span else (new, new)
            pending_keys += rows
            pending_pages += 1
            if pages_seen % keep_every == 0 or event.get("done"):
                # Carry BOTH the keys and the pages the dropped events stood for, so a
                # downsampled replay still ends on the run's true totals.
                stream.append([t, K_PAGE, node, worker, new, pending_keys, pending_pages])
                pending_keys = pending_pages = 0
                pages_kept += 1

        elif kind == "split":
            splits += 1
            mech = event.get("mechanism") or "?"
            mech_count[mech] += 1
            reason_count[event.get("reason") or "?"] += 1
            if len(mech_pivots[mech]) < 3 and event.get("pivot") is not None:
                mech_pivots[mech].append(show(event["pivot"]))
            if first_split_ms is None:
                first_split_ms = t
            cursor, pivot, hi = at(event, "cursor", 0.0), at(event, "pivot", 0.0), hi_of(event)
            root = ancestor(node)
            seed_splits[root] += 1
            close_segment(node, t, cursor)
            live = max(0, live - 1)
            bounds = claimed_bounds.get(node)
            if bounds:
                leaves.append((bounds[0], key_bytes(event.get("cursor"))))
            stream.append([t, K_SPLIT, node, event.get("lower", -1), event.get("upper", -1), worker,
                           cursor, pivot, hi, index(mechanisms, mech), root])

        elif kind in ("completed", "failed"):
            if kind == "completed":
                completed += 1
                if node in claimed_bounds:
                    leaves.append(claimed_bounds[node])
            else:
                failed += 1
            close_segment(node, t)
            live = max(0, live - 1)
            stream.append([t, K_DONE if kind == "completed" else K_FAIL, node])

        elif kind == "run_end":
            end = event

    if pending_keys or pending_pages:
        stream.append([ms(events[-1]), K_PAGE, -1, -1, 1.0, pending_keys, pending_pages])
    last_ms = ms(events[-1])
    for node in list(open_seg):
        close_segment(node, last_ms)

    # ---- seed lineage: the figure the page is built around ----------------------
    seeds = []
    for node in lineage_order:
        info = seed_info[node]
        seeds.append({"id": node, "lo": info["lo"], "hi": info["hi"],
                      "open": info["open"], "x0": info["x0"], "x1": info["x1"],
                      "keys": int(seed_keys.get(node, 0)),
                      "splits": int(seed_splits.get(node, 0)),
                      "workers": len(seed_workers.get(node, ()))})
    seed_spans = [[s["x0"], s["x1"], s["keys"], s["id"]] for s in seeds]
    by_mass = sorted(seeds, key=lambda s: -s["keys"])
    seeded_total = sum(s["keys"] for s in seeds) or 1
    top = by_mass[0] if by_mass else None
    tidy = [s["keys"] for s in by_mass[1:] if s["keys"] > 0]
    median = sorted(tidy)[len(tidy) // 2] if tidy else 0

    # Lineage colour slots: the heaviest few get their own, everything else shares one.
    palette_ids = [s["id"] for s in by_mass[:5]]
    lineage_slot = {nid: i for i, nid in enumerate(palette_ids)}

    # Busy worker-time: every segment is one range held by one worker for a stretch, so
    # their durations sum to the fleet's occupied time. Against the wall clock that gives
    # the honest speedup number — and, more usefully, where the shortfall went.
    workers = int(start.get("workers") or 0) or len(lanes)
    busy_ms = sum(seg[3] - seg[2] for seg in segments)
    ideal_ms = busy_ms / float(workers) if workers else 0.0
    idle_ms = max(0.0, duration * workers - busy_ms)

    uniform = sum(n for m, n in mech_count.items() if m in UNIFORM)
    dropped = int(end.get("dropped", 0) or 0) if end else None

    findings = {
        "seedCount": len(seeds),
        "seedRequests": seed.get("requests") if seed else None,
        "seedRoundTrips": seed.get("round_trips") if seed else None,
        "seedMs": round(seed.get("dur_ns", 0) / 1e6, 1) if seed else None,
        "seedStop": seed.get("stop") if seed else None,
        "seedWhole": bool(seed and seed.get("stop") == "one_page"),
        "topKeys": top["keys"] if top else 0,
        "topShare": round(100.0 * top["keys"] / seeded_total, 1) if top else 0.0,
        "topSplits": top["splits"] if top else 0,
        "topWorkers": top["workers"] if top else 0,
        "topLo": top["lo"] if top else None,
        "topHi": top["hi"] if top else None,
        "medianKeys": median,
        "ratio": int(top["keys"] / median) if (top and median) else 0,
        "emptySeeds": sum(1 for s in seeds if s["keys"] == 0),
        "firstSplitMs": first_split_ms,
        "peakLive": peak_live, "peakLiveMs": peak_live_ms,
        "totalSplits": splits,
        "slowSplits": reason_count.get("slow", 0),
        "uniformSplits": uniform,
        "claimed": claimed, "completed": completed, "failed": failed,
        "dropped": dropped,
        "ledgerOk": (seeds_made + 2 * splits == claimed == completed + splits
                     and failed == 0 and dropped == 0),
        "tiled": tiles_scope(leaves, key_bytes(start.get("resume_after")) or b""),
        "reqPer1k": round(1000.0 * pages_seen / total_keys, 3) if total_keys else None,
        "busySec": round(busy_ms / 1000.0, 1),
        "idealSec": round(ideal_ms / 1000.0, 1),
        "idleSec": round(idle_ms / 1000.0, 1),
        "speedupPct": round(100.0 * ideal_ms / duration, 1) if duration and workers else None,
        "pageSize": int(start.get("page_size") or 0),
    }
    page_size = findings["pageSize"] or 1000
    findings["floorPages"] = (total_keys + page_size - 1) // page_size if total_keys else 0

    peak_rate = max(keys_bin) or 1.0
    return {
        "meta": {"title": title, "events": len(events), "skippedLines": skipped,
                 "pagesSeen": pages_seen, "pagesPlotted": pages_kept,
                 "keepEvery": keep_every, "totalKeys": total_keys,
                 "workers": workers, "durationMs": round(duration, 1),
                 "cdfPoints": axis.count, "anonymized": anonymize,
                 "version": start.get("version"), "status": end.get("status") if end else None,
                 "traceVersion": start.get("trace_version")},
        "findings": findings,
        "seeds": by_mass[:14],
        "seedSpans": seed_spans,
        "lineage": lineage_slot,
        "families": [{"name": k, "dirs": len(family_dirs[k]), "keys": int(v),
                      "x0": round(family_span.get(k, (0.0, 0.0))[0], 5),
                      "x1": round(family_span.get(k, (0.0, 0.0))[1], 5)}
                     for k, v in families.most_common(10)],
        "rate": [round(v / peak_rate, 4) for v in keys_bin],
        "busy": busy_bin,
        "peakRate": round(peak_rate * TIME_BINS / (duration / 1000.0), 0) if duration else 0,
        "mechanisms": mechanisms,
        "mechRows": sorted([{"name": m, "n": n, "pivots": mech_pivots.get(m, [])}
                            for m, n in mech_count.items()], key=lambda r: -r["n"]),
        "stream": stream,
        "segments": segments,
    }
