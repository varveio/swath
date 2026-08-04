"""Reducing a trace to the figures and the findings a page states in words."""

import collections

from .keys import MassAxis, common_prefix, key_bytes
from .trace import family_of

TIME_BINS = 120
MAX_PAGE_EVENTS = 9000

# Event kinds as emitted into the page's compact event stream.
K_SEED, K_CLAIM, K_PAGE, K_SPLIT, K_OWNER_SPLIT, K_DONE, K_FAIL, K_STEAL = range(8)


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


