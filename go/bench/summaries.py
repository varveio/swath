#!/usr/bin/env python3
"""Tabulate run reports into one TSV row per run: Java swath's _swath_summary.json and the Go tool's run.json
(report_version 1), mapped onto the same columns (the Java-to-Go mapping lives here, see docs/upgrade.md).

usage: summaries.py PATH... > runs.tsv     (files, or directories searched recursively)

Each row is keyed by bucket (config.target), version (shape.fingerprint.git_sha), producer and config
(concurrency, seed mode, the policy toggles that are off), and carries the headline numbers (objects,
wall, listing time, API calls, pages, splits, steals, peak in flight, retries, throttles) plus every
engagement counter: Java's swath.steal_reason counters as OUTCOME.reason columns, and for the Go tool
its split, guard, seed and empty-child counters. Columns are the union over all inputs, sorted, so the
output of a large batch stays one table. Missing values are empty.
"""
import json, os, sys


def files(paths):
    for p in paths:
        if os.path.isdir(p):
            for r, _, fs in os.walk(p):
                for f in fs:
                    if f in ("_swath_summary.json", "run.json") or f.endswith("-report.json") or f.endswith(".summary.json"):
                        yield os.path.join(r, f)
        else:
            yield p


def flat(prefix, x, out):
    if isinstance(x, dict):
        for k, v in x.items():
            flat(f"{prefix}.{k}" if prefix else k, v, out)
    elif isinstance(x, (int, float, bool)) and not isinstance(x, bool) or isinstance(x, bool):
        out[prefix] = x


def go_row(path, d):
    """A Go run report (run.json, or what -json printed)."""
    inv, out, t = d["invocation"], d["output"], d["timings"]
    stats = (d.get("source") or {}).get("stats", {})
    src = dict((stats.get("counters") or {}).get("values", {}))
    src["mean_active"] = stats.get("mean_active")
    feats = (stats.get("counters") or {}).get("features", {})
    r = {
        "file": path, "producer": d["build"]["tool"], "bucket": inv["source"], "version": d["build"].get("revision", ""),
        "concurrency": inv["workers"], "seed_mode": "shallow" if feats.get("seed") else "none",
        "toggles_off": ",".join(sorted(k for k, v in feats.items() if not v)), "completed": d["outcome"]["complete"], "objects": out["rows_listed"],
        "duration_ms": t["wall_ms"], "listing_duration_ms": t["listing_ms"], "api_calls": src.get("requests"),
        "pages": src.get("pages"), "splits": sum(v for k, v in src.items() if k.startswith("split.accepted")),
        "steals": 0, "peak_in_flight": src.get("max_active"), "avg_in_flight": src.get("mean_active"),
        "files_out": out["parts_written"] + out["parts_retained"], "bytes_out": out["bytes"],
        "retries": src.get("retries", ""), "throttle_events": src.get("throttles", ""),
        "overfetch_keys": src.get("overfetch_keys"),
        "empty_children": src.get("child_empty.lower", 0) + src.get("child_empty.upper", 0),
        "probe_requests": src.get("probe.requests", 0) + src.get("structure.requests", 0) + src.get("seed.probes", 0),
    }
    g = {}
    flat("go", {k: v for k, v in src.items() if k.split(".")[0] in ("split", "seed", "child_empty", "overfetch_keys", "max_queued", "mean_active", "timeouts", "attempts", "probe")}, g)
    r.update(g)
    return r


def row(path):
    d = json.load(open(path))
    if "report_version" in d:
        return go_row(path, d)
    cfg, flags = d.get("config", {}), d.get("engine_flags", {})
    producer = d.get("producer", "swath-java")
    off = sorted(k for k, v in flags.items() if v is False and k not in ("readahead", "sequential", "seed"))
    r = {
        "file": path, "producer": producer, "bucket": cfg.get("target", ""),
        "version": d.get("shape", {}).get("fingerprint", {}).get("git_sha", ""),
        "concurrency": cfg.get("max_parallel_listings", ""), "seed_mode": d.get("seed", {}).get("mode", ""),
        "toggles_off": ",".join(off), "completed": d.get("completed"), "objects": d.get("objects"),
        "duration_ms": d.get("duration_ms"), "listing_duration_ms": d.get("listing_duration_ms"),
        "api_calls": d.get("cost", {}).get("api_calls"), "pages": d.get("engine", {}).get("pages"),
        "splits": d.get("engine", {}).get("splits"), "steals": d.get("engine", {}).get("steals"),
        "peak_in_flight": d.get("engine", {}).get("peak_in_flight"), "avg_in_flight": d.get("engine", {}).get("avg_in_flight"),
        "files_out": d.get("output", {}).get("files"), "bytes_out": d.get("output", {}).get("compressed_size_bytes"),
    }
    re_ = d.get("recovered_errors", {})
    r["retries"] = re_.get("retries", "")
    r["throttle_events"] = re_.get("throttle_events", "")
    for m in d.get("meters", []):
        if m.get("name") == "swath.steal_reason":
            t = m.get("tags", {})
            r[f"{t.get('outcome')}.{t.get('reason')}"] = int(m.get("value", m.get("count", 0)) or 0)
        elif m.get("name") in ("swath.throttle.events", "swath.retries") and "value" in m:
            r[m["name"]] = m["value"]
        elif m.get("name") == "swath.page.raw_keys" and d.get("objects") is not None:
            # keys the worker pages returned less the keys listed: the trimmed past-bound keys
            r["overfetch_keys"] = int(m.get("value", 0) or 0) - d["objects"]
        elif m.get("name") in ("swath.probe.fetches", "swath.probe.structure_fetches"):
            r["probe_requests"] = r.get("probe_requests", 0) + int(m.get("value", 0) or 0)
    r["empty_children"] = r.get("CHILD_MASS.empty", 0)
    if "probes" in d.get("seed", {}):
        r["seed.probes"] = d["seed"]["probes"]
        r["probe_requests"] = r.get("probe_requests", 0) + d["seed"]["probes"]
    if d.get("go", {}).get("s3"):
        g = {}
        flat("go", {k: v for k, v in d["go"]["s3"].items() if k in ("split", "seed", "child_empty", "overfetch_keys", "max_queued", "mean_active", "retries", "throttles", "timeouts", "attempts")}, g)
        r.update(g)
        r["retries"], r["throttle_events"] = d["go"]["s3"].get("retries", ""), d["go"]["s3"].get("throttles", "")
    return r


def main():
    rows = []
    for f in files(sys.argv[1:]):
        try:
            rows.append(row(f))
        except Exception as e:  # keep going over a large batch
            print(f"skip {f}: {e}", file=sys.stderr)
    head = ["file", "producer", "bucket", "version", "concurrency", "seed_mode", "toggles_off", "completed", "objects",
            "duration_ms", "listing_duration_ms", "api_calls", "pages", "splits", "steals", "peak_in_flight",
            "avg_in_flight", "retries", "throttle_events", "files_out", "bytes_out", "overfetch_keys", "empty_children",
            "probe_requests"]
    extra = sorted({k for r in rows for k in r} - set(head))
    print("\t".join(head + extra))
    for r in sorted(rows, key=lambda r: (str(r["bucket"]), str(r["version"]), str(r["toggles_off"]), r["file"])):
        print("\t".join("" if r.get(k) is None else str(r.get(k, "")) for k in head + extra))


if __name__ == "__main__":
    main()
