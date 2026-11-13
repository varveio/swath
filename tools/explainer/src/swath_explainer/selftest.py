"""Internal checks over a captured trace — no other inputs, so CI can run them anywhere."""

import json
import sys
import tempfile
from pathlib import Path

from .keys import MassAxis, common_prefix, key_bytes, key_text
from .model import K_SPLIT, build_model
from .render import render_report, render_video
from .trace import NotATrace, family_of, load_events

# A small run captured through the README's recipe: swath diag count --trace against
# swath-replay serving 1,670 keys (big/ holds 1,500 of them; a/, c/ to f/ 30 each; z/ 20,
# with a non-ASCII name), --workers 4 --page-size 50, 40 ms a page.
FIXTURE = Path(__file__).parent / "testdata" / "small.trace.jsonl"
FIXTURE_KEYS = 1670
GO_MECHANISMS = {"reflect", "midpoint", "density", "structure"}


def self_test():
    failures, checks = [], [0]

    def check(label, got, want):
        checks[0] += 1
        if got != want:
            failures.append("%s: got %r, want %r" % (label, got, want))

    # ---- keys and the axis ------------------------------------------------------
    check("a UTF-8 key is its bytes", key_bytes("z/xé"), "z/xé".encode("utf-8"))
    check("a hex key is its bytes", key_bytes({"hex": "61ff"}), b"a\xff")
    check("a hex key displays escaped", key_text({"hex": "61ff"}), "a\\xff")
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

    # ---- the captured run -------------------------------------------------------
    events, skipped = load_events(FIXTURE)
    check("fixture parses", skipped, 0)
    splits = sum(1 for e in events if e["event"] == "split")
    model = build_model([dict(e) for e in events], 0, "self-test", False)
    F, meta = model["findings"], model["meta"]
    check("objects summed", meta["totalKeys"], FIXTURE_KEYS)
    check("splits counted", F["totalSplits"], splits)
    check("the fixture splits", splits > 0, True)
    check("seeded + 2 x splits = claimed", F["claimed"], F["seedCount"] + 2 * splits)
    check("claimed = completed + splits", F["claimed"], F["completed"] + splits)
    check("ledger balances", F["ledgerOk"], True)
    check("the listed ranges tile the scope", F["tiled"], True)
    check("workers from run_start", meta["workers"], 4)
    check("lineage weights sum to the objects", sum(s[2] for s in model["seedSpans"]), FIXTURE_KEYS)
    check("the heaviest guess is big/", F["topLo"], "big/")
    check("big/ is most of the bucket", F["topShare"] > 85, True)
    check("Go's mechanisms", set(model["mechanisms"]) <= GO_MECHANISMS, True)
    # A split ends its range at the cursor: its segment stops there, never at the pivot.
    split_at = {e[2]: e[6] for e in model["stream"] if e[1] == K_SPLIT}
    ends = [s[1] for s in model["segments"] if s[4] is not None]
    check("a split range's segment ends at its cursor",
          all(any(abs(x - cur) < 1e-9 for x in ends) for cur in split_at.values()), True)

    future = [dict(e) for e in events] + [{"event": "future_kind", "ts_ns": events[-1]["ts_ns"]}]
    check("an unknown kind is ignored",
          all(e[1] in range(6) for e in build_model(future, 0, "x", False)["stream"]), True)

    lossy = [dict(e) for e in events]
    i = next(i for i, e in enumerate(lossy) if e["event"] == "claimed")
    del lossy[i]
    lossy[-1]["dropped"] = 1
    check("a lossy trace does not balance", build_model(lossy, 0, "x", False)["findings"]["ledgerOk"], False)

    raw = [dict(e) for e in events]
    for e in raw:
        if e["event"] == "page" and e.get("last") == "big/part-0001.csv":
            e["last"] = {"hex": b"big/part-0001.csv\xff".hex()}
    check("a key that is not UTF-8 reads", build_model(raw, 0, "x", False)["meta"]["totalKeys"], FIXTURE_KEYS)

    # The tiling is rebuilt from the trace and checked against the scope: a failed
    # range is a gap, at the scope's start as anywhere else.
    def synthetic(ends):
        out = [{"event": "run_start", "ts_ns": 0, "trace_version": 1, "workers": 2, "page_size": 10}]
        bounds = [("", "m"), ("m", "t"), ("t", None)]
        for i, (lo, hi) in enumerate(bounds):
            tile = {"id": i + 1, "lo": lo, "hi": hi} if hi else {"id": i + 1, "lo": lo, "open": True}
            out.append(dict(tile, event="seeded", ts_ns=1))
            out.append(dict(tile, event="claimed", ts_ns=2 + i, worker=i))
            out.append({"event": "page", "ts_ns": 10 + i, "id": i + 1, "worker": i, "after": lo,
                        "last": lo + "k", "rows": 5, "dur_ns": 1, "done": ends[i] == "completed"})
            out.append({"event": ends[i], "ts_ns": 20 + i, "id": i + 1, "worker": i, "rows": 5})
        out.append({"event": "run_end", "ts_ns": 30, "status": "committed", "events": len(out), "dropped": 0})
        return out

    whole = build_model(synthetic(["completed"] * 3), 0, "x", False)["findings"]
    check("three completed ranges tile the scope", (whole["tiled"], whole["ledgerOk"]), (True, True))
    for at, ends in (("the start", ["failed", "completed", "completed"]),
                     ("the middle", ["completed", "failed", "completed"])):
        F2 = build_model(synthetic(ends), 0, "x", False)["findings"]
        check("a failed range at %s is a gap" % at, (F2["tiled"], F2["ledgerOk"]), (False, False))

    try:
        build_model([e for e in events if e["event"] != "run_start"], 0, "x", False)
        check("a trace without run_start is refused", False, True)
    except NotATrace:
        check("a trace without run_start is refused", True, True)

    with tempfile.TemporaryDirectory() as tmp:
        torn = Path(tmp) / "torn.jsonl"
        torn.write_text(FIXTURE.read_text(encoding="utf-8") + '{"event":"page","ts_ns":9', encoding="utf-8")
        got, skipped = load_events(torn)
        check("a torn last line is skipped", skipped, 1)
        check("intact lines kept", len(got), len(events))

    # ---- rendering --------------------------------------------------------------
    page = render_report(build_model([dict(e) for e in events], 0, "self-test", True))
    check("model injected", "/*__MODEL__*/null" in page, False)
    check("anonymized page withholds keys", "big/part" in page, False)
    check("page is standalone", "http://" in page or "https://" in page, False)
    for style in ("strip", "map"):
        video = render_video(model, style)
        check("video %s injected" % style, "/*__MODEL__*/null" in video, False)

    # A bucket is not a trusted input: a key may try to close the script block.
    evil = "</script><script>x/"
    hostile = [dict(e) for e in events]
    for e in hostile:
        for field in ("lo", "hi", "cursor", "pivot", "last"):
            if e.get(field) == "big/":
                e[field] = evil
    hostile_page = render_report(build_model(hostile, 0, "self-test", False))
    check("hostile key reaches the payload",
          evil.replace("<", "\\u003c").replace(">", "\\u003e") in hostile_page, True)
    check("hostile key cannot close the script block", "</script><script>" in hostile_page, False)
    check("the run's facts are JSON", json.loads(json.dumps(model["findings"]))["tiled"], True)

    for line in failures:
        print("FAIL " + line, file=sys.stderr)
    print("self-test: %d checks, %d failures" % (checks[0], len(failures)), file=sys.stderr)
    return 1 if failures else 0
