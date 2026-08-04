"""Internal checks — no inputs needed, so CI can run them anywhere."""

import json
import sys
from pathlib import Path

from .keys import MassAxis, common_prefix, unescape
from .trace import family_of, load_events
from .model import build_model
from .render import render_report as render


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


