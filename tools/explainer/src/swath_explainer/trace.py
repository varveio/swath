"""Reading a run trace (swath diag list --trace), leniently."""

import json
import re

# The trace schema this reader understands (run_start's trace_version).
TRACE_VERSION = 1


class NotATrace(ValueError):
    """The file is not a swath run trace."""


def load_events(path):
    """Parse a trace file leniently, in time order.

    Tolerates what docs/internals/trace.md requires a reader to tolerate: a torn
    final line after a hard kill, and event kinds this reader does not know. Events
    of concurrent ranges may be written slightly out of ``ts_ns`` order; a stable
    sort restores time order and keeps a range's own events, which the file holds in
    causal order, in that order.
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
    events.sort(key=lambda e: e["ts_ns"])
    return events, skipped


def run_start(events):
    """The trace's run_start, or NotATrace: every swath trace begins with one."""
    for e in events:
        if e.get("event") == "run_start":
            return e
    raise NotATrace("no run_start event: not a swath run trace (swath diag list --trace)")


def family_of(key):
    """Collapse a key to its top-level directory, generalizing long digit runs.

    ``estofs.20210101/x`` and ``estofs.20210102/y`` become one family. A presentation
    heuristic for grouping the object-count table, not a claim about the bucket's schema —
    the page says as much.
    """
    seg = (key or "").split("/")[0]
    return re.sub(r"\d{6,}", "N", seg) or "(root)"
