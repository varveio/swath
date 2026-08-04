"""Reading a --trace JSONL file, leniently."""

import collections
import json
import re


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


