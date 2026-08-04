"""Command line entry point."""

import argparse
import sys
from pathlib import Path

from .trace import load_events
from .model import build_model
from .render import render_report, render_video
from .selftest import self_test

SCHEMA_VERSION = 1


def fmt_int(n):
    return "{:,}".format(n)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="explainer.py",
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
    parser.add_argument("--video-style", choices=("strip", "map"), default="strip",
                        help="strip (default): the algorithm as it happens — seed cuts, parallel "
                             "drains, stealing. map: the space-time carving diagram, denser and "
                             "more diagnostic")
    parser.add_argument("--self-test", action="store_true", help="run internal checks and exit")
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()
    if args.trace is None:
        parser.error("a trace file is required (or --self-test)")
    if not args.trace.is_file():
        print("explainer: no such file: %s" % args.trace, file=sys.stderr)
        return 2

    events, skipped = load_events(args.trace)
    if not events:
        print("explainer: %s held no parseable trace events" % args.trace, file=sys.stderr)
        return 1

    unknown = {e.get("v") for e in events if "v" in e} - {SCHEMA_VERSION}
    if unknown:
        print("explainer: warning: unrecognized schema version(s) %s — this reader understands "
              "v%d; fields may be missing" % (sorted(unknown), SCHEMA_VERSION), file=sys.stderr)

    model = build_model(events, skipped, args.title or args.trace.stem, args.anonymize)
    out = args.out or args.trace.with_suffix(".html")
    page = render_video(model, args.video_style) if args.video else render_report(model)
    out.write_text(page, encoding="utf-8")

    meta, F = model["meta"], model["findings"]
    print("explainer: %s -> %s" % (args.trace, out), file=sys.stderr)
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
