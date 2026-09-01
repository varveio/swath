"""Injecting a model into a template."""

import json
from pathlib import Path

_TEMPLATES = Path(__file__).parent / "templates"
_PLACEHOLDER = "/*__MODEL__*/null"
_FACTS_PLACEHOLDER = "/*__FACTS__*/null"
_STATIC_SUMMARY_PLACEHOLDER = "<!--__STATIC_SUMMARY__-->"

UNKNOWN = "unknown in retained evidence"

# (visible label, dotted path into a swath-public-run-v1 facts record), in table order.
# Kept in sync with the identical list in report.html's inline script.
FACTS_FIELDS = [
    ("Run ID", "run_id"),
    ("swath version", "swath.version"),
    ("swath commit", "swath.commit"),
    ("Captured at", "captured_at"),
    ("Source URI", "target.uri"),
    ("S3 region", "target.region"),
    ("Client provider / region", "client.region"),
    ("Machine", "client.machine_type"),
    ("Output mode", "output.format"),
    ("Command", "command"),
    ("Listing wall clock", "clocks.listing_wall_label"),
    ("Visualization playback length", "clocks.video_playback_label"),
    ("API attempts", "result.api_attempts"),
    ("Initial ranges", "result.initial_ranges"),
    ("Splits", "result.splits"),
    ("Completed ranges", "result.completed_ranges"),
    ("Source report", "artifacts.summary"),
    ("Raw trace", "artifacts.raw_trace"),
]


def _load(name):
    return (_TEMPLATES / name).read_text(encoding="utf-8")


def _inject(template, model):
    return template.replace(_PLACEHOLDER, _script_safe(json.dumps(model, separators=(",", ":"))))


def _script_safe(payload):
    """Make a JSON document safe to embed in an HTML <script> block.

    Object keys come from the bucket, so a key containing ``</script>`` would otherwise
    close the block and inject markup into the report. JSON has no bare ``<``, ``>`` or
    ``&`` outside string literals, so escaping them unconditionally is safe and leaves the
    parsed value identical. U+2028/9 are valid in JSON strings but not in JS source.
    """
    return (payload.replace("<", "\\u003c").replace(">", "\\u003e")
                   .replace("&", "\\u0026")
                   .replace("\u2028", "\\u2028").replace("\u2029", "\\u2029"))


def _html_escape(text):
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _dig(facts, path):
    node = facts
    for part in path.split("."):
        if not isinstance(node, dict) or node.get(part) is None:
            return None
        node = node[part]
    return node


def _static_summary(model, facts):
    """Plain HTML describing the run, valid without JavaScript.

    Rendered once at generation time from the same ``model`` and ``facts`` the interactive
    page uses, so it never drifts from what the script-driven page shows. Every facts field
    that is absent reads ``unknown in retained evidence`` rather than being inferred.
    """
    meta = model["meta"]
    run_id = _dig(facts, "run_id") if facts else None

    def _cell(value):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return "{:,}".format(value)
        return str(value)

    rows = "".join(
        "<tr><td>%s</td><td>%s</td></tr>" % (
            _html_escape(label),
            _html_escape(_cell(_dig(facts, path))) if facts and _dig(facts, path) is not None
            else UNKNOWN)
        for label, path in FACTS_FIELDS)
    return (
        "<p>Run <code>%s</code> &mdash; %s objects, %s committed listing pages, %d workers, "
        "%.1fs recorded trace duration.</p>\n"
        "<table><tbody>%s</tbody></table>"
        % (_html_escape(str(run_id)) if run_id else UNKNOWN,
           "{:,}".format(meta["totalKeys"]), "{:,}".format(meta["pagesSeen"]),
           meta["workers"], meta["durationMs"] / 1000.0, rows))


def render_report(model, facts=None):
    """The self-contained HTML explainer for a run.

    ``facts`` is an optional provenance record (schema ``swath-public-run-v1``): run ID,
    swath version/commit, capture date, target, client, command, and clocks for the specific
    run this trace represents. Pass it with ``--run-facts PATH`` on the CLI. It is rendered
    as a visible provenance table, embedded as machine-readable JSON
    (``#swath-run-facts``), and used for a static ``<noscript>`` summary. Any field the
    record omits is shown as "unknown in retained evidence", never guessed.
    """
    page = _load("report.html")
    page = page.replace(_STATIC_SUMMARY_PLACEHOLDER, _static_summary(model, facts))
    page = page.replace(_FACTS_PLACEHOLDER,
                         _script_safe(json.dumps(facts)) if facts is not None else "null")
    return _inject(page, model)


def render_video(model, style="strip"):
    """A capture-oriented 1080x1350 page.

    ``strip`` shows the algorithm as it happens — seed cuts, parallel drains, stealing.
    ``map`` shows the space-time carving diagram: denser, more diagnostic, harder to narrate
    cold. Both expose ``window.__seek(ms)`` and draw synchronously so a driver can step them
    frame by frame instead of racing a live animation.
    """
    return _inject(_load("video-map.html" if style == "map" else "video-strip.html"), model)
