"""Injecting a model into a template."""

import json
from pathlib import Path

_TEMPLATES = Path(__file__).parent / "templates"
_PLACEHOLDER = "/*__MODEL__*/null"


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


def render_report(model):
    """The self-contained HTML explainer for a run."""
    return _inject(_load("report.html"), model)


def render_video(model, style="strip"):
    """A capture-oriented 1080x1350 page.

    ``strip`` shows the algorithm as it happens — seed cuts, parallel drains, stealing.
    ``map`` shows the space-time carving diagram: denser, more diagnostic, harder to narrate
    cold. Both expose ``window.__seek(ms)`` and draw synchronously so a driver can step them
    frame by frame instead of racing a live animation.
    """
    return _inject(_load("video-map.html" if style == "map" else "video-strip.html"), model)
