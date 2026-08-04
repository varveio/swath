"""Injecting a model into a template."""

import json
from pathlib import Path

_TEMPLATES = Path(__file__).parent / "templates"
_PLACEHOLDER = "/*__MODEL__*/null"


def _load(name):
    return (_TEMPLATES / name).read_text(encoding="utf-8")


def _inject(template, model):
    return template.replace(_PLACEHOLDER, json.dumps(model, separators=(",", ":")))


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
