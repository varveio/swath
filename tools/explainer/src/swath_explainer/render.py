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


def _esc(text):
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace('"', "&quot;"))


def _description(model):
    """One shareable sentence, computed from the same model the page renders."""
    meta, f = model["meta"], model["findings"]
    return ("%s objects \u00b7 %d workers \u00b7 %.0fs \u2014 heaviest of %s seed guesses "
            "held %.1f%% of the objects"
            % ("{:,}".format(meta["totalKeys"]), meta["workers"], meta["durationMs"] / 1000.0,
               "{:,}".format(f["seedCount"]), f["topShare"]))


def _head_meta(model):
    title, desc = _esc(model["meta"]["title"]), _esc(_description(model))
    return ('<meta name="description" content="%s">\n'
            '<meta property="og:title" content="%s">\n'
            '<meta property="og:description" content="%s">\n'
            '<meta property="og:type" content="article">' % (desc, title, desc))


_SITE_LINKS = ('<div id="site-links">listed by '
               '<a href="https://github.com/varveio/swath">swath</a> \u00b7 how the '
               'algorithm works: '
               '<a href="https://swath.varve.io/field-guide/">the field guide</a></div>')


def render_report(model, links=True):
    """The self-contained HTML explainer for a run.

    Self-contained means the page fetches nothing — no CDN, no external images. Plain
    anchors don't break that, so the default report carries a one-line footer pointing
    at the project and the field guide, and a real <title>/description so a shared link
    identifies itself. ``links=False`` strips the footer for a page that must carry no
    URLs at all; the head metadata stays, since it names the run, not an address.
    """
    page = _inject(_load("report.html"), model)
    page = page.replace("<title>swath run trace</title>",
                        "<title>%s</title>" % _esc(model["meta"]["title"]), 1)
    page = page.replace("<!--__HEAD_META__-->", _head_meta(model), 1)
    return page.replace("<!--__SITE_LINKS__-->", _SITE_LINKS if links else "", 1)


def render_video(model, style="strip"):
    """A capture-oriented 1080x1350 page.

    ``strip`` shows the algorithm as it happens — seed cuts, parallel drains, stealing.
    ``map`` shows the space-time carving diagram: denser, more diagnostic, harder to narrate
    cold. Both expose ``window.__seek(ms)`` and draw synchronously so a driver can step them
    frame by frame instead of racing a live animation.
    """
    return _inject(_load("video-map.html" if style == "map" else "video-strip.html"), model)
