"""swath-explainer — turn a swath run's --trace log into a readable explanation.

See the README beside this package for what it produces and how to run it.
"""

from .model import build_model
from .render import render_report, render_video
from .trace import load_events

__all__ = ["build_model", "render_report", "render_video", "load_events"]
__version__ = "0.1.0"
