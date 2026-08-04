#!/usr/bin/env python3
"""Run the explainer without installing it: `tools/explainer/explainer.py TRACE.jsonl ...`.

The package under `src/` is the real code; this is a convenience shim so the tool works from a
clone with nothing set up. Installed users get the `swath-explainer` command instead.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from swath_explainer.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
