# explainer

Turns a swath run's `--trace` event log into something a person can read: a self-contained HTML
report that explains what the run did and why, and a share-ready video of the same run.

It works on **any bucket**. Nothing about the output is written for a particular listing — the
tool computes the run's findings from its events and states them in prose, so it describes
whichever trace you point it at, including deciding which findings are worth stating at all.

Developer tooling, not a shipped artifact. The generator is **stdlib-only Python** — no runtime
dependencies at all, so the public repo keeps its zero-dependency property and CI needs nothing
installed. Only the optional video capture step needs anything extra.

## Layout

```text
tools/explainer/
├── explainer.py                  # run from a clone with nothing set up
├── pyproject.toml                # or install it: provides `swath-explainer`
├── capture-video.js              # optional video frame driver (Node + playwright)
└── src/swath_explainer/
    ├── keys.py                   # key bytes, and the mass-CDF keyspace projection
    ├── trace.py                  # lenient --trace JSONL reading
    ├── model.py                  # the reduction: figures, lineage, findings
    ├── render.py                 # model + template -> page
    ├── cli.py                    # argument handling
    ├── selftest.py               # internal checks, no inputs needed
    └── templates/
        ├── report.html           # the explainer report
        ├── video-strip.html      # video: the algorithm as it happens
        └── video-map.html        # video: the space-time carving diagram
```

The templates are real `.html` files rather than string literals in Python, so the CSS and
canvas code are editable as themselves. `render.py` substitutes one placeholder into them.

## Run it on any bucket

```bash
# 1. list a bucket with tracing on (any swath list/resume run accepts --trace)
swath list s3://your-bucket/ --region us-east-1 --format parquet -o out/ \
      --trace run.trace.jsonl

# 2. explain it
tools/explainer/explainer.py run.trace.jsonl -o run.html --title "s3://your-bucket"

# or with uv, if you'd rather not think about interpreters
uv run tools/explainer/explainer.py run.trace.jsonl -o run.html
```

Open `run.html`. One file, no CDN, no external images, safe from `file://`, light and dark.
The page carries a real `<title>` and description computed from the run (so a shared link
identifies itself), and a one-line footer pointing at the project and the field guide —
plain anchors, nothing fetched. `--no-links` strips the footer when the page must carry
no URLs at all.

```text
explainer.py TRACE.jsonl [-o OUT.html] [--title NAME] [--anonymize]
                             [--video [--video-style strip|map]]
explainer.py --self-test              # internal checks, no inputs needed
```

| Flag | What it does |
|---|---|
| `--title` | Page heading. Defaults to the trace file's stem. |
| `--anonymize` | Withhold every key name — positions and counts only. Use when the picture has to travel further than the trace does. |
| `--no-links` | Omit the site-links footer, leaving a page with no URLs at all. The report never fetches anything either way. |
| `--video` | Emit a 1080×1350 capture page instead of the report (see below). |
| `--video-style` | `strip` (default) or `map`. Two different arguments about the same run. |

## The report

Sections, in the order a newcomer needs them:

1. **The guesses, and how they turned out** — the seed ranges ranked by the objects actually
   found behind them. Weight rolls up through the split genealogy, so each bar is the work that
   guess was really responsible for, not just what drained before it got cut.
2. **What the run actually felt like** — objects/s and live ranges over time, plus the
   owner-split gate ledger.
3. **Most of these keys do not exist** — the pivot cascade with sample pivots verbatim.
4. **The whole run, on one map** — keyspace × time.
5. **Watch it happen** — the same events replayed.
6. **The ledger** — `seeds + splits == claimed == completed`, and request overhead against the
   theoretical floor.

Above all of it, the findings the tool decided were worth stating. A run whose seed was accurate
does not get told a dramatic story: the "guess that was wrong" finding only appears when one seed
lineage actually held a large share, and an interrupted run gets its unbalanced ledger flagged
instead.

## Video

```bash
explainer.py run.trace.jsonl -o video.html --video          # or --video-style map
node tools/explainer/capture-video.js http://localhost:PORT/video.html
ffmpeg -framerate 30 -i /tmp/frames/f%05d.png -c:v libx264 -preset slow -crf 20 \
       -pix_fmt yuv420p -movflags +faststart run.mp4
```

`capture-video.js` needs the `playwright` npm package resolvable from where you run it
(`npm install playwright`, or point `NODE_PATH` at an existing install).

1080×1350 (4:5) — the tallest ratio the common social feeds accept, so it claims the most
vertical space. Silent by design, because those feeds autoplay muted: the frame narrates itself
with act captions derived from the run's own findings.

The page exposes `window.__seek(ms)` and draws synchronously, so the driver steps it frame by
frame rather than screenshotting a live animation and hoping. Deterministic frames, and the ease
curve spends them on the busy middle of the run rather than the quiet ends.

The capture step needs Node with playwright, and ffmpeg. Both are **dev-only** — the generator
never touches them, and neither is a repo dependency. Serve the HTML over `http://` rather than
opening it as a file, since headless Chromium blocks `file://` navigation.

**Two styles, deliberately kept:**

- **`strip`** — the algorithm as it happens. The keyspace is cut into seed ranges, all of them
  drain in parallel with each bar filling to its worker's cursor, then stealing subdivides
  whatever is left. Seed cut points are drawn as fence posts, the heaviest guesses are labelled
  with their real bounds, and prefix families are named over the span their objects occupy. This
  is the one to share.
- **`map`** — the space-time carving diagram: every range a rectangle over the keyspace it owned
  and the time it owned it, accumulating downward. Denser, and closer to a flame graph in feel.
  Harder to narrate cold, but it shows structure the strip cannot — split-chain depth, which
  regions outlived the run, where the tail was.

## Two design decisions worth knowing

**The keyspace axis is the measured key-mass CDF**, built from `page_committed`'s
`(cursor, keys)` pairs. **Equal width means an equal number of objects**, so a range's area is
proportional to the work it did, and a region holding two thirds of a bucket gets two thirds of
the picture. The two obvious alternatives both fail on real data: a raw byte-value axis collapses
a deep-prefix bucket onto a hairline, and an axis anchored on range boundaries gives every
boundary equal width, so a run seeded into hundreds of ranges renders as hundreds of identical
slivers and width encodes nothing. The cost, stated on the generated page too: **screen distance
is not byte distance**, and empty keyspace takes no width.

**Colour is seed lineage, not worker id.** A reader cares which original guess the work descends
from, not which of N threads happened to run it. With 64 workers and a palette of 8, colour-by-
worker is decoration; colour-by-lineage is information.

## Determinism, and what wrote what

Rendering the same trace twice produces a **byte-identical file** — verify with `sha256sum`. No
model is consulted at render time. The per-run sentences are string interpolation over computed
numbers; the fixed explanatory prose is checked into this script as source and is the same for
every run. What varies with the data is every number, every figure, and which findings appear.

## Sensitivity

A trace carries real key names on nearly every event ([`metrics-internals.md`](../../docs/internals/metrics-internals.md)
§7) — the same sensitivity class as the listing output itself. The report inherits that: seed
bounds and split pivots are shown verbatim, because they are most of the value. `--anonymize`
withholds all of them and emits positions and counts only.

## Reading a picture

- **Healthy run** — confetti: many short drains, everyone busy, wide short rectangles.
- **Dense serial tail** — a single tall thin column outliving the rest of the run.
- **Thief probe storm** — many steal attempts with few committed children behind them.
- **Owner-split confetti** — a stack of hairline rectangles in one keyspace region.

The trace format itself is specified in
[`docs/internals/metrics-internals.md`](../../docs/internals/metrics-internals.md) §7. A consumer
is required to tolerate a torn final line after a hard kill and to ignore event kinds it does not
know; this tool does both.
