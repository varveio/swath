# swath.varve.io source

This directory is the website. Edit it here, on `main`, in the same pull request as the
behavior it describes.

The site is published by `.github/workflows/site.yml`, which builds this directory into a
GitHub Pages artifact and deploys it on every push to `main`. Nothing about the published
site is stored in a branch, so there is no generated tree to hand-edit and no deploy
history to accumulate.

## What is here

| Path | What it is |
| --- | --- |
| `index.html` | The homepage. |
| `field-guide/index.html` | The visual field guide, a single self-contained page. |
| `runs/<run>/index.html` | Generated report pages. Produced by `tools/explainer` from a run's `--trace` log — change the generator or the run record, not the page. |
| `data/channel.json` | Which swath the site documents: `{"channel": "release", "version": "X.Y.Z"}`. |
| `data/runs/*.json` | Canonical `swath-public-run-v1` records, one per published run. Every figure the site quotes about a run comes from here. |
| `data/media.json` | The videos, which are **not** in git: the release they live on, and the sha256 the deploy must see. |
| `assets/`, `card.png` | Posters and the social card. The videos land in `assets/` at deploy time. |
| `CNAME`, `.nojekyll` | Custom domain and the marker that keeps Pages from running Jekyll. |

`README.md` is the one file the build leaves behind; everything else here is served from
the site root.

## Before you push

```bash
./scripts/ci/build-site.sh build/site            # exactly what gets deployed
./scripts/ci/fetch-site-media.sh build/site      # pulls and verifies the videos
python3 scripts/ci/check-site.py --site build/site
```

The same three commands run in CI and gate the deploy. They check that site-local links,
media, and fragments resolve; that links into this repository name files and headings that
exist on `main`; that the public spelling and naming rules in
[`docs/style.md`](../docs/style.md) hold across the site and the current documentation;
that the version the homepage advertises matches `data/channel.json` and the newest
release in the changelog; that the run figures quoted on the pages equal their records
under `data/runs/`; and a small set of accessibility smoke tests.

The fetch step needs network access. Without it, `check-site.py` still passes: a path
declared in `data/media.json` counts as resolvable, and the digest is verified only for
files actually present.

## Releasing a new version

Update `data/channel.json` and the homepage's "Documentation for swath X.Y.Z" line
together. The channel check fails while they disagree, and fails again if either disagrees
with the newest released version in [`CHANGELOG.md`](../CHANGELOG.md).

## Publishing a new run

Add its record under `data/runs/<run-id>.json` first; the pages quote the record, not the
other way round. `swath-cli`'s `PublicRunFactsTest` guards the copies in `README.md` and
`docs/full-scale-demo.md`, and `scripts/ci/check-site.py` guards the copies here.
Capturing a run and regenerating its report end to end is issue #193.

## Replacing a video

Videos are deliberately not in git: a re-record replaces about 1.5 MB of binary content,
and git keeps every version forever. They live as assets on a GitHub release, and the
deploy fetches them and verifies each digest against `data/media.json`.

**Treat a release asset as immutable once a deploy has referenced it.** Replacing bytes
under an existing asset name would leave every reviewed digest describing something that no
longer exists. Publish a new asset name, or a new dated media release, instead.

The videos come from `tools/explainer`: `--video` renders a replay from a run's `--trace`
log through `capture-video.js` (see [`tools/explainer/README.md`](../tools/explainer/README.md)).
Capturing the run that feeds it is issue #193.

```bash
# 1. Hash the new file. This value is what makes the reference trustworthy.
sha256sum swath-noaa-gestofs-2027-01.mp4

# 2a. Add it to the current media release under a NEW asset name:
gh release upload site-media-2026-09 swath-noaa-gestofs-2027-01.mp4

# 2b. Or, when starting a fresh set, create a new dated release. Two flags matter:
#     the tag must not start with `v` (that triggers .github/workflows/release.yml), and
#     `--latest=false` keeps it from displacing the newest swath release as "Latest",
#     which is what `/releases/latest` resolves to.
gh release create site-media-2027-01 \
  --title "Site media 2027-01" \
  --latest=false \
  --notes "Video assets served by swath.varve.io; fetched by the site deploy workflow, pinned by sha256." \
  swath-noaa-gestofs-2027-01.mp4
```

3. Update `data/media.json`: the `release` tag if it changed, and the entry's `asset`,
   `bytes`, and `sha256`. `path` is where the file lands in the built site and is what the
   pages reference — keep it stable unless the pages change too.
4. Update any `data/runs/<run-id>.json` record that carries the old video's `sha256` under
   `artifacts.video`, so the run record still describes a file that exists.
5. Open the pull request. `scripts/ci/fetch-site-media.sh` downloads each declared asset and
   fails loudly on a digest mismatch, so a stale `media.json` or a swapped asset is caught
   before it is published, not after.
