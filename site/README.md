# swath.varve.io source

The website is a [VitePress](https://vitepress.dev/) build of the user documentation in
[`docs/`](../docs/README.md) and the home page in this directory. Edit it in a topic branch and pull
request, alongside the behavior it describes. Do not push directly to `main`: that publishes the site.

`.github/workflows/site.yml` builds the site into a GitHub Pages artifact, checks it, and deploys it on
every push to `main`. Nothing about the published site is stored in a branch.

## What is here

| Path | What it is |
| --- | --- |
| `index.md` | The home page, served at `/`. |
| `field-guide/` | The field guide, served at `/field-guide/`: how swath divides a listing, orders it and resumes it, on the home page's run. `index.md` holds the text and its diagrams (inline SVG in the theme's colors) and imports three Vue components: `MiniModel.vue`, the animated miniature model (`mini-model.ts` runs it, `ModelFrame.vue` draws a frame; still frames with `prefers-reduced-motion`), and `RunSeeds.vue` and `RunInFlight.vue`, the run's figures, drawn from `run-figures.json` (below). |
| `moved/` | Short pages for addresses that were published once or that readers type (`/docs/sources`, `/docs/commands/`, `/docs/reference/`): each says where the content is now, and forwards where one page replaces it. `MOVED` in `.vitepress/config.mts` maps them; they are not in search or the sitemap. |
| `.vitepress/config.mts` | Navigation, sidebar, search, the pages that are published and the link rewriting. |
| `.vitepress/theme/` | The default theme with Varve's colors and fonts (Inter, Geist Mono, self-hosted from npm), the development-docs notice and the home page's terminal example (`index.ts`); no custom components. |
| `static/` | Copied verbatim into the site root: `CNAME`, `.nojekyll`, `robots.txt`, `favicon.svg`, the nav logo (`logo-light.svg`, `logo-dark.svg`), the social card `card.png`, `data/channel.json`, `data/media.json` (the videos the deploy fetches, below), `media/` (the replay's poster) and `runs/noaa-gestofs-pds/index.html` (the home page's run report, below) |
| `card/` | The social card's source, `card.svg`, and `render.sh`, which renders it to `static/card.png`. |
| `readme/` | `hero.png`, the home page's lead terminal example as the repository README shows it, and `render.sh` (with `render.cjs`), which renders it from the built site. Not published. |
| `package.json`, `package-lock.json` | VitePress, pinned to 1.6.4, and the font packages, with their locked dependencies. An `overrides` entry holds Vite at 6.4.x, which has the security fixes Vite 5 does not get; VitePress 1.6.4 asks for Vite 5. Remove it when a stable VitePress requires Vite 6 or later. |

## How the documentation is published

The Markdown in `docs/` is the source of truth and stays readable on GitHub: no front matter, relative `.md`
links, GitHub alerts and GitHub heading anchors. The build adapts it without editing it:

- `docs/PAGE.md` is served at `/docs/PAGE`, and a directory's `README.md` at the directory, as `/docs/`.
- Contributor pages are not published or searchable: `docs/internals/`, `docs/ops/` and `docs/style.md`; nor is
  `docs/sources.md`, the old single Sources page, whose address forwards to `/docs/sources/`. Every other
  Markdown file under `docs/` is published.
- Headings get GitHub's anchors, so `page.md#heading` links resolve on the site as they do on GitHub.
- A link from a published page to anything outside the published set (an internals page, code, the
  repository README) is rewritten to its GitHub URL on `main`. The build prints how many it rewrote.
- Each page has an "Edit this page on GitHub" link to its source.

A new user page in `docs/` needs a sidebar entry in `.vitepress/config.mts`; it is published either way.

## Before you push

```bash
./scripts/ci/build-site.sh build/site            # exactly what gets deployed; needs Node.js and npm
./scripts/ci/fetch-site-media.sh build/site       # the videos, from a release, verified by sha256
python3 scripts/ci/check-site.py --site build/site
```

The same three commands run in CI and gate the deploy. The build fails on a dead link between pages. The
checks then verify, over the built tree, that site-local links, assets and fragments resolve; that links into
this repository name files and headings that exist on `main`; that no contributor page is published; that
the public spelling and naming rules in [`docs/style.md`](../docs/style.md) hold across the site and the
current documentation; that the version the home page offers matches `data/channel.json` and every container
image tag on the site; and a small set of accessibility smoke tests.

To work on the pages with live reload, run `npm ci && npx vitepress dev` in this directory.

## Branding, the social card and the README image

The colors, fonts and chevron mark are Varve's, from its UI's design tokens (`ui/src/design.css` in the
Varve repository): the accent petrol for links and buttons, the brand teal for the hero name, the neutral
backgrounds and text, and the mark's four stroke colors per theme in the logo files. The footer links to
varve.io.

`static/card.png` (1200x630, the `og:image` and `twitter:image` of every page) is rendered from
`card/card.svg`. After editing the SVG, regenerate the PNG and commit both:

```bash
site/card/render.sh    # npm ci if needed, Playwright's Chromium, a local HTTP server for the fonts
```

The repository README opens with the home page's lead terminal example as an image, `readme/hero.png`, because
GitHub drops the colors of HTML in a README. It is a screenshot of the built home page at twice its CSS size, in
the dark color scheme with transparent corners, so it reads on GitHub's light and dark themes. After changing
`HERO` in `.vitepress/theme/index.ts` or the terminal's styles, regenerate it and commit it with the change:

```bash
site/readme/render.sh  # builds the site into a temporary directory, serves it and screenshots the terminal
```

## The run report

`static/runs/noaa-gestofs-pds/index.html` is the report [`tools/explainer`](../tools/explainer/README.md) renders
from the home page's run trace, published as generated. It is not a VitePress page: links to it carry
`target="_self"` so the browser loads it instead of the router (in Markdown,
`[…](https://swath.varve.io/runs/noaa-gestofs-pds/){target="_self"}`: `go test ./docs` reads a root-relative
link as a file in the repository), and
`check-site.py` holds it to its own title, description, landmark and skip link, not to the development-docs
notice or a canonical URL. The trace (48,216 events) and the run report are kept outside git, at
`gs://s3-listing-study-results-29c02004/scratch-swath-go/homepage-2026-11-15/`. The page embeds its run-facts
record (`#swath-run-facts`); to regenerate it, save that record as `facts.json` and run:

```bash
tools/explainer/explainer.py noaa-gestofs-pds.trace.jsonl -o site/static/runs/noaa-gestofs-pds/index.html \
  --title s3://noaa-gestofs-pds --run-facts facts.json
```

The field guide's run figures come from the same trace. `field-guide/run-figures.py` (stdlib Python) reduces it to
`field-guide/run-figures.json`: the objects each starting range held, and the listing requests in flight and the
largest range's splits in quarter-second steps.

```bash
site/field-guide/run-figures.py noaa-gestofs-pds.trace.jsonl > site/field-guide/run-figures.json
```

## Replacing a video

Videos are not in git: a re-render replaces over a megabyte of binary content, and git keeps every version
forever. They are assets on a GitHub release, and `.github/workflows/site.yml` fetches them into the built site
with `scripts/ci/fetch-site-media.sh` and verifies each sha256 against `static/data/media.json`, on pull requests
too, before the checks; a digest mismatch or a missing asset fails the job. `check-site.py` counts a declared
path as present, so a local build without the videos still checks, and its `media` check holds the manifest to
account (a release, a digest per file, a page that references it, and the bytes when they are there). The home
page's replay is rendered by [`tools/explainer`](../tools/explainer/README.md) from the run in
[the measurements](../docs/internals/measurements.md#a-public-bucket-for-the-home-page-noaa-gestofs-pds-2026-11-15);
its poster is a frame of the same capture, kept in `static/media/`.

**Treat a release asset as immutable once a deploy has referenced it.** Publish a new asset name, or a new dated
media release, rather than replacing bytes under a name a reviewed digest describes.

```bash
sha256sum swath-noaa-gestofs-pds-2027-01-10.mp4
# Into the current media release under a new asset name:
gh release upload site-media-2026-11 swath-noaa-gestofs-pds-2027-01-10.mp4
# Or a new dated release. The tag must not start with `v` (that triggers release.yml), and
# --latest=false keeps it from displacing the newest swath release as "Latest".
gh release create site-media-2027-01 --title "Site media 2027-01" --latest=false \
  --notes "Video assets served by swath.varve.io; fetched by the site deploy workflow, pinned by sha256." \
  swath-noaa-gestofs-pds-2027-01-10.mp4
```

Then update `static/data/media.json` (the `release`, and the entry's `asset`, `bytes` and `sha256`; `path` is where
the file lands in the site and what the page references) and run
`scripts/ci/fetch-site-media.sh build/site` after building to check the pin.

## Releasing a new version

`static/data/channel.json` names the version the site offers: `{"channel": "release", "version": "X.Y.Z"}`
while the documentation describes a release, `{"channel": "main", "version": "X.Y.Z-rc.N"}` while it describes
`main` ahead of one. Update it together with the "The newest published build is swath X, ..." sentence on the
home page and in `docs/install.md`, the image tags in `docs/install.md` and the release's heading in
[`CHANGELOG.md`](../CHANGELOG.md). The channel check requires that sentence on both pages and fails while any
of them disagree, and on the release channel it also fails unless the version is the newest released version
in the changelog. On the main channel every page must carry the development-docs notice: set
`DEV_DOCS_NOTICE` in `.vitepress/config.mts` to its text then, and back to `false` once the release is out.
