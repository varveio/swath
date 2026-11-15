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
| `moved/` | Short pages for addresses that were published once or that readers type (`/docs/sources`, `/docs/commands/`, `/docs/reference/`, `/field-guide/`, `/runs/noaa-gestofs-pds/`): each says where the content is now, and forwards where one page replaces it. `MOVED` in `.vitepress/config.mts` maps them; they are not in search or the sitemap. |
| `.vitepress/config.mts` | Navigation, sidebar, search, the pages that are published and the link rewriting. |
| `.vitepress/theme/` | The default theme with Varve's colors and fonts (Inter, Geist Mono, self-hosted from npm), the development-docs notice and the home page's terminal example (`index.ts`); no custom components. |
| `static/` | Copied verbatim into the site root: `CNAME`, `.nojekyll`, `robots.txt`, `favicon.svg`, the nav logo (`logo-light.svg`, `logo-dark.svg`), the social card `card.png`, `data/channel.json`, `data/media.json` (the videos the deploy fetches, below) and `media/` (the replay's poster) |
| `card/` | The social card's source, `card.svg`, and `render.sh`, which renders it to `static/card.png`. |
| `package.json`, `package-lock.json` | VitePress, pinned to 1.6.4, and the font packages, with their locked dependencies. |

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

## Branding and the social card

The colors, fonts and chevron mark are Varve's, from its UI's design tokens (`ui/src/design.css` in the
Varve repository): the accent petrol for links and buttons, the brand teal for the hero name, the neutral
backgrounds and text, and the mark's four stroke colors per theme in the logo files. The footer links to
varve.io.

`static/card.png` (1200x630, the `og:image` and `twitter:image` of every page) is rendered from
`card/card.svg`. After editing the SVG, regenerate the PNG and commit both:

```bash
site/card/render.sh    # npm ci if needed, Playwright's Chromium, a local HTTP server for the fonts
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

When 0.4.0 is released, set `DEV_DOCS_NOTICE` in `.vitepress/config.mts` to `false` to remove the
development-docs notice above the navigation, and retire `docs/release-candidate.md`.

`static/data/channel.json` names the version the site offers: `{"channel": "main", "version": "X.Y.Z-rc.N"}`
while the documentation describes `main` ahead of a release, `{"channel": "release", "version": "X.Y.Z"}`
once it describes a release. Update it together with the "The newest published build is swath X, ..."
sentence on the home page and in `docs/install.md` (with its note that the pages describe the upcoming
release, which goes once the release is out) and the image tags in `docs/install.md`. The channel check
requires that sentence on both pages and fails while any of them disagree, and
on the release channel it also fails unless the version is the newest released version in
[`CHANGELOG.md`](../CHANGELOG.md).
