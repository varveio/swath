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
| `404.md` | The page for a missing address. VitePress renders it in the browser. |
| `.vitepress/config.mts` | Navigation, sidebar, search, the pages that are published and the link rewriting. |
| `.vitepress/theme/` | The default theme with Varve's colors and fonts (Inter, Geist Mono, self-hosted from npm); no custom components. |
| `static/` | Copied verbatim into the site root: `CNAME`, `.nojekyll`, `favicon.svg`, the nav logo (`logo-light.svg`, `logo-dark.svg`), the social card `card.png`, and `data/channel.json`. |
| `card/` | The social card's source, `card.svg`, and `render.sh`, which renders it to `static/card.png`. |
| `package.json`, `package-lock.json` | VitePress, pinned to 1.6.4, and the font packages, with their locked dependencies. |

## How the documentation is published

The Markdown in `docs/` is the source of truth and stays readable on GitHub: no front matter, relative `.md`
links, GitHub alerts and GitHub heading anchors. The build adapts it without editing it:

- `docs/PAGE.md` is served at `/docs/PAGE.html`, and `docs/README.md` at `/docs/`.
- Contributor pages are not published or searchable: `docs/internals/`, `docs/ops/`, `docs/style.md` and
  `docs/upgrade.md`. Every other Markdown file under `docs/` is published.
- Headings get GitHub's anchors, so `page.md#heading` links resolve on the site as they do on GitHub.
- A link from a published page to anything outside the published set (an internals page, code, the
  repository README) is rewritten to its GitHub URL on `main`. The build prints how many it rewrote.
- Each page has an "Edit this page on GitHub" link to its source.

A new user page in `docs/` needs a sidebar entry in `.vitepress/config.mts`; it is published either way.

## Before you push

```bash
./scripts/ci/build-site.sh build/site            # exactly what gets deployed; needs Node.js and npm
python3 scripts/ci/check-site.py --site build/site
```

The same two commands run in CI and gate the deploy. The build fails on a dead link between pages. The
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

## Releasing a new version

`static/data/channel.json` names the version the site offers: `{"channel": "main", "version": "X.Y.Z-rc.N"}`
while the documentation describes `main` ahead of a release, `{"channel": "release", "version": "X.Y.Z"}`
once it describes a release. Update it together with the "The newest published build is swath X, ..."
sentence on the home page and in `docs/install.md` (with its note that the pages describe the upcoming
release, which goes once the release is out) and the image tags in `docs/install.md`. The channel check
requires that sentence on both pages and fails while any of them disagree, and
on the release channel it also fails unless the version is the newest released version in
[`CHANGELOG.md`](../CHANGELOG.md).
