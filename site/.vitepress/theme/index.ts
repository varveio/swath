// The default VitePress theme with Varve's fonts and colors. Three layout slots carry site content:
// the development-docs notice above the navigation (themeConfig.devDocsNotice; false turns it off)
// and the home page's terminal example beside the hero and the replay under it.
// The fonts are the ones Varve's UI uses, self-hosted from npm: Inter for text and Geist Mono
// for code, in place of the theme's bundled Inter.
import { defineComponent, h } from 'vue'
import { useData, withBase } from 'vitepress'
import DefaultTheme from 'vitepress/theme-without-fonts'
import '@fontsource-variable/inter/opsz.css'
import '@fontsource-variable/geist-mono/wght.css'
import './custom.css'

const REPO = 'https://github.com/varveio/swath/blob/main/'

// Real output of a live run on 2026-11-15 with main at d8fbe439: a sorted inventory of a public bucket, then a
// DuckDB query over it (docs/internals/measurements.md, "A public bucket for the home page"). The run was
// `swath diag list` with --trace and --report added, for the replay below the hero; swath's cost estimate line
// is left out where the output reads "…", and DuckDB's trailing spaces are trimmed. Nothing else is changed.
const MEASURED = REPO + 'docs/internals/measurements.md#a-public-bucket-for-the-home-page-noaa-gestofs-pds-2026-11-15'
const HERO_CAPTION = [
  "On 2026-11-15 a build of main wrote this public bucket's sorted Parquet inventory in 61 seconds, and the " +
    'query over it took 3 seconds. The run was ',
  { code: 'swath diag list' },
  ', this command with a trace and a report added. Commands and output are abbreviated (… marks a line left ' +
    'out): ',
  { text: 'full details', href: MEASURED },
  '.',
]
const HERO_SESSION = [
  [`swath list s3://noaa-gestofs-pds/ -o noaa \\
    --anonymous --s3-region us-east-1`,
    `  37,064,981 objects in 1m00s · 609,802 keys/s
  39,016 API calls · in flight avg 52.27 · peak 64
  …
  11 parts · 825 MiB written · peak RSS 1.51 GiB`],
  [`duckdb -column -c "SELECT
    regexp_extract(key, '\\.(\\w+)$', 1) AS ext,
    count(*) AS objects,
    format_bytes(sum(size)::BIGINT) AS bytes
  FROM 'noaa/data/*.parquet'
  GROUP BY ext ORDER BY sum(size) DESC
  LIMIT 5"`,
    `ext    objects   bytes
-----  --------  --------
nc     382202    1.3 PiB
grib2  13178305  39.8 TiB
tgz    1051      5.3 TiB
tar    18140     4.5 TiB
png    22913029  1.3 TiB`],
]

// The replay of that run, under the hero. The video is a release asset that the deploy fetches and verifies
// (site/static/data/media.json, scripts/ci/fetch-site-media.sh); the poster is in site/static/media/.
const REPLAY = {
  video: '/media/noaa-gestofs-pds-replay.mp4',
  poster: '/media/noaa-gestofs-pds-replay-poster.jpg',
  label: 'A 36-second replay of the swath listing of s3://noaa-gestofs-pds shown above, rendered from its run trace',
  heading: 'The same run, replayed',
  text: [
    'The replay shows swath handing the busiest part of the bucket to workers as they become free, so the ' +
      'listing keeps moving without being told how the keys are laid out. Here one of its 257 starting ranges ' +
      'held 64.5% of the objects and was split 1,834 times.',
    'Measured once on main at d8fbe439 from a shared 4-vCPU cloud VM; the client\'s region was not recorded. ' +
      'The bucket changes over time, so a later listing differs.',
  ],
  links: [['How this was measured', MEASURED]],
}

interface DevDocsNotice {
  text: string
  linkText: string
  link: string
}

const DevDocsBanner = defineComponent({
  setup() {
    const { theme } = useData()
    return () => {
      const notice = theme.value.devDocsNotice as DevDocsNotice | false | undefined
      if (!notice) return null
      return h('div', { class: 'swath-dev-banner', role: 'note' }, [
        h('span', notice.text + ' '),
        h('a', { href: withBase(notice.link) }, notice.linkText),
      ])
    }
  },
})

const HeroTerminal = () =>
  h('figure', { class: 'swath-terminal' }, [
    h('figcaption', { class: 'swath-terminal-caption' }, HERO_CAPTION.map((c) =>
      typeof c === 'string' ? c : 'code' in c ? h('code', c.code) : h('a', { href: c.href }, c.text),
    )),
    h('pre', { tabindex: 0 }, [
      h('code', HERO_SESSION.flatMap(([command, output], i) => [
        i ? '\n' : '',
        h('span', { class: 'swath-terminal-prompt', 'aria-hidden': 'true' }, '$ '),
        command, '\n', output,
      ])),
    ]),
  ])

const HomeReplay = () =>
  h('section', { class: 'swath-replay', 'aria-labelledby': 'the-same-run-replayed' }, [
    h('div', { class: 'swath-replay-container' }, [
      h('video', {
        class: 'swath-replay-video',
        controls: true, muted: true, playsinline: true, preload: 'metadata',
        poster: withBase(REPLAY.poster), src: withBase(REPLAY.video), 'aria-label': REPLAY.label,
      }, 'The replay is an MP4 video your browser does not play.'),
      h('div', { class: 'swath-replay-text' }, [
        h('h2', { id: 'the-same-run-replayed' }, REPLAY.heading),
        ...REPLAY.text.map((t) => h('p', t)),
        h('p', { class: 'swath-replay-links' }, REPLAY.links.flatMap(([text, href], i) => [
          i ? ' · ' : '', h('a', { href }, text),
        ])),
      ]),
    ]),
  ])

export default {
  extends: DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'layout-top': () => h(DevDocsBanner),
      'home-hero-image': () => HeroTerminal(),
      'home-features-before': () => HomeReplay(),
    })
  },
}
