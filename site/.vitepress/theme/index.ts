// The default VitePress theme with Varve's fonts and colors. Two layout slots carry site content:
// the development-docs notice above the navigation (themeConfig.devDocsNotice; false turns it off)
// and the terminal example beside the home page's hero.
// The fonts are the ones Varve's UI uses, self-hosted from npm: Inter for text and Geist Mono
// for code, in place of the theme's bundled Inter.
import { defineComponent, h } from 'vue'
import { useData, withBase } from 'vitepress'
import DefaultTheme from 'vitepress/theme-without-fonts'
import '@fontsource-variable/inter/opsz.css'
import '@fontsource-variable/geist-mono/wght.css'
import './custom.css'

// Real output of a live run on 2026-11-15 with main at d8fbe439: a sorted inventory of a public bucket, then a
// DuckDB query over it (docs/internals/measurements.md, "A public bucket for the home page"). The run was
// `swath diag list` with --trace and --report added, for the replay further down the page; swath's cost
// estimate line is left out where the output reads "…". Nothing else is changed.
const HERO_CAPTION =
  'A live run on a public NOAA bucket, 2026-11-15: list it once into a sorted Parquet inventory, then query it without listing again (DuckDB took about 3 s). … marks a line left out.'
const HERO_SESSION = [
  ['swath list s3://noaa-gestofs-pds/ -o noaa \\\n    --anonymous --s3-region us-east-1',
    `  37,064,981 objects in 1m02s · 598,740 keys/s
  39,098 API calls · in flight avg 53.25 · peak 64
  …
  12 parts · 825 MiB written · peak RSS 1.50 GiB`],
  [`duckdb -c "SELECT regexp_extract(key, '\\.(\\w+)$', 1) AS ext,
    count(*) AS objects, format_bytes(sum(size)::BIGINT) AS bytes
    FROM 'noaa/data/*.parquet'
    GROUP BY ext ORDER BY sum(size) DESC LIMIT 5"`,
    `┌─────────┬──────────┬──────────┐
│   ext   │ objects  │  bytes   │
│ varchar │  int64   │ varchar  │
├─────────┼──────────┼──────────┤
│ nc      │   382202 │ 1.3 PiB  │
│ grib2   │ 13178305 │ 39.8 TiB │
│ tgz     │     1051 │ 5.3 TiB  │
│ tar     │    18140 │ 4.5 TiB  │
│ png     │ 22913029 │ 1.3 TiB  │
└─────────┴──────────┴──────────┘`],
]

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
    h('figcaption', { class: 'swath-terminal-caption' }, HERO_CAPTION),
    h('pre', { tabindex: 0 }, [
      h('code', HERO_SESSION.flatMap(([command, output], i) => [
        i ? '\n' : '',
        h('span', { class: 'swath-terminal-prompt', 'aria-hidden': 'true' }, '$ '),
        command, '\n', output,
      ])),
    ]),
  ])

export default {
  extends: DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'layout-top': () => h(DevDocsBanner),
      'home-hero-image': () => HeroTerminal(),
    })
  },
}
