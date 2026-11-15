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

// Real output of `swath du -d 1 media` on the example tree of docs/first-inventory.md, verified with main.
const HERO_COMMAND = 'swath du -d 1 media'
const HERO_OUTPUT = `size (apparent)   on disk  files  dirs  path
       64.8 MiB   784 KiB      2     0  archive/
       6.10 MiB  6.11 MiB      3     2  photos/
       29.6 MiB  29.6 MiB      1     0  videos/
        100 MiB  36.4 MiB      6     5  (total)`

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
    h('figcaption', { class: 'swath-terminal-caption' }, 'Example: totals per directory, one level down'),
    h('pre', { tabindex: 0 }, [
      h('code', [h('span', { class: 'swath-terminal-prompt', 'aria-hidden': 'true' }, '$ '), HERO_COMMAND, '\n', HERO_OUTPUT]),
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
