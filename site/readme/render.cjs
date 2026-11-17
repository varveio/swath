// Screenshots the home page's lead terminal, the HERO session of site/.vitepress/theme/index.ts, for
// site/readme/render.sh: at twice its CSS size, in the dark color scheme, with nothing behind its rounded
// corners, so the image reads on both of GitHub's themes.
//
// Usage: node render.cjs URL OUT.png   (needs `playwright` resolvable, through NODE_PATH or otherwise)
const { chromium } = require('playwright')

const [url, out] = process.argv.slice(2)
if (!url || !out) {
  console.error('usage: node render.cjs URL OUT.png')
  process.exit(2)
}

;(async () => {
  const browser = await chromium.launch()
  try {
    // At 960px and wider the theme sets the terminal in 14px type, the size the home page shows it at.
    const page = await browser.newPage({
      viewport: { width: 1400, height: 1000 }, deviceScaleFactor: 2, colorScheme: 'dark',
    })
    await page.goto(url, { waitUntil: 'networkidle' })
    await page.evaluate(() => document.fonts.ready)
    const terminal = page.locator('.VPHero .swath-terminal pre')
    if (await terminal.evaluate((el) => el.scrollWidth > el.clientWidth)) {
      throw new Error('the terminal scrolls inside its box at this viewport, so the image would cut it off')
    }
    await page.addStyleTag({
      content: '.VPHero .image-bg { display: none } ' +
        'html, body, #app, .Layout, .VPContent, .VPHome, .VPHero, .VPHero .image, .VPHero .image-container, ' +
        '.swath-terminal { background: transparent !important }',
    })
    await terminal.screenshot({ path: out, omitBackground: true })
  } finally {
    await browser.close()
  }
})().catch((err) => {
  console.error(err)
  process.exit(1)
})
