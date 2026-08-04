const { chromium } = require('/usr/local/share/npm-global/lib/node_modules/@playwright/cli/node_modules/playwright');
const fs = require('fs'), path = require('path');
(async () => {
  const url = process.argv[2], OUT = '/tmp/frames';
  fs.rmSync(OUT, {recursive:true, force:true}); fs.mkdirSync(OUT, {recursive:true});
  const FPS = 30, SECONDS = 42, HOLD = 3;
  const b = await chromium.launch();
  const pg = await b.newPage({viewport:{width:1080,height:1350}, deviceScaleFactor:1});
  await pg.goto(url, {waitUntil:'networkidle'});
  await pg.waitForFunction('window.__seek !== undefined');
  const dur = await pg.evaluate('window.__duration');
  const pre = await pg.evaluate('window.__preroll || 0');
  const videoTime = await pg.evaluate('window.__videoTime === true');
  // A video-time template already owns its own pacing: step it linearly, no ease,
  // no pre-roll, and take its duration as the clip length.
  const total = videoTime ? Math.round(FPS*(dur/1000)) : FPS*SECONDS;
  const PRE = (!videoTime && pre) ? FPS*9 : 0;                    // watch the seed cuts land
  for (let i=0;i<PRE;i++){
    await pg.evaluate(`window.__seek(${-pre + (i/PRE)*pre})`);
    await pg.screenshot({path: path.join(OUT, `f${String(i).padStart(5,'0')}.png`)});
  }
  for (let i=0;i<total;i++){
    const u = i/(total-1), e = videoTime ? u : (3*u*u - 2*u*u*u);
    await pg.evaluate(`window.__seek(${e*dur})`);
    await pg.screenshot({path: path.join(OUT, `f${String(i+PRE).padStart(5,'0')}.png`)});
    if (i%90===0) console.log(`  ${i}/${total}`);
  }
  await pg.evaluate(`window.__seek(${dur})`);
  for (let j=total;j<total+FPS*HOLD;j++)
    await pg.screenshot({path: path.join(OUT, `f${String(j+PRE).padStart(5,'0')}.png`)});
  await b.close();
  console.log('frames:', fs.readdirSync(OUT).length);
})();
