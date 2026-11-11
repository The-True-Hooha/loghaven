// Drives the dashboard in headless Chromium. Needs `npm i playwright && npx playwright install chromium`.
// Seed the app first (see docs/MANUAL_TESTING.md, "Phase 3: dashboard"), with exactly these three records:
//   TX=0x$(printf 'cd%.0s' $(seq 32))
//   loghaven send web 'plain hello'
//   loghaven send web '<img src=x onerror="window.PWNED=1"><script>window.PWNED=2</script>' --level error --tx-hash $TX
//   loghaven send web 'payment confirmed' --level warn --chain ethereum --tx-hash $TX
// then: node scripts/dashboard_e2e.js "$(loghaven dashboard --app web --no-open)"
const { chromium } = require('playwright');
const url = process.argv[2];
const outDir = process.argv[3] || '.';
if (!url) { console.error('usage: node dashboard_e2e.js <dashboard url> [screenshot dir]'); process.exit(2); }
const TX = '0x' + 'cd'.repeat(32);
const ok = (c, m) => { console.log((c ? 'PASS ' : 'FAIL ') + m); if (!c) process.exitCode = 1; };
(async () => {
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ viewport: { width: 1100, height: 700 } });
  const page = await ctx.newPage();
  const dialogs = [], consoleMsgs = [], referers = [];
  page.on('dialog', d => { dialogs.push(d.message()); d.dismiss(); });
  page.on('console', m => consoleMsgs.push(m.text()));
  page.on('request', r => referers.push([r.url(), r.headers()['referer']]));
  await page.goto(url);
  await page.waitForSelector('#rows tr', { timeout: 8000 });
  await page.waitForTimeout(2500);
  ok((await page.locator('#rows tr').count()) === 3, '3 rows rendered');
  ok((await page.evaluate(() => window.PWNED)) === undefined, 'no script from log text ran');
  ok(dialogs.length === 0, 'no dialogs');
  ok((await page.locator('#rows img').count()) === 0, 'no img element from log text');
  ok((await page.locator('#rows td.msg', { hasText: '<img src=x' }).count()) === 1, 'markup shown as text');
  ok(!page.url().includes('token') && !page.url().includes('#'), 'fragment removed from address bar: ' + page.url());
  ok(await page.evaluate(() => history.length) <= 2, 'no extra history entries');
  ok(referers.every(([, r]) => !r), 'no Referer header sent');
  ok(!(await page.evaluate(() => JSON.stringify(sessionStorage) + JSON.stringify(localStorage) + document.cookie)).match(/[0-9a-f]{64}/), 'token not in storage or cookies');
  // CSP: inline script and eval are blocked
  await page.evaluate(() => { const s = document.createElement('script'); s.textContent = 'window.INLINE=1'; document.body.append(s); });
  ok((await page.evaluate(() => window.INLINE)) === undefined, 'CSP blocks inline script');
  ok(consoleMsgs.some(m => /Content Security Policy/i.test(m)), 'CSP violation reported');
  // interactions
  await page.locator('#rows tr', { hasText: 'payment confirmed' }).click();
  ok(await page.locator('#detail').isVisible(), 'detail opens');
  ok((await page.locator('#fields').innerText()).includes(TX), 'detail shows tx hash');
  await page.click('#trace');
  await page.waitForTimeout(1200);
  ok((await page.locator('#rows tr').count()) === 2, 'trace lists the 2 records with the hash');
  ok((await page.locator('#title').innerText()).includes('trace'), 'title says trace');
  await page.keyboard.press('Escape');
  ok((await page.locator('#rows tr').count()) === 3, 'Escape leaves the trace');
  await page.selectOption('#level', 'error');
  ok((await page.locator('#rows tr').count()) === 1, 'level filter error');
  await page.selectOption('#level', 'all');
  await page.fill('#text', 'payment');
  ok((await page.locator('#rows tr').count()) === 1, 'text filter');
  await page.fill('#text', '');
  await page.click('#follow');
  ok((await page.locator('#status').innerText()) === 'paused', 'pause');
  await page.screenshot({ path: outDir + '/dash-light.png' });
  await page.emulateMedia({ colorScheme: 'dark' });
  await page.screenshot({ path: outDir + '/dash-dark.png' });
  // phone width
  await page.setViewportSize({ width: 375, height: 700 });
  ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'no horizontal scroll at 375px');
  // Host header check from the browser's network stack
  const r = await ctx.request.get('http://127.0.0.1:9200/dashboard', { headers: { Host: 'evil.example:9200' } });
  ok(r.status() === 403, 'evil Host refused: ' + r.status());
  // bad token
  const p2 = await ctx.newPage();
  await p2.goto(url.replace(/token=[0-9a-f]+/, 'token=bogus'));
  await p2.waitForTimeout(2500);
  ok(/expired|not allowed/.test(await p2.locator('#banner').innerText()), 'bad token shows banner');
  ok((await p2.locator('#status').innerText()) === 'stopped', 'bad token stops polling');
  // no token in page without fragment
  const p3 = await ctx.newPage();
  await p3.goto('http://127.0.0.1:9200/dashboard');
  ok(/loghaven dashboard/.test(await p3.locator('#banner').innerText()), 'bare page explains how to open it');
  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
