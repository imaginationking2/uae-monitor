import { Actor } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

await Actor.init();

const TODAY = new Date().toISOString().split('T')[0];
const SCRAPED_AT = new Date().toISOString();

const results = {
  date: TODAY, scraped_at: SCRAPED_AT,
  dubizzle_jobs: null,
  dubizzle_motors: null,
  dubizzle_property_sale: null,
  dubizzle_property_rent: null,
  luxury: null,
  bayut_d9: null,
  bayut_ajman_sale: null,
  bayut_ajman_rent: null,
  benchmark: null,
  errors: []
};

// ── Targets ───────────────────────────────────────────────────────────────────
const TARGETS = [
  // Dubizzle — use residential proxy (previously worked for classified)
  { id: 'dubizzle_jobs',          url: 'https://uae.dubizzle.com/jobs/' },
  { id: 'dubizzle_motors',        url: 'https://uae.dubizzle.com/motors/' },
  { id: 'dubizzle_property_sale', url: 'https://uae.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'dubizzle_property_rent', url: 'https://uae.dubizzle.com/en/property-for-rent/residential/' },
  // Luxury — direct (no proxy needed)
  { id: 'luxury',                 url: 'https://www.luxurypricedrops.com/dubai/' },
  // Bayut — residential proxy
  { id: 'bayut_d9',               url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale',       url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent',       url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',              url: 'https://www.bayut.com/property/details-13073585.html' }
];

// ── Parsers ───────────────────────────────────────────────────────────────────

function parseDubizzleJobs(page_eval) {
  // Target: <a href="/jobs/s/type/full-time/"> ... <p ...>(1,211+ Jobs)</p>
  // page_eval returns {fullTime, total}
  if (!page_eval) return null;
  const ft = page_eval.fullTime;
  const m = ft?.match(/([\d,]+)/);
  const fullTimeCount = m ? parseInt(m[1].replace(/,/g,'')) : null;
  if (!fullTimeCount) return null;
  return { full_time_jobs: fullTimeCount };
}

function parseDubizzleMotors(page_eval) {
  // Target: <p data-testid="Used Cars">Used Cars</p><p>38,596</p>
  if (!page_eval || !page_eval.usedCars) return null;
  const m = page_eval.usedCars.match(/([\d,]+)/);
  return m ? { used_cars: parseInt(m[1].replace(/,/g,'')) } : null;
}

function parseDubizzlePropertySale(page_eval) {
  // Target: <h1>Properties for sale in UAE<span>•</span><span>246,503 Ads</span></h1>
  if (!page_eval || !page_eval.total) return null;
  const m = page_eval.total.match(/([\d,]+)/);
  return m ? { total_uae: parseInt(m[1].replace(/,/g,'')) } : null;
}

function parseDubizzlePropertyRent(page_eval) {
  // Target: <h1>Properties for rent in UAE<span>•</span><span>220,048 Ads</span></h1>
  // + emirate breakdown from city filter chips
  if (!page_eval || !page_eval.total) return null;
  const m = page_eval.total.match(/([\d,]+)/);
  const total = m ? parseInt(m[1].replace(/,/g,'')) : null;
  if (!total) return null;
  return {
    total_uae: total,
    dubai: page_eval.dubai || null,
    ajman: page_eval.ajman || null,
    sharjah: page_eval.sharjah || null,
    abu_dhabi: page_eval.abu_dhabi || null
  };
}

function parseLuxury(page_eval) {
  // Target: "2,775 drops  −6.6% avg  38.0K watching"
  if (!page_eval || !page_eval.statsText) return null;
  const dropM = page_eval.statsText.match(/([\d,]+)\s*drops/i);
  const avgM = page_eval.statsText.match(/−([\d.]+)%\s*avg/i);
  const watchM = page_eval.statsText.match(/([\d.]+)K\s*watching/i);
  const drops = dropM ? parseInt(dropM[1].replace(/,/g,'')) : null;
  if (!drops) return null;
  return {
    drop_count: drops,
    avg_drop_pct: avgM ? parseFloat(avgM[1]) : null,
    watching: watchM ? Math.round(parseFloat(watchM[1]) * 1000) : null
  };
}

function parseBayutCount(text) {
  if (!text) return null;
  const cl = text.split('\n').map(l=>l.trim()).find(l=>/\d+ to \d+ of [\d,]+ Propert/i.test(l));
  if (cl) return parseInt(cl.match(/of ([\d,]+)/)?.[1]?.replace(/,/g,''));
  const m = text.match(/([\d,]+)\s+Propert/i);
  return m ? parseInt(m[1].replace(/,/g,'')) : null;
}

function parseBayutD9(text) {
  const count = parseBayutCount(text);
  const am = text?.split('\n').map(l=>l.trim()).find(l=>/average sale price.*AED/i.test(l));
  const avg = am ? parseInt(am.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g,'')) : null;
  return count != null ? { district9_listings: count, avg_sale_price: avg } : null;
}

function parseBayutAjmanSale(text) {
  const count = parseBayutCount(text);
  const avgLine = text?.split('\n').map(l=>l.trim()).find(l=>/average sale price.*AED/i.test(l));
  const avg = avgLine ? parseInt(avgLine.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g,'')) : null;
  return count != null ? { count, avg_sale_price: avg } : null;
}

function parseBenchmark(title) {
  const m = title?.match(/AED ([\d.]+)M/i);
  const price = m ? Math.round(parseFloat(m[1]) * 1e6) : null;
  return price ? { benchmark_price_aed: price, benchmark_flag: 'NO CHANGE' } : null;
}

function computeStress(r) {
  // Keep original 4-component model — dubizzle component now uses full_time_jobs as proxy
  // Baseline full_time_jobs: 1,211 (May 2026) — INVERTED: fewer jobs = more stress
  const jobs = r.dubizzle_jobs?.full_time_jobs || 1211;
  const jobsScore = Math.min(25, Math.max(0, Math.round((1211 - jobs) / 1211 * 100 / 0.4)));

  const drops = r.luxury?.drop_count || 1542;
  const luxuryScore = Math.min(25, Math.round((drops - 1542) / 1542 * 100 / 2));

  const d9 = r.bayut_d9?.district9_listings ?? 31;
  const bayutScore = Math.min(25, Math.round((31 - d9) / 31 * 100 / 2));

  const sale = r.bayut_ajman_sale?.count || 0;
  const rent = r.bayut_ajman_rent || 1;
  const ratio = (sale && rent) ? parseFloat((sale / rent).toFixed(3)) : 0;
  const ratioScore = Math.min(25, Math.max(0, Math.round(ratio * 10 - 12)));

  const total = jobsScore + luxuryScore + bayutScore + ratioScore;
  const band = total < 30 ? 'Stable - no signal'
    : total < 45 ? 'Mild stress building'
    : total < 60 ? 'Clear stress building'
    : total < 75 ? 'High stress - monitor closely'
    : 'Crisis signal';

  return { total, band, components: { jobs: jobsScore, luxury: luxuryScore, bayut: bayutScore, ajman_ratio: ratioScore }, ratio };
}

// ── Proxy ─────────────────────────────────────────────────────────────────────
let proxyConfiguration;
try {
  proxyConfiguration = await Actor.createProxyConfiguration({ groups: ['RESIDENTIAL'], countryCode: 'AE' });
} catch (e) { console.log('Proxy failed: ' + e.message); }

// ── Crawler ───────────────────────────────────────────────────────────────────
const crawler = new PlaywrightCrawler({
  proxyConfiguration,
  maxRequestRetries: 2,
  navigationTimeoutSecs: 90,
  requestHandlerTimeoutSecs: 120,
  maxConcurrency: 1,
  launchContext: { launchOptions: { args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu'] } },

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;
    log.info('Scraping: ' + id);

    try { await page.waitForLoadState('networkidle', { timeout: 30000 }); }
    catch { await page.waitForLoadState('domcontentloaded'); await sleep(3000); }

    const title = await page.title();
    log.info(id + ': title="' + title.substring(0,80) + '"');

    if (/captcha|robot|blocked|challenge|interruption/i.test(title)) {
      throw new Error('Blocked on ' + id + ': ' + title);
    }

    let parsed = null;

    switch(id) {

      case 'dubizzle_jobs': {
        // Extra wait for React to render job type cards
        await sleep(3000);
        const eval_result = await page.evaluate(() => {
          // Full-time: <a href="/jobs/s/type/full-time/"> ... <p>(1,211+ Jobs)</p>
          const ftLink = document.querySelector('a[href*="full-time"]');
          const ftP = ftLink?.querySelector('p');
          const fullTime = ftP?.textContent?.trim() || null;
          // Also grab total from page heading if available
          const heading = document.querySelector('h1, [class*="heading"]')?.textContent?.trim() || null;
          return { fullTime, heading };
        });
        log.info('jobs eval: ' + JSON.stringify(eval_result));
        parsed = parseDubizzleJobs(eval_result);
        if (!parsed) throw new Error('dubizzle_jobs parse failed - fullTime=' + eval_result?.fullTime);
        results.dubizzle_jobs = parsed;
        break;
      }

      case 'dubizzle_motors': {
        await sleep(2000);
        const eval_result = await page.evaluate(() => {
          // <p data-testid="Used Cars">Used Cars</p><p>38,596</p>
          const usedCarsLabel = document.querySelector('p[data-testid="Used Cars"]');
          const usedCarsCount = usedCarsLabel?.nextElementSibling?.textContent?.trim() || null;
          // Fallback: find by text proximity
          const allP = Array.from(document.querySelectorAll('p'));
          let usedCarsAlt = null;
          for (let i = 0; i < allP.length - 1; i++) {
            if (allP[i].textContent.trim() === 'Used Cars') {
              usedCarsAlt = allP[i+1]?.textContent?.trim();
              break;
            }
          }
          return { usedCars: usedCarsCount || usedCarsAlt };
        });
        log.info('motors eval: ' + JSON.stringify(eval_result));
        parsed = parseDubizzleMotors(eval_result);
        if (!parsed) throw new Error('dubizzle_motors parse failed - usedCars=' + eval_result?.usedCars);
        results.dubizzle_motors = parsed;
        break;
      }

      case 'dubizzle_property_sale': {
        await sleep(2000);
        const eval_result = await page.evaluate(() => {
          // <h1>Properties for sale in UAE<span>•</span><span>246,503 Ads</span></h1>
          const h1 = document.querySelector('h1');
          const spans = h1?.querySelectorAll('span');
          const countSpan = spans ? Array.from(spans).find(s => /\d/.test(s.textContent)) : null;
          const total = countSpan?.textContent?.trim() || h1?.textContent?.trim() || null;
          return { total };
        });
        log.info('prop_sale eval: ' + JSON.stringify(eval_result));
        parsed = parseDubizzlePropertySale(eval_result);
        if (!parsed) throw new Error('dubizzle_property_sale parse failed - total=' + eval_result?.total);
        results.dubizzle_property_sale = parsed;
        break;
      }

      case 'dubizzle_property_rent': {
        await sleep(2000);
        const eval_result = await page.evaluate(() => {
          // <h1>Properties for rent in UAE<span>•</span><span>220,048 Ads</span></h1>
          const h1 = document.querySelector('[data-testid="page-title"] h1') || document.querySelector('h1');
          const spans = h1?.querySelectorAll('span');
          const countSpan = spans ? Array.from(spans).find(s => /\d/.test(s.textContent)) : null;
          const total = countSpan?.textContent?.trim() || null;
          // Emirate counts from filter chips: "Dubai (111,367)" style
          const lines = document.body.innerText.split('\n').map(l=>l.trim()).filter(Boolean);
          const getEmirate = (name) => {
            const idx = lines.findIndex(l => new RegExp('^' + name + '$','i').test(l));
            if (idx < 0) return null;
            const ctx = lines.slice(idx, idx+5).join(' ');
            const m = ctx.match(/([\d,]{4,})/);
            return m ? parseInt(m[1].replace(/,/g,'')) : null;
          };
          return {
            total,
            dubai: getEmirate('Dubai'),
            ajman: getEmirate('Ajman'),
            sharjah: getEmirate('Sharjah'),
            abu_dhabi: getEmirate('Abu Dhabi')
          };
        });
        log.info('prop_rent eval: ' + JSON.stringify(eval_result));
        parsed = parseDubizzlePropertyRent(eval_result);
        if (!parsed) throw new Error('dubizzle_property_rent parse failed - total=' + eval_result?.total);
        results.dubizzle_property_rent = parsed;
        break;
      }

      case 'luxury': {
        await sleep(2000);
        const eval_result = await page.evaluate(() => {
          // Find: "2,775 drops  −6.6% avg  38.0K watching"
          const allText = document.body.innerText;
          const lines = allText.split('\n').map(l=>l.trim()).filter(Boolean);
          const statsLine = lines.find(l => /drops.*avg.*watching/i.test(l) || /[\d,]+\s*drops/i.test(l));
          // Also try finding individual elements
          const dropEl = Array.from(document.querySelectorAll('*')).find(e =>
            e.children.length === 0 && /^[\d,]+$/.test(e.textContent.trim()) &&
            parseInt(e.textContent.replace(/,/g,'')) > 100
          );
          return { statsText: statsLine, dropCount: dropEl?.textContent?.trim() };
        });
        log.info('luxury eval: ' + JSON.stringify(eval_result));
        parsed = parseLuxury(eval_result);
        if (!parsed) throw new Error('luxury parse failed - stats=' + eval_result?.statsText);
        results.luxury = parsed;
        break;
      }

      case 'bayut_d9': {
        const bodyText = await page.evaluate(() => document.body?.innerText || '');
        parsed = parseBayutD9(bodyText);
        if (!parsed) throw new Error('bayut_d9 parse failed');
        results.bayut_d9 = parsed;
        break;
      }

      case 'bayut_ajman_sale': {
        const bodyText = await page.evaluate(() => document.body?.innerText || '');
        parsed = parseBayutAjmanSale(bodyText);
        if (!parsed) throw new Error('bayut_ajman_sale parse failed');
        results.bayut_ajman_sale = parsed;
        break;
      }

      case 'bayut_ajman_rent': {
        const bodyText = await page.evaluate(() => document.body?.innerText || '');
        parsed = parseBayutCount(bodyText);
        if (!parsed) throw new Error('bayut_ajman_rent parse failed');
        results.bayut_ajman_rent = parsed;
        break;
      }

      case 'benchmark': {
        parsed = parseBenchmark(title);
        if (!parsed) throw new Error('benchmark parse failed');
        results.benchmark = parsed;
        break;
      }
    }

    log.info('OK ' + id + ': ' + JSON.stringify(parsed).substring(0,120));
    await page.close();
  },

  failedRequestHandler({ request, error }) {
    console.error('FAILED ' + request.userData.id + ': ' + error.message);
    results.errors.push({ source: request.userData.id, error: error.message });
  }
});

await crawler.run(TARGETS.map(t => ({ url: t.url, userData: { id: t.id } })));

const stress = computeStress(results);
const bayutSaleCount = results.bayut_ajman_sale?.count || null;
const bayutSaleAvg = results.bayut_ajman_sale?.avg_sale_price || null;

const output = {
  date: TODAY, scraped_at: SCRAPED_AT,
  stress: { date: TODAY, total: stress.total, band: stress.band, components: stress.components },
  dubizzle_jobs_entry: results.dubizzle_jobs ? { date: TODAY, ...results.dubizzle_jobs } : null,
  dubizzle_motors_entry: results.dubizzle_motors ? { date: TODAY, ...results.dubizzle_motors } : null,
  dubizzle_property_sale_entry: results.dubizzle_property_sale ? { date: TODAY, ...results.dubizzle_property_sale } : null,
  dubizzle_property_rent_entry: results.dubizzle_property_rent ? { date: TODAY, ...results.dubizzle_property_rent } : null,
  luxury_entry: results.luxury ? { date: TODAY, ...results.luxury } : null,
  bayut_entry: results.bayut_d9 ? {
    date: TODAY,
    district9_listings: results.bayut_d9.district9_listings,
    d9_avg_price: results.bayut_d9.avg_sale_price,
    benchmark_price_aed: results.benchmark?.benchmark_price_aed ?? 3200000,
    benchmark_flag: 'NO CHANGE'
  } : null,
  ajman_entry: (bayutSaleCount && results.bayut_ajman_rent) ? {
    date: TODAY, scraped_at: SCRAPED_AT,
    ajman_for_sale: bayutSaleCount,
    ajman_for_sale_avg_price: bayutSaleAvg,
    ajman_for_rent: results.bayut_ajman_rent,
    ratio: stress.ratio
  } : null,
  errors: results.errors,
  success: results.errors.length === 0
};

console.log('=== FINAL OUTPUT ===');
console.log(JSON.stringify(output, null, 2));
await Actor.pushData(output);
await Actor.setValue('OUTPUT', output);
await Actor.exit();
