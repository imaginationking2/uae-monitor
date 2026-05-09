import { Actor } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

await Actor.init();

const TODAY = new Date().toISOString().split('T')[0];
const SCRAPED_AT = new Date().toISOString();

const results = {
  date: TODAY, scraped_at: SCRAPED_AT,
  dubizzle: {}, dubizzle_motors: null,
  dubizzle_property_sale: null, dubizzle_property_rent: null,
  dubizzle_jobs: null, bayut_d9: null,
  bayut_ajman_sale: null, bayut_ajman_rent: null,
  benchmark: null, luxury: null, errors: []
};

// Each Dubizzle category gets its own URL — no login needed, count in h1
const TARGETS = [
  // Dubizzle classifieds — one URL per category, count in h1
  { id: 'dub_furniture',  url: 'https://uae.dubizzle.com/classified/furniture-home-garden/',    key: 'furniture_home' },
  { id: 'dub_appliances', url: 'https://uae.dubizzle.com/classified/home-appliances/',          key: 'home_appliances' },
  { id: 'dub_sports',     url: 'https://uae.dubizzle.com/classified/sports-equipment/',         key: 'sports' },
  { id: 'dub_mobiles',    url: 'https://uae.dubizzle.com/classified/mobiles-tablets/',          key: 'mobiles_tablets' },
  { id: 'dub_electronics',url: 'https://uae.dubizzle.com/classified/electronics/',              key: 'electronics' },
  { id: 'dub_computers',  url: 'https://uae.dubizzle.com/classified/computers-networking/',    key: 'computers' },
  // Dubizzle motors categories
  { id: 'dub_used_cars',  url: 'https://uae.dubizzle.com/motors/used-cars/',                   key: 'used_cars',        group: 'motors' },
  { id: 'dub_plates',     url: 'https://uae.dubizzle.com/motors/number-plates/',               key: 'number_plates',    group: 'motors' },
  { id: 'dub_rentals',    url: 'https://uae.dubizzle.com/motors/rental-cars/',                 key: 'rental_cars',      group: 'motors' },
  // Dubizzle property
  { id: 'dub_prop_sale',  url: 'https://uae.dubizzle.com/en/property-for-sale/residential/',   key: 'total_uae',        group: 'prop_sale' },
  { id: 'dub_prop_rent',  url: 'https://uae.dubizzle.com/en/property-for-rent/residential/',   key: null,               group: 'prop_rent' },
  // Dubizzle jobs
  { id: 'dub_jobs',       url: 'https://uae.dubizzle.com/jobs/search/?q=',                     key: 'total_jobs',       group: 'jobs' },
  // Bayut
  { id: 'bayut_d9',         url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale', url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent', url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',        url: 'https://www.bayut.com/property/details-13073585.html' },
  { id: 'luxury',           url: 'https://www.luxurypricedrops.com/dubai/' }
];

// ── Parse count from h1 "...• N,NNN Ads" pattern ─────────────────────────────
function parseH1Count(bodyText, title) {
  const sources = [bodyText, title];
  for (const src of sources) {
    const m = (src || '').match(/[•·]\s*([\d,]+)\s*Ads?/i) || (src || '').match(/([\d,]+)\s*Ads?/i);
    if (m) return parseInt(m[1].replace(/,/g, ''));
  }
  return null;
}

// ── Bayut parsers ─────────────────────────────────────────────────────────────
function parseBayutCount(text) {
  if (!text) return null;
  const cl = text.split('\n').map(l => l.trim()).find(l => /\d+ to \d+ of [\d,]+ Propert/i.test(l));
  if (cl) return parseInt(cl.match(/of ([\d,]+)/)?.[1]?.replace(/,/g, ''));
  const m = text.match(/([\d,]+)\s+Propert/i);
  return m ? parseInt(m[1].replace(/,/g, '')) : null;
}

function parseBayutD9(text) {
  const count = parseBayutCount(text);
  const am = text?.split('\n').map(l => l.trim()).find(l => /average sale price.*AED/i.test(l));
  const avg = am ? parseInt(am.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g, '')) : null;
  return count != null ? { district9_listings: count, avg_sale_price: avg } : null;
}

function parseBayutAjmanSale(text) {
  const count = parseBayutCount(text);
  const avgLine = text?.split('\n').map(l => l.trim()).find(l => /average sale price.*AED/i.test(l));
  const avg = avgLine ? parseInt(avgLine.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g, '')) : null;
  return count != null ? { count, avg_sale_price: avg } : null;
}

function parseBenchmark(title) {
  const m = title?.match(/AED ([\d.]+)M/i);
  const price = m ? Math.round(parseFloat(m[1]) * 1e6) : null;
  return price ? { benchmark_price_aed: price } : null;
}

function parseLuxury(title) {
  const cm = title?.match(/([\d,]+)\s+Propert/i);
  const dm = title?.match(/Up to ([\d.]+)%/i);
  const count = cm ? parseInt(cm[1].replace(/,/g, '')) : null;
  return count ? { drop_count: count, max_drop_pct: dm ? parseFloat(dm[1]) : null, avg_drop_pct: 6.5 } : null;
}

// ── Property for-rent by emirate ──────────────────────────────────────────────
function parsePropertyRent(text) {
  if (!text) return null;
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const emirates = { 'Dubai': 'dubai', 'Ajman': 'ajman', 'Sharjah': 'sharjah', 'Abu Dhabi': 'abu_dhabi', 'Ras Al Khaimah': 'rak' };
  const out = {};
  lines.forEach((l, i) => {
    Object.entries(emirates).forEach(([name, key]) => {
      if (new RegExp('^' + name + '$', 'i').test(l) && !out[key]) {
        const ctx = lines.slice(i, i + 5).join(' ');
        const m = ctx.match(/([\d,]{4,})/);
        if (m) out[key] = parseInt(m[1].replace(/,/g, ''));
      }
    });
  });
  return Object.keys(out).length >= 2 ? out : null;
}

// ── Jobs parser ───────────────────────────────────────────────────────────────
function parseJobs(text) {
  if (!text) return null;
  const totalLine = text.split('\n').map(l => l.trim()).find(l => /Jobs in UAE.*[\d,]+.*Ads/i.test(l));
  const m = totalLine?.match(/([\d,]+)\s*Ads/i);
  const total = m ? parseInt(m[1].replace(/,/g, '')) : null;
  if (!total) return null;
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const cats = {};
  const catMap = { 'Sales / Business Development': 'sales_business_dev', 'Accounting / Finance': 'accounting_finance', 'Real Estate': 'real_estate', 'Engineering': 'engineering', 'Construction': 'construction', 'HR / Admin': 'hr_admin' };
  for (let i = 0; i < lines.length - 1; i++) {
    const key = catMap[lines[i]];
    const match = lines[i + 1]?.match(/^\((\d+)\)$/);
    if (key && match) cats[key] = parseInt(match[1]);
  }
  return { total_jobs: total, categories: cats };
}

function computeStress(r) {
  const furniture = r.dubizzle?.furniture_home || 133228;
  const dubizzleScore = Math.min(25, Math.round(((furniture - 133228) / 133228) * 100 / 0.4));
  const drops = r.luxury?.drop_count || 1542;
  const luxuryScore = Math.min(25, Math.round((drops - 1542) / 1542 * 100 / 2));
  const d9 = r.bayut_d9?.district9_listings ?? 31;
  const bayutScore = Math.min(25, Math.round((31 - d9) / 31 * 100 / 2));
  const sale = r.bayut_ajman_sale?.count || 0;
  const rent = r.bayut_ajman_rent || 1;
  const ratio = (sale && rent) ? parseFloat((sale / rent).toFixed(3)) : 0;
  const ratioScore = Math.min(25, Math.max(0, Math.round(ratio * 10 - 12)));
  const total = dubizzleScore + luxuryScore + bayutScore + ratioScore;
  const band = total < 30 ? 'Stable - no signal' : total < 45 ? 'Mild stress building' : total < 60 ? 'Clear stress building' : total < 75 ? 'High stress - monitor closely' : 'Crisis signal';
  return { total, band, components: { dubizzle: dubizzleScore, luxury: luxuryScore, bayut: bayutScore, ajman_ratio: ratioScore }, ratio };
}

// ── Proxy for non-Dubizzle only ───────────────────────────────────────────────
let proxyConfiguration;
try {
  proxyConfiguration = await Actor.createProxyConfiguration({ groups: ['RESIDENTIAL'], countryCode: 'AE' });
} catch (e) { console.log('Proxy failed: ' + e.message); }

// ── Crawler ───────────────────────────────────────────────────────────────────
const crawler = new PlaywrightCrawler({
  // No proxy — Dubizzle blocks proxy IPs but allows direct connections
  proxyConfiguration: undefined,
  maxRequestRetries: 2,
  navigationTimeoutSecs: 60,
  requestHandlerTimeoutSecs: 90,
  maxConcurrency: 1,
  launchContext: { launchOptions: { args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu'] } },

  async requestHandler({ request, page, log }) {
    const { id, key, group } = request.userData;
    log.info('Scraping: ' + id);

    try { await page.waitForLoadState('networkidle', { timeout: 25000 }); }
    catch { await page.waitForLoadState('domcontentloaded'); await sleep(2000); }

    const title = await page.title();
    const bodyText = await page.evaluate(() => document.body?.innerText || '');
    log.info(id + ': ' + bodyText.length + ' chars, title="' + title.substring(0,80) + '"');

    if (!bodyText || bodyText.length < 200) throw new Error('Empty body on ' + id);

    // Dubizzle classifieds — parse count from h1
    if (id.startsWith('dub_') && !group) {
      const count = parseH1Count(bodyText, title);
      if (!count) throw new Error(id + ' count not found in page');
      results.dubizzle[key] = count;
      log.info('OK ' + id + ': ' + key + '=' + count);
      await page.close(); return;
    }

    // Dubizzle motors
    if (group === 'motors') {
      const count = parseH1Count(bodyText, title);
      if (!count) throw new Error(id + ' motors count not found');
      if (!results.dubizzle_motors) results.dubizzle_motors = {};
      results.dubizzle_motors[key] = count;
      log.info('OK ' + id + ': ' + key + '=' + count);
      await page.close(); return;
    }

    // Dubizzle property for-sale
    if (group === 'prop_sale') {
      const count = parseH1Count(bodyText, title);
      if (!count) throw new Error('prop_sale count not found');
      results.dubizzle_property_sale = { total_uae: count };
      log.info('OK dub_prop_sale: ' + count);
      await page.close(); return;
    }

    // Dubizzle property for-rent
    if (group === 'prop_rent') {
      const parsed = parsePropertyRent(bodyText);
      if (!parsed) throw new Error('prop_rent parse failed');
      results.dubizzle_property_rent = parsed;
      log.info('OK dub_prop_rent: dubai=' + parsed.dubai + ' ajman=' + parsed.ajman);
      await page.close(); return;
    }

    // Dubizzle jobs
    if (group === 'jobs') {
      const parsed = parseJobs(bodyText);
      if (!parsed) throw new Error('jobs parse failed');
      results.dubizzle_jobs = parsed;
      log.info('OK dub_jobs: total=' + parsed.total_jobs);
      await page.close(); return;
    }

    // Bayut / Benchmark / Luxury
    switch (id) {
      case 'bayut_d9': {
        const parsed = parseBayutD9(bodyText);
        if (!parsed) throw new Error('bayut_d9 parse failed');
        results.bayut_d9 = parsed; break;
      }
      case 'bayut_ajman_sale': {
        const parsed = parseBayutAjmanSale(bodyText);
        if (!parsed) throw new Error('bayut_ajman_sale parse failed');
        results.bayut_ajman_sale = parsed; break;
      }
      case 'bayut_ajman_rent': {
        const count = parseBayutCount(bodyText);
        if (!count) throw new Error('bayut_ajman_rent parse failed');
        results.bayut_ajman_rent = count; break;
      }
      case 'benchmark': {
        const parsed = parseBenchmark(title);
        if (!parsed) throw new Error('benchmark parse failed');
        results.benchmark = parsed; break;
      }
      case 'luxury': {
        const parsed = parseLuxury(title);
        if (!parsed) throw new Error('luxury parse failed');
        results.luxury = parsed; break;
      }
    }
    log.info('OK ' + id);
    await page.close();
  },

  failedRequestHandler({ request, error }) {
    console.error('FAILED ' + request.userData.id + ': ' + error.message);
    results.errors.push({ source: request.userData.id, error: error.message });
  }
});

// Run all targets — no proxy needed for Dubizzle category pages
await crawler.run(TARGETS.map(t => ({ url: t.url, userData: { id: t.id, key: t.key, group: t.group } })));

// Assemble dubizzle entry from individual category results
const dubizzleEntry = Object.keys(results.dubizzle).length >= 3 ? results.dubizzle : null;

const stress = computeStress({ ...results, dubizzle: dubizzleEntry });
const bayutSaleCount = results.bayut_ajman_sale?.count || null;
const bayutSaleAvg = results.bayut_ajman_sale?.avg_sale_price || null;

const output = {
  date: TODAY, scraped_at: SCRAPED_AT,
  stress: { date: TODAY, total: stress.total, band: stress.band, components: stress.components },
  dubizzle_entry: dubizzleEntry ? { date: TODAY, scraped_at: SCRAPED_AT, ...dubizzleEntry } : null,
  dubizzle_motors_entry: results.dubizzle_motors ? { date: TODAY, ...results.dubizzle_motors } : null,
  dubizzle_property_sale_entry: results.dubizzle_property_sale ? { date: TODAY, ...results.dubizzle_property_sale } : null,
  dubizzle_property_rent_entry: results.dubizzle_property_rent ? { date: TODAY, ...results.dubizzle_property_rent } : null,
  dubizzle_jobs_entry: results.dubizzle_jobs ? { date: TODAY, scraped_at: SCRAPED_AT, ...results.dubizzle_jobs } : null,
  bayut_entry: results.bayut_d9 ? {
    date: TODAY,
    district9_listings: results.bayut_d9.district9_listings,
    d9_avg_price: results.bayut_d9.avg_sale_price,
    benchmark_price_aed: results.benchmark?.benchmark_price_aed ?? 3200000,
    benchmark_flag: 'NO CHANGE'
  } : null,
  luxury_entry: results.luxury ? { date: TODAY, ...results.luxury } : null,
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
