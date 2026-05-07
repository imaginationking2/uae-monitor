import { Actor } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

await Actor.init();

const TODAY = new Date().toISOString().split('T')[0];
const SCRAPED_AT = new Date().toISOString();

const results = {
  date: TODAY, scraped_at: SCRAPED_AT,
  dubizzle: null, bayut_d9: null, bayut_ajman_sale: null,
  bayut_ajman_rent: null, benchmark: null, luxury: null,
  dubizzle_jobs: null, errors: []
};

const TARGETS = [
  { id: 'dubizzle',         url: 'https://uae.dubizzle.com/classified/' },
  { id: 'dubizzle_jobs',    url: 'https://uae.dubizzle.com/jobs/search/?q=' },
  { id: 'bayut_d9',         url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale', url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent', url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',        url: 'https://www.bayut.com/property/details-13073585.html' },
  { id: 'luxury',           url: 'https://www.luxurypricedrops.com/dubai/' }
];

// ── Parsers ───────────────────────────────────────────────────────────────────

function parseDubizzle(text) {
  if (!text || text.length < 100) return null;
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const out = {};
  for (let i = 0; i < lines.length - 1; i++) {
    const upper = lines[i].toUpperCase();
    const next = lines[i + 1];
    const num = parseInt((next || '').replace(/,/g, ''));
    if (/^[\d,]+$/.test(next) && num > 5000) {
      if (upper.includes('FURNITURE') && !out.furniture_home) out.furniture_home = num;
      if (upper.includes('HOME APPL') && !out.home_appliances) out.home_appliances = num;
      if (upper.includes('SPORTS') && !out.sports) out.sports = num;
      if (upper.includes('MOBILE') && !out.mobiles_tablets) out.mobiles_tablets = num;
      if (upper === 'ELECTRONICS' && !out.electronics) out.electronics = num;
      if (upper.includes('COMPUTERS') && !out.computers) out.computers = num;
    }
  }
  return Object.keys(out).length >= 3 ? out : null;
}

function parseDubizzleJobs(text) {
  if (!text || text.length < 100) return null;
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);

  // Total: "Jobs in UAE•1,310 Ads"
  const totalLine = lines.find(l => /Jobs in UAE[^\d]*(\d[\d,]+)\s*Ads/i.test(l));
  const totalMatch = totalLine?.match(/(\d[\d,]+)\s*Ads/i);
  const total = totalMatch ? parseInt(totalMatch[1].replace(/,/g, '')) : null;

  // Category breakdown — format: category name line followed by "(196)" count line
  const cats = {};
  const catMap = {
    'sales': 'sales_biz_dev',
    'accounting': 'accounting_finance',
    'real estate': 'real_estate',
    'engineering': 'engineering',
    'construction': 'construction',
    'driver': 'driver_delivery',
    'manufacturing': 'manufacturing',
    'hr': 'hr_admin',
    'hospitality': 'hospitality',
    'it': 'it_software'
  };
  for (let i = 1; i < lines.length; i++) {
    const m = lines[i].match(/^\((\d+)\)$/);
    if (m && lines[i-1]) {
      const cat = lines[i-1].toLowerCase();
      for (const [keyword, key] of Object.entries(catMap)) {
        if (cat.includes(keyword) && !cats[key]) {
          cats[key] = parseInt(m[1]);
        }
      }
    }
  }

  if (!total) return null;
  return { total_jobs: total, categories: cats };
}

function parseBayutCount(text) {
  if (!text) return null;
  const cl = text.split('\n').map(l => l.trim()).find(l => /\d+ to \d+ of [\d,]+ Propert/i.test(l));
  if (cl) return parseInt(cl.match(/of ([\d,]+)/)?.[1]?.replace(/,/g, ''));
  const m = text.match(/([\d,]+)\s+Propert/i);
  return m ? parseInt(m[1].replace(/,/g, '')) : null;
}

function parseBayutD9(text) {
  if (!text) return null;
  const count = parseBayutCount(text);
  const am = text.split('\n').map(l => l.trim()).find(l => /average sale price.*AED/i.test(l));
  const avg = am ? parseInt(am.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g, '')) : null;
  return count != null ? { district9_listings: count, avg_sale_price: avg } : null;
}

function parseBayutAjmanSale(text) {
  if (!text) return null;
  const count = parseBayutCount(text);
  const avgLine = text.split('\n').map(l => l.trim()).find(l => /average sale price.*AED/i.test(l));
  const avg = avgLine ? parseInt(avgLine.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g, '')) : null;
  return count != null ? { count, avg_sale_price: avg } : null;
}

function parseBenchmark(title) {
  const m = title.match(/AED ([\d.]+)M/i);
  const price = m ? Math.round(parseFloat(m[1]) * 1e6) : null;
  return price ? { benchmark_price_aed: price, benchmark_flag: 'NO CHANGE' } : null;
}

function parseLuxury(title, text) {
  const cm = title.match(/([\d,]+)\s+Propert/i);
  const dm = title.match(/Up to ([\d.]+)%/i);
  const count = cm ? parseInt(cm[1].replace(/,/g, '')) : null;
  const maxDrop = dm ? parseFloat(dm[1]) : null;
  if (!count) return null;
  const lines = (text || '').split('\n').map(l => l.trim()).filter(Boolean);
  const ol = lines.find(l => /off-market deals this week/i.test(l));
  const offMarket = ol ? parseInt(ol.match(/(\d+)/)?.[1]) : null;
  const al = lines.find(l => /avg.*below asking/i.test(l));
  const avgDrop = al?.match(/-?\s*(\d+)%/) ? parseFloat(al.match(/-?\s*(\d+)%/)[1]) : 6.5;
  return { drop_count: count, max_drop_pct: maxDrop, avg_drop_pct: avgDrop, off_market_deals: offMarket };
}

function computeStress(r) {
  const furniture = r.dubizzle?.furniture_home || 133228;
  const furniturePct = ((furniture - 133228) / 133228) * 100;
  const dubizzleScore = Math.min(25, Math.round(furniturePct / 0.4));
  const drops = r.luxury?.drop_count || 1542;
  const luxuryScore = Math.min(25, Math.round((drops - 1542) / 1542 * 100 / 2));
  const d9 = r.bayut_d9?.district9_listings ?? 31;
  const bayutScore = Math.min(25, Math.round((31 - d9) / 31 * 100 / 2));
  const sale = r.bayut_ajman_sale?.count || 0;
  const rent = r.bayut_ajman_rent || 1;
  const ratio = (sale && rent) ? parseFloat((sale / rent).toFixed(3)) : 0;
  const ratioScore = Math.min(25, Math.max(0, Math.round(ratio * 10 - 12)));
  const total = dubizzleScore + luxuryScore + bayutScore + ratioScore;
  const band = total < 30 ? 'Stable - no signal'
    : total < 45 ? 'Mild stress building'
    : total < 60 ? 'Clear stress building'
    : total < 75 ? 'High stress - monitor closely'
    : 'Crisis signal';
  return { total, band, components: { dubizzle: dubizzleScore, luxury: luxuryScore, bayut: bayutScore, ajman_ratio: ratioScore }, ratio };
}

// ── Proxy ─────────────────────────────────────────────────────────────────────
let proxyConfiguration;
try {
  proxyConfiguration = await Actor.createProxyConfiguration({ groups: ['RESIDENTIAL'], countryCode: 'AE' });
} catch (e) { console.log('Proxy failed:', e.message); }

// ── Crawler ───────────────────────────────────────────────────────────────────
const crawler = new PlaywrightCrawler({
  proxyConfiguration,
  maxRequestRetries: 3,
  navigationTimeoutSecs: 90,
  requestHandlerTimeoutSecs: 150,
  maxConcurrency: 1,
  launchContext: { launchOptions: { args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'] } },

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;
    log.info(`Scraping: ${id}`);

    try { await page.waitForLoadState('networkidle', { timeout: 30000 }); }
    catch { await page.waitForLoadState('domcontentloaded'); await sleep(3000); }

    const title = await page.title();
    const currentUrl = page.url();
    if (/captcha|robot|blocked|challenge/i.test(title) || currentUrl.includes('captchaChallenge'))
      throw new Error(`CAPTCHA on ${id}`);

    const bodyText = await page.evaluate(() => {
      const inner = document.body?.innerText || '';
      return inner.trim().length > 50 ? inner : document.documentElement?.outerHTML || '';
    });
    log.info(`${id}: ${bodyText.length} chars`);
    if (!bodyText || bodyText.length < 100) throw new Error(`Empty body on ${id}`);

    let parsed = null;

    switch (id) {
      case 'dubizzle': {
        parsed = parseDubizzle(bodyText);
        if (!parsed) {
          parsed = await page.evaluate(() => {
            const out = {};
            const lines = document.body.innerText.split('\n').map(l => l.trim()).filter(Boolean);
            for (let i = 0; i < lines.length - 1; i++) {
              const upper = lines[i].toUpperCase();
              const next = lines[i + 1];
              const num = parseInt((next || '').replace(/,/g, ''));
              if (/^[\d,]+$/.test(next) && num > 5000) {
                if (upper.includes('FURNITURE')) out.furniture_home = num;
                if (upper.includes('HOME APPL')) out.home_appliances = num;
                if (upper.includes('SPORTS')) out.sports = num;
                if (upper.includes('MOBILE')) out.mobiles_tablets = num;
                if (upper === 'ELECTRONICS') out.electronics = num;
                if (upper.includes('COMPUTERS')) out.computers = num;
              }
            }
            return Object.keys(out).length >= 3 ? out : null;
          });
        }
        if (!parsed) throw new Error('dubizzle: all strategies failed');
        results.dubizzle = parsed;
        break;
      }

      case 'dubizzle_jobs': {
        parsed = parseDubizzleJobs(bodyText);
        if (!parsed) {
          // Fallback: extract from page DOM directly
          parsed = await page.evaluate(() => {
            const lines = document.body.innerText.split('\n').map(l => l.trim()).filter(Boolean);
            const totalLine = lines.find(l => /Jobs in UAE[^\d]*[\d,]+\s*Ads/i.test(l));
            const totalMatch = totalLine?.match(/([\d,]+)\s*Ads/i);
            const total = totalMatch ? parseInt(totalMatch[1].replace(/,/g, '')) : null;
            if (!total) return null;
            const cats = {};
            for (let i = 1; i < lines.length; i++) {
              const m = lines[i].match(/^\((\d+)\)$/);
              if (m && lines[i-1]) {
                const cat = lines[i-1].toLowerCase();
                if (cat.includes('sales')) cats.sales_biz_dev = parseInt(m[1]);
                if (cat.includes('real estate')) cats.real_estate = parseInt(m[1]);
                if (cat.includes('engineering')) cats.engineering = parseInt(m[1]);
                if (cat.includes('accounting')) cats.accounting_finance = parseInt(m[1]);
                if (cat.includes('construction')) cats.construction = parseInt(m[1]);
                if (cat.includes('driver')) cats.driver_delivery = parseInt(m[1]);
              }
            }
            return { total_jobs: total, categories: cats };
          });
        }
        if (!parsed) { log.warning('dubizzle_jobs: parse failed'); throw new Error('dubizzle_jobs parse failed'); }
        results.dubizzle_jobs = parsed;
        break;
      }

      case 'bayut_d9':
        parsed = parseBayutD9(bodyText);
        if (!parsed) throw new Error('bayut_d9 parse failed');
        results.bayut_d9 = parsed;
        break;

      case 'bayut_ajman_sale':
        parsed = parseBayutAjmanSale(bodyText);
        if (!parsed) throw new Error('bayut_ajman_sale parse failed');
        results.bayut_ajman_sale = parsed;
        break;

      case 'bayut_ajman_rent':
        parsed = parseBayutCount(bodyText);
        if (!parsed) throw new Error('bayut_ajman_rent parse failed');
        results.bayut_ajman_rent = parsed;
        break;

      case 'benchmark':
        parsed = parseBenchmark(title);
        if (!parsed) throw new Error('benchmark parse failed');
        results.benchmark = parsed;
        break;

      case 'luxury':
        parsed = parseLuxury(title, bodyText);
        if (!parsed) throw new Error('luxury parse failed');
        results.luxury = parsed;
        break;
    }

    log.info(`OK ${id}: ${JSON.stringify(parsed).substring(0, 150)}`);
    await page.close();
  },

  failedRequestHandler({ request, log, error }) {
    log.error(`FAILED ${request.userData.id}: ${error.message}`);
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
  dubizzle_entry: results.dubizzle ? { date: TODAY, scraped_at: SCRAPED_AT, ...results.dubizzle } : null,
  dubizzle_jobs_entry: results.dubizzle_jobs ? { date: TODAY, scraped_at: SCRAPED_AT, ...results.dubizzle_jobs } : null,
  bayut_entry: results.bayut_d9 ? {
    date: TODAY, district9_listings: results.bayut_d9.district9_listings,
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
