import { Actor } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

await Actor.init();

const TODAY = new Date().toISOString().split('T')[0];
const SCRAPED_AT = new Date().toISOString();

const results = {
  date: TODAY, scraped_at: SCRAPED_AT,
  dubizzle: null,
  dubizzle_motors: null,
  dubizzle_property_sale: null,
  dubizzle_property_rent: null,
  dubizzle_jobs: null,
  bayut_d9: null,
  bayut_ajman_sale: null,
  bayut_ajman_rent: null,
  benchmark: null,
  luxury: null,
  errors: []
};

const TARGETS = [
  { id: 'dubizzle',               url: 'https://uae.dubizzle.com/classified/' },
  { id: 'dubizzle_motors',        url: 'https://uae.dubizzle.com/motors/' },
  { id: 'dubizzle_property_sale', url: 'https://uae.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'dubizzle_property_rent', url: 'https://uae.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'dubizzle_jobs',          url: 'https://uae.dubizzle.com/jobs/search/?q=' },
  { id: 'bayut_d9',               url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale',       url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent',       url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',              url: 'https://www.bayut.com/property/details-13073585.html' },
  { id: 'luxury',                 url: 'https://www.luxurypricedrops.com/dubai/' }
];

// ── Parsers ───────────────────────────────────────────────────────────────────

function parseCategoryPage(text) {
  // Generic parser for pages with CATEGORY NAME\nCOUNT pattern
  if (!text || text.length < 100) return null;
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const out = {};
  for (let i = 0; i < lines.length - 1; i++) {
    const next = lines[i + 1];
    if (/^[\d,]+$/.test(next)) {
      const num = parseInt(next.replace(/,/g, ''));
      if (num > 100) out[lines[i]] = num;
    }
  }
  return Object.keys(out).length > 0 ? out : null;
}

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

function parseDubizzleMotors(text) {
  if (!text || text.length < 100) return null;
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const out = {};
  for (let i = 0; i < lines.length - 1; i++) {
    const upper = lines[i].toUpperCase();
    const next = lines[i + 1];
    const num = parseInt((next || '').replace(/,/g, ''));
    if (/^[\d,]+$/.test(next) && num > 50) {
      if (upper === 'USED CARS') out.used_cars = num;
      if (upper === 'NUMBER PLATES') out.number_plates = num;
      if (upper === 'RENTAL CARS') out.rental_cars = num;
      if (upper === 'MOTORCYCLES') out.motorcycles = num;
      if (upper.includes('AUTO ACCESSORIES')) out.auto_accessories = num;
      if (upper === 'HEAVY VEHICLES') out.heavy_vehicles = num;
      if (upper === 'BOATS') out.boats = num;
    }
  }
  return Object.keys(out).length >= 3 ? out : null;
}

function parseDubizzlePropertySale(text, title) {
  if (!text || text.length < 100) return null;
  // Total from title: "Properties for Sale in UAE - 245,565 Properties for Sale"
  const titleM = title.match(/([\d,]+)\s+Properties for Sale/i);
  const total = titleM ? parseInt(titleM[1].replace(/,/g, '')) : null;
  return total ? { total_uae: total } : null;
}

function parseDubizzlePropertyRent(text) {
  if (!text || text.length < 100) return null;
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const emirates = {
    'Dubai': 'dubai',
    'Ajman': 'ajman',
    'Sharjah': 'sharjah',
    'Abu Dhabi': 'abu_dhabi',
    'Ras Al Khaimah': 'rak',
    'Umm Al Quwain': 'uaq',
    'Fujairah': 'fujairah'
  };
  const out = {};
  lines.forEach((l, i) => {
    Object.entries(emirates).forEach(([name, key]) => {
      if (new RegExp(name, 'i').test(l) && l.length < 30 && !out[key]) {
        const ctx = lines.slice(i, i + 5).join(' ');
        const m = ctx.match(/([\d,]{4,})/);
        if (m) out[key] = parseInt(m[1].replace(/,/g, ''));
      }
    });
  });
  return Object.keys(out).length >= 3 ? out : null;
}

function parseDubizzleJobs(text) {
  if (!text || text.length < 100) return null;
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const totalLine = lines.find(l => /Jobs in UAE.*\d[\d,]+.*Ads/i.test(l));
  const totalMatch = totalLine?.match(/([\d,]+)\s*Ads/i);
  const total = totalMatch ? parseInt(totalMatch[1].replace(/,/g, '')) : null;
  if (!total) return null;
  const cats = {};
  const catMap = {
    'Sales / Business Development': 'sales_business_dev',
    'Accounting / Finance': 'accounting_finance',
    'Driver / Delivery': 'driver_delivery',
    'Real Estate': 'real_estate',
    'Engineering': 'engineering',
    'Construction': 'construction',
    'Manufacturing / Warehouse': 'manufacturing_warehouse',
    'HR / Admin': 'hr_admin',
    'IT / Software Development': 'it_software',
    'Handyman / Technician': 'handyman_technician',
    'Secretarial / Front Office': 'secretarial'
  };
  for (let i = 0; i < lines.length - 1; i++) {
    const key = catMap[lines[i]];
    const m = lines[i + 1]?.match(/^\((\d+)\)$/);
    if (key && m) cats[key] = parseInt(m[1]);
  }
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
  const band = total < 30 ? 'Stable – no signal'
    : total < 45 ? 'Mild stress building'
    : total < 60 ? 'Clear stress building'
    : total < 75 ? 'High stress – monitor closely'
    : 'Crisis signal';
  return { total, band, components: { dubizzle: dubizzleScore, luxury: luxuryScore, bayut: bayutScore, ajman_ratio: ratioScore }, ratio };
}

// ── Proxy ─────────────────────────────────────────────────────────────────────
let proxyConfiguration;
try {
  proxyConfiguration = await Actor.createProxyConfiguration({
    groups: ['RESIDENTIAL'], countryCode: 'AE'
  });
} catch (e) { console.log('Proxy config failed:', e.message); }

// ── Crawler ───────────────────────────────────────────────────────────────────
const crawler = new PlaywrightCrawler({
  proxyConfiguration,
  maxRequestRetries: 3,
  navigationTimeoutSecs: 90,
  requestHandlerTimeoutSecs: 150,
  maxConcurrency: 1,
  launchContext: {
    launchOptions: {
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu']
    }
  },

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;
    log.info(`Scraping: ${id}`);
    try {
      await page.waitForLoadState('networkidle', { timeout: 30000 });
    } catch {
      await page.waitForLoadState('domcontentloaded');
      await sleep(3000);
    }
    const title = await page.title();
    const currentUrl = page.url();
    if (/captcha|robot|blocked|challenge/i.test(title) || currentUrl.includes('captchaChallenge')) {
      throw new Error(`CAPTCHA on ${id}`);
    }
    const bodyText = await page.evaluate(() => {
      const inner = document.body?.innerText || '';
      return inner.trim().length > 50 ? inner : document.documentElement?.outerHTML || '';
    });
    log.info(`${id}: ${bodyText.length} chars, title="${title}"`);
    if (!bodyText || bodyText.length < 100) throw new Error(`Empty body on ${id}`);

    let parsed = null;
    switch (id) {
      case 'dubizzle': {
        parsed = parseDubizzle(bodyText);
        if (!parsed) {
          parsed = await page.evaluate(() => {
            const out = {};
            const lines = document.body.innerText.split('\n').map(l=>l.trim()).filter(Boolean);
            for (let i=0;i<lines.length-1;i++) {
              const upper = lines[i].toUpperCase();
              const num = parseInt((lines[i+1]||'').replace(/,/g,''));
              if (/^[\d,]+$/.test(lines[i+1]) && num > 5000) {
                if (upper.includes('FURNITURE')) out.furniture_home=num;
                if (upper.includes('HOME APPL')) out.home_appliances=num;
                if (upper.includes('SPORTS')) out.sports=num;
                if (upper.includes('MOBILE')) out.mobiles_tablets=num;
                if (upper==='ELECTRONICS') out.electronics=num;
                if (upper.includes('COMPUTERS')) out.computers=num;
              }
            }
            return Object.keys(out).length>=3?out:null;
          });
        }
        if (!parsed) throw new Error('dubizzle parse failed');
        results.dubizzle = parsed;
        break;
      }
      case 'dubizzle_motors': {
        parsed = parseDubizzleMotors(bodyText);
        if (!parsed) throw new Error('dubizzle_motors parse failed');
        results.dubizzle_motors = parsed;
        break;
      }
      case 'dubizzle_property_sale': {
        parsed = parseDubizzlePropertySale(bodyText, title);
        if (!parsed) throw new Error('dubizzle_property_sale parse failed');
        results.dubizzle_property_sale = parsed;
        break;
      }
      case 'dubizzle_property_rent': {
        parsed = parseDubizzlePropertyRent(bodyText);
        if (!parsed) throw new Error('dubizzle_property_rent parse failed');
        results.dubizzle_property_rent = parsed;
        break;
      }
      case 'dubizzle_jobs': {
        parsed = parseDubizzleJobs(bodyText);
        if (!parsed) throw new Error('dubizzle_jobs parse failed');
        results.dubizzle_jobs = parsed;
        break;
      }
      case 'bayut_d9': {
        parsed = parseBayutD9(bodyText);
        if (!parsed) throw new Error('bayut_d9 parse failed');
        results.bayut_d9 = parsed;
        break;
      }
      case 'bayut_ajman_sale': {
        parsed = parseBayutAjmanSale(bodyText);
        if (!parsed) throw new Error('bayut_ajman_sale parse failed');
        results.bayut_ajman_sale = parsed;
        break;
      }
      case 'bayut_ajman_rent': {
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
      case 'luxury': {
        parsed = parseLuxury(title, bodyText);
        if (!parsed) throw new Error('luxury parse failed');
        results.luxury = parsed;
        break;
      }
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
  dubizzle_entry: results.dubizzle
    ? { date: TODAY, scraped_at: SCRAPED_AT, ...results.dubizzle } : null,
  dubizzle_motors_entry: results.dubizzle_motors
    ? { date: TODAY, ...results.dubizzle_motors } : null,
  dubizzle_property_sale_entry: results.dubizzle_property_sale
    ? { date: TODAY, ...results.dubizzle_property_sale } : null,
  dubizzle_property_rent_entry: results.dubizzle_property_rent
    ? { date: TODAY, ...results.dubizzle_property_rent } : null,
  dubizzle_jobs_entry: results.dubizzle_jobs
    ? { date: TODAY, scraped_at: SCRAPED_AT, ...results.dubizzle_jobs } : null,
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
