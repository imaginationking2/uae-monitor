import { Actor } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

await Actor.init();

const TODAY = new Date().toISOString().split('T')[0];
const SCRAPED_AT = new Date().toISOString();

const results = {
  date: TODAY, scraped_at: SCRAPED_AT,
  jobs_fulltime: null, motors_usedcars: null,
  classified: null,
  property_sale: {}, property_rent: {},
  luxury: null,
  bayut_d9: null, bayut_ajman_sale: null, bayut_ajman_rent: null,
  benchmark: null,
  errors: []
};

const PROP_SALE_TARGETS = [
  { id: 'prop_sale_uae',       key: 'uae',       url: 'https://uae.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_sale_dubai',     key: 'dubai',     url: 'https://dubai.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_sale_abudhabi',  key: 'abu_dhabi', url: 'https://abudhabi.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_sale_sharjah',   key: 'sharjah',   url: 'https://sharjah.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_sale_ajman',     key: 'ajman',     url: 'https://ajman.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_sale_rak',       key: 'rak',       url: 'https://rak.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_sale_fujairah',  key: 'fujairah',  url: 'https://fujairah.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_sale_uaq',       key: 'uaq',       url: 'https://uaq.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_sale_alain',     key: 'alain',     url: 'https://alain.dubizzle.com/en/property-for-sale/residential/' }
];

const PROP_RENT_TARGETS = [
  { id: 'prop_rent_uae',       key: 'uae',       url: 'https://uae.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'prop_rent_dubai',     key: 'dubai',     url: 'https://dubai.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'prop_rent_abudhabi',  key: 'abu_dhabi', url: 'https://abudhabi.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'prop_rent_sharjah',   key: 'sharjah',   url: 'https://sharjah.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'prop_rent_ajman',     key: 'ajman',     url: 'https://ajman.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'prop_rent_rak',       key: 'rak',       url: 'https://rak.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'prop_rent_fujairah',  key: 'fujairah',  url: 'https://fujairah.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'prop_rent_uaq',       key: 'uaq',       url: 'https://uaq.dubizzle.com/en/property-for-rent/residential/' }
];

const CLASSIFIED_TARGETS = [
  { id: 'cls_furniture',    key: 'furniture_home',  url: 'https://uae.dubizzle.com/en/classified/furniture-home-garden/' },
  { id: 'cls_appliances',   key: 'home_appliances', url: 'https://uae.dubizzle.com/en/classified/home-appliances/' },
  { id: 'cls_sports',       key: 'sports',          url: 'https://uae.dubizzle.com/en/classified/sports-equipment/' },
  { id: 'cls_mobiles',      key: 'mobiles_tablets', url: 'https://uae.dubizzle.com/en/classified/mobile-phones/' },
  { id: 'cls_electronics',  key: 'electronics',     url: 'https://uae.dubizzle.com/en/classified/tv-audio-cameras/' },
  { id: 'cls_computers',    key: 'computers',       url: 'https://uae.dubizzle.com/en/classified/computers-networking/' }
];

// IMPORTANT: Run jobs+motors FIRST to warm up the proxy with simple homepage-like requests
const SINGLE_TARGETS = [
  { id: 'jobs',             url: 'https://uae.dubizzle.com/jobs/' },
  { id: 'motors',           url: 'https://uae.dubizzle.com/motors/' },
  { id: 'bayut_d9',         url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale', url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent', url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',        url: 'https://www.bayut.com/property/details-13073585.html' }
];

const LUXURY_TARGETS = [
  { id: 'luxury',           url: 'https://www.luxurypricedrops.com/dubai/' }
];

async function parseJobs(page, log) {
  try { await page.waitForSelector('a[href*="full-time"] p', { timeout: 12000 }); }
  catch { log.warning('jobs: waitForSelector timed out'); }
  return page.evaluate(() => {
    const ftLinks = Array.from(document.querySelectorAll('a[href*="full-time"]'));
    for (const link of ftLinks) {
      const p = link.querySelector('p');
      if (p && /Jobs/i.test(p.textContent)) {
        const m = p.textContent.match(/([\d,]+)/);
        if (m) return { full_time: parseInt(m[1].replace(/,/g, '')) };
      }
    }
    return null;
  });
}

async function parseMotors(page, log) {
  try { await page.waitForSelector('[data-testid="Used Cars"]', { timeout: 10000 }); }
  catch { log.warning('motors: waitForSelector timed out'); }
  return page.evaluate(() => {
    const el = document.querySelector('[data-testid="Used Cars"]');
    if (el && el.nextElementSibling) {
      const m = el.nextElementSibling.textContent.match(/([\d,]+)/);
      if (m) return { used_cars: parseInt(m[1].replace(/,/g, '')) };
    }
    return null;
  });
}

async function parseListingCount(page) {
  try { await page.waitForSelector('.mui-style-1cryx81, [data-testid="page-title"] h1, h1', { timeout: 15000 }); }
  catch {}
  return page.evaluate(() => {
    const span = document.querySelector('.mui-style-1cryx81');
    if (span) {
      const m = span.textContent.match(/([\d,]+)/);
      if (m) return parseInt(m[1].replace(/,/g, ''));
    }
    const h1 = document.querySelector('[data-testid="page-title"] h1') || document.querySelector('h1');
    if (h1) {
      const m = h1.textContent.match(/([\d,]+)\s*Ads/i);
      if (m) return parseInt(m[1].replace(/,/g, ''));
    }
    return null;
  });
}

async function parseLuxury(page) {
  return page.evaluate(() => {
    const countEl = document.querySelector('[data-meta="count"]');
    const drop_count = countEl ? parseInt(countEl.textContent.replace(/,/g, '')) : null;
    if (!drop_count) return null;
    const mstatEls = Array.from(document.querySelectorAll('[class*="mstat-value"]'));
    let avg_drop_pct = null;
    for (const el of mstatEls) {
      const m = el.textContent.match(/[\d.]+/);
      if (m) { avg_drop_pct = parseFloat(m[0]); break; }
    }
    const pctEls = Array.from(document.querySelectorAll('*')).filter(e =>
      e.children.length === 0 && /^[\u2212\-][\d.]+%$/.test(e.textContent.trim())
    );
    const pcts = pctEls.map(e => parseFloat(e.textContent.replace(/[\u2212\-]/, '')));
    const max_drop_pct = pcts.length ? Math.max(...pcts) : null;
    return { drop_count, avg_drop_pct, max_drop_pct };
  });
}

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
  return m ? { benchmark_price_aed: Math.round(parseFloat(m[1]) * 1e6) } : null;
}

function computeStress(r) {
  const usedCars = r.motors_usedcars?.used_cars || 38770;
  const dubizzleScore = Math.min(25, Math.max(0, Math.round(((usedCars - 38770) / 38770) * 100 / 0.4)));
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

let proxyUAE;
try {
  proxyUAE = await Actor.createProxyConfiguration({ groups: ['RESIDENTIAL'], countryCode: 'AE' });
} catch (e) { console.log('UAE proxy failed: ' + e.message); }

let proxyUS;
try {
  proxyUS = await Actor.createProxyConfiguration({ groups: ['RESIDENTIAL'], countryCode: 'US' });
} catch (e) { console.log('US proxy failed: ' + e.message); }

async function handleRequest({ request, page, log }) {
  const { id, group, key } = request.userData;
  log.info('Scraping: ' + id);

  // Anti-bot: realistic browser headers
  await page.setExtraHTTPHeaders({
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
    'Cache-Control': 'no-cache',
    'Sec-Ch-Ua': '"Chromium";v="124", "Not(A:Brand";v="24", "Google Chrome";v="124"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1'
  });

  try { await page.waitForLoadState('networkidle', { timeout: 25000 }); }
  catch { await page.waitForLoadState('domcontentloaded'); await sleep(3000); }

  const title = await page.title();
  const bodyLen = await page.evaluate(() => document.body?.innerText?.length || 0);
  log.info(id + ': title="' + title.substring(0, 60) + '" bodyLen=' + bodyLen);

  if (bodyLen < 200) throw new Error('Empty body on ' + id);

  let parsed = null;

  if (group === 'prop_sale') {
    parsed = await parseListingCount(page);
    if (!parsed) throw new Error(id + ' parse failed');
    results.property_sale[key] = parsed;
    log.info('OK ' + id + ': ' + key + '=' + parsed);
    await sleep(2000); // Polite delay between Dubizzle requests
    await page.close();
    return;
  }
  if (group === 'prop_rent') {
    parsed = await parseListingCount(page);
    if (!parsed) throw new Error(id + ' parse failed');
    results.property_rent[key] = parsed;
    log.info('OK ' + id + ': ' + key + '=' + parsed);
    await sleep(2000);
    await page.close();
    return;
  }
  if (group === 'classified') {
    parsed = await parseListingCount(page);
    if (!parsed) throw new Error(id + ' parse failed');
    if (!results.classified) results.classified = {};
    results.classified[key] = parsed;
    log.info('OK ' + id + ': ' + key + '=' + parsed);
    await sleep(2000);
    await page.close();
    return;
  }

  switch (id) {
    case 'jobs':
      parsed = await parseJobs(page, log);
      if (!parsed) throw new Error('jobs parse failed - fullTime=null');
      results.jobs_fulltime = parsed;
      break;
    case 'motors':
      parsed = await parseMotors(page, log);
      if (!parsed) throw new Error('motors parse failed');
      results.motors_usedcars = parsed;
      break;
    case 'luxury':
      parsed = await parseLuxury(page);
      if (!parsed) throw new Error('luxury parse failed');
      results.luxury = parsed;
      break;
    case 'bayut_d9': {
      const text = await page.evaluate(() => document.body?.innerText || '');
      parsed = parseBayutD9(text);
      if (!parsed) throw new Error('bayut_d9 parse failed');
      results.bayut_d9 = parsed;
      break;
    }
    case 'bayut_ajman_sale': {
      const text = await page.evaluate(() => document.body?.innerText || '');
      parsed = parseBayutAjmanSale(text);
      if (!parsed) throw new Error('bayut_ajman_sale parse failed');
      results.bayut_ajman_sale = parsed;
      break;
    }
    case 'bayut_ajman_rent': {
      const text = await page.evaluate(() => document.body?.innerText || '');
      parsed = parseBayutCount(text);
      if (!parsed) throw new Error('bayut_ajman_rent parse failed');
      results.bayut_ajman_rent = parsed;
      break;
    }
    case 'benchmark':
      parsed = parseBenchmark(title);
      if (!parsed) throw new Error('benchmark parse failed');
      results.benchmark = parsed;
      break;
  }
  log.info('OK ' + id + ': ' + JSON.stringify(parsed).substring(0, 120));
  await sleep(1500);
  await page.close();
}

function failedHandler({ request, error }) {
  console.error('FAILED ' + request.userData.id + ': ' + error.message);
  results.errors.push({ source: request.userData.id, error: error.message });
}

// CRITICAL: useSessionPool keeps same proxy IP per session, persistCookiesPerSession reuses cookies
// This makes us look like a real user instead of fresh anonymous request each time
const uaeCrawler = new PlaywrightCrawler({
  proxyConfiguration: proxyUAE,
  useSessionPool: true,
  persistCookiesPerSession: true,
  sessionPoolOptions: { maxPoolSize: 1 },
  maxRequestRetries: 2,
  navigationTimeoutSecs: 90,
  requestHandlerTimeoutSecs: 120,
  maxConcurrency: 1,
  launchContext: {
    launchOptions: {
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu','--disable-blink-features=AutomationControlled']
    },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
  },
  requestHandler: handleRequest,
  failedRequestHandler: failedHandler
});

const usCrawler = new PlaywrightCrawler({
  proxyConfiguration: proxyUS,
  maxRequestRetries: 2,
  navigationTimeoutSecs: 60,
  requestHandlerTimeoutSecs: 90,
  maxConcurrency: 1,
  launchContext: {
    launchOptions: { args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu'] },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
  },
  requestHandler: handleRequest,
  failedRequestHandler: failedHandler
});

// Order: Bayut FIRST (warm up), THEN jobs/motors (warm Dubizzle session), THEN classified, THEN per-emirate
const uaeTargets = [
  ...SINGLE_TARGETS.map(t => ({ url: t.url, userData: { id: t.id } })),
  ...CLASSIFIED_TARGETS.map(t => ({ url: t.url, userData: { id: t.id, group: 'classified', key: t.key } })),
  ...PROP_SALE_TARGETS.map(t => ({ url: t.url, userData: { id: t.id, group: 'prop_sale', key: t.key } })),
  ...PROP_RENT_TARGETS.map(t => ({ url: t.url, userData: { id: t.id, group: 'prop_rent', key: t.key } }))
];

await uaeCrawler.run(uaeTargets);
await usCrawler.run(LUXURY_TARGETS.map(t => ({ url: t.url, userData: { id: t.id } })));

const stress = computeStress(results);
const bayutSaleCount = results.bayut_ajman_sale?.count || null;
const bayutSaleAvg = results.bayut_ajman_sale?.avg_sale_price || null;

const propertySaleEntry = Object.keys(results.property_sale).length > 0 ? { date: TODAY, ...results.property_sale } : null;
const propertyRentEntry = Object.keys(results.property_rent).length > 0 ? { date: TODAY, ...results.property_rent } : null;
const classifiedEntry = results.classified && Object.keys(results.classified).length > 0
  ? { date: TODAY, scraped_at: SCRAPED_AT, ...results.classified } : null;

const output = {
  date: TODAY, scraped_at: SCRAPED_AT,
  stress: { date: TODAY, total: stress.total, band: stress.band, components: stress.components },
  dubizzle_classified_entry:    classifiedEntry,
  dubizzle_jobs_entry:          results.jobs_fulltime   ? { date: TODAY, ...results.jobs_fulltime }   : null,
  dubizzle_motors_entry:        results.motors_usedcars ? { date: TODAY, ...results.motors_usedcars } : null,
  dubizzle_property_sale_entry: propertySaleEntry,
  dubizzle_property_rent_entry: propertyRentEntry,
  luxury_entry:                 results.luxury          ? { date: TODAY, ...results.luxury }          : null,
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
