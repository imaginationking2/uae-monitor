import { Actor } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

await Actor.init();

const TODAY = new Date().toISOString().split('T')[0];
const SCRAPED_AT = new Date().toISOString();

const results = {
  date: TODAY, scraped_at: SCRAPED_AT,
  motors_usedcars: null,
  classified: null,
  property_sale: null,
  property_rent: null,
  luxury: null,
  bayut_d9: null,
  bayut_ajman_sale: null,
  bayut_ajman_rent: null,
  benchmark: null,
  errors: []
};

// 9 URLs total — minimal volume, all confirmed selectors
const SINGLE_TARGETS = [
  { id: 'classified',       url: 'https://uae.dubizzle.com/classified/' },
  { id: 'motors',           url: 'https://uae.dubizzle.com/motors/' },
  { id: 'prop_sale',        url: 'https://uae.dubizzle.com/en/property-for-sale/residential/' },
  { id: 'prop_rent',        url: 'https://uae.dubizzle.com/en/property-for-rent/residential/' },
  { id: 'bayut_d9',         url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale', url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent', url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',        url: 'https://www.bayut.com/property/details-13073585.html' }
];

const LUXURY_TARGETS = [
  { id: 'luxury',           url: 'https://www.luxurypricedrops.com/dubai/' }
];

// Map of data-testid names on the /classified/ homepage to our internal keys
const CLASSIFIED_TESTIDS = {
  'Furniture, Home & Garden': 'furniture_home',
  'Home Appliances':          'home_appliances',
  'Sports Equipment':         'sports',
  'Mobile Phones & Tablets':  'mobiles_tablets',
  'Electronics':              'electronics',
  'Computers & Networking':   'computers'
};

// Parse the /classified/ homepage — ALL 6 category counts in ONE page
async function parseClassified(page, log) {
  try { await page.waitForSelector('[data-testid="subcategories"]', { timeout: 15000 }); }
  catch { log.warning('classified: waitForSelector timed out'); }
  return page.evaluate((testidMap) => {
    const section = document.querySelector('[data-testid="subcategories"]');
    if (!section) return null;
    const result = {};
    const cards = section.querySelectorAll('a > div > div');
    cards.forEach(card => {
      const nameEl = card.querySelector('[data-testid]');
      if (!nameEl) return;
      const name = nameEl.getAttribute('data-testid');
      const countText = card.children[1]?.textContent?.trim();
      const key = testidMap[name];
      if (key && countText) {
        const m = countText.match(/([\d,]+)/);
        if (m) result[key] = parseInt(m[1].replace(/,/g, ''));
      }
    });
    return Object.keys(result).length > 0 ? result : null;
  }, CLASSIFIED_TESTIDS);
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
  const { id } = request.userData;
  log.info('Scraping: ' + id);

  try { await page.waitForLoadState('networkidle', { timeout: 25000 }); }
  catch { await page.waitForLoadState('domcontentloaded'); await sleep(3000); }

  const title = await page.title();
  const bodyLen = await page.evaluate(() => document.body?.innerText?.length || 0);
  log.info(id + ': title="' + title.substring(0, 60) + '" bodyLen=' + bodyLen);

  if (bodyLen < 200) throw new Error('Empty body on ' + id);

  let parsed = null;

  switch (id) {
    case 'classified':
      parsed = await parseClassified(page, log);
      if (!parsed) throw new Error('classified parse failed');
      results.classified = parsed;
      break;
    case 'motors':
      parsed = await parseMotors(page, log);
      if (!parsed) throw new Error('motors parse failed');
      results.motors_usedcars = parsed;
      break;
    case 'prop_sale':
      parsed = await parseListingCount(page);
      if (!parsed) throw new Error('prop_sale parse failed');
      results.property_sale = { total_uae: parsed };
      break;
    case 'prop_rent':
      parsed = await parseListingCount(page);
      if (!parsed) throw new Error('prop_rent parse failed');
      results.property_rent = { total_uae: parsed };
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
  log.info('OK ' + id + ': ' + JSON.stringify(parsed).substring(0, 150));
  await page.close();
}

function failedHandler({ request, error }) {
  console.error('FAILED ' + request.userData.id + ': ' + error.message);
  results.errors.push({ source: request.userData.id, error: error.message });
}

const uaeCrawler = new PlaywrightCrawler({
  proxyConfiguration: proxyUAE,
  maxRequestRetries: 2,
  navigationTimeoutSecs: 60,
  requestHandlerTimeoutSecs: 90,
  maxConcurrency: 1,
  launchContext: { launchOptions: { args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu'] } },
  requestHandler: handleRequest,
  failedRequestHandler: failedHandler
});

const usCrawler = new PlaywrightCrawler({
  proxyConfiguration: proxyUS,
  maxRequestRetries: 2,
  navigationTimeoutSecs: 60,
  requestHandlerTimeoutSecs: 90,
  maxConcurrency: 1,
  launchContext: { launchOptions: { args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu'] } },
  requestHandler: handleRequest,
  failedRequestHandler: failedHandler
});

await uaeCrawler.run(SINGLE_TARGETS.map(t => ({ url: t.url, userData: { id: t.id } })));
await usCrawler.run(LUXURY_TARGETS.map(t => ({ url: t.url, userData: { id: t.id } })));

const stress = computeStress(results);
const bayutSaleCount = results.bayut_ajman_sale?.count || null;
const bayutSaleAvg = results.bayut_ajman_sale?.avg_sale_price || null;

const propertySaleEntry = results.property_sale ? { date: TODAY, ...results.property_sale } : null;
const propertyRentEntry = results.property_rent ? { date: TODAY, ...results.property_rent } : null;
const classifiedEntry = results.classified ? { date: TODAY, scraped_at: SCRAPED_AT, ...results.classified } : null;

const output = {
  date: TODAY, scraped_at: SCRAPED_AT,
  stress: { date: TODAY, total: stress.total, band: stress.band, components: stress.components },
  dubizzle_classified_entry:    classifiedEntry,
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
