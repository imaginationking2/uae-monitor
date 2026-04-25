import { Actor } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

await Actor.init();

const TODAY = new Date().toISOString().split('T')[0];
const SCRAPED_AT = new Date().toISOString();

const results = {
  date: TODAY, scraped_at: SCRAPED_AT,
  dubizzle: null, bayut_d9: null, bayut_ajman_sale: null,
  bayut_ajman_rent: null, benchmark: null, luxury: null, errors: []
};

const TARGETS = [
  { id: 'dubizzle',         url: 'https://uae.dubizzle.com/classified/' },
  { id: 'bayut_d9',         url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale', url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent', url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',        url: 'https://www.bayut.com/property/details-13073585.html' },
  { id: 'luxury',           url: 'https://www.luxurypricedrops.com/dubai/' }
];

// ── Parsers ─────────────────────────────────────────────────────────────────

function parseDubizzle(bodyText) {
  const lines = bodyText.split('\n').map(l => l.trim()).filter(Boolean);
  
  // Strategy 1: exact category name followed by count on next line
  const catMap = {
    'FURNITURE, HOME & GARDEN': 'furniture_home',
    'HOME APPLIANCES': 'home_appliances',
    'SPORTS EQUIPMENT': 'sports',
    'MOBILE PHONES & TABLETS': 'mobiles_tablets',
    'ELECTRONICS': 'electronics',
    'COMPUTERS & NETWORKING': 'computers'
  };
  const out = {};
  for (let i = 0; i < lines.length - 1; i++) {
    const key = catMap[lines[i].toUpperCase()];
    if (key && /^[\d,]+$/.test(lines[i + 1])) {
      out[key] = parseInt(lines[i + 1].replace(/,/g, ''));
    }
  }
  if (Object.keys(out).length >= 4) return out;

  // Strategy 2: look for numbers near category keywords anywhere in text
  const text = bodyText.toUpperCase();
  const patterns = [
    { key: 'furniture_home',  re: /FURNITURE[^0-9]{0,50}([\d,]{3,})/s },
    { key: 'home_appliances', re: /HOME APPLIAN[^0-9]{0,50}([\d,]{3,})/s },
    { key: 'sports',          re: /SPORTS EQUIP[^0-9]{0,50}([\d,]{3,})/s },
    { key: 'mobiles_tablets', re: /MOBILE PHONE[^0-9]{0,50}([\d,]{3,})/s },
    { key: 'electronics',     re: /ELECTRONICS[^0-9]{0,50}([\d,]{3,})/s },
    { key: 'computers',       re: /COMPUTERS[^0-9]{0,50}([\d,]{3,})/s }
  ];
  const out2 = {};
  for (const { key, re } of patterns) {
    const m = text.match(re);
    if (m) out2[key] = parseInt(m[1].replace(/,/g, ''));
  }
  if (Object.keys(out2).length >= 4) return out2;

  // Strategy 3: scrape via page DOM directly (passed as JSON from page.evaluate)
  return null;
}

function parseBayutCount(bodyText) {
  if (!bodyText) return null;
  const lines = bodyText.split('\n').map(l => l.trim()).filter(Boolean);
  const cl = lines.find(l => /\d+ to \d+ of [\d,]+ Propert/i.test(l));
  if (cl) return parseInt(cl.match(/of ([\d,]+)/)?.[1]?.replace(/,/g, ''));
  // fallback: find any "X Properties" pattern
  const m = bodyText.match(/(\d[\d,]+)\s+Propert/i);
  return m ? parseInt(m[1].replace(/,/g, '')) : null;
}

function parseBayutD9(bodyText) {
  if (!bodyText) return null;
  const count = parseBayutCount(bodyText);
  const lines = bodyText.split('\n').map(l => l.trim()).filter(Boolean);
  const am = lines.find(l => /average sale price.*AED/i.test(l));
  const avg = am ? parseInt(am.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g, '')) : null;
  return count != null ? { district9_listings: count, avg_sale_price: avg } : null;
}

function parseBenchmark(pageTitle) {
  const m = pageTitle.match(/AED ([\d.]+)M/i);
  const price = m ? Math.round(parseFloat(m[1]) * 1e6) : null;
  return price ? { benchmark_price_aed: price, benchmark_flag: 'NO CHANGE' } : null;
}

function parseLuxury(pageTitle, bodyText) {
  const cm = pageTitle.match(/([\d,]+)\s+Propert/i);
  const dm = pageTitle.match(/Up to ([\d.]+)%/i);
  const count = cm ? parseInt(cm[1].replace(/,/g, '')) : null;
  const maxDrop = dm ? parseFloat(dm[1]) : null;
  if (!count) return null;
  const lines = (bodyText || '').split('\n').map(l => l.trim()).filter(Boolean);
  const ol = lines.find(l => /off-market deals this week/i.test(l));
  const offMarket = ol ? parseInt(ol.match(/(\d+)/)?.[1]) : null;
  const al = lines.find(l => /avg.*below asking/i.test(l));
  const avgDropM = al?.match(/-?\s*(\d+)%/);
  const avgDrop = avgDropM ? parseFloat(avgDropM[1]) : 6.5;
  return { drop_count: count, max_drop_pct: maxDrop, avg_drop_pct: avgDrop, off_market_deals: offMarket };
}

function computeStress(r) {
  const furniturePct = r.dubizzle?.furniture_home
    ? ((r.dubizzle.furniture_home - 133228) / 133228) * 100 : 0;
  const dubizzleScore = Math.min(25, Math.round(furniturePct / 0.4));
  const luxuryScore = r.luxury?.drop_count != null
    ? Math.min(25, Math.round((r.luxury.drop_count - 1542) / 1542 * 100 / 2)) : 0;
  const d9 = r.bayut_d9?.district9_listings ?? 31;
  const bayutScore = Math.min(25, Math.round((31 - d9) / 31 * 100 / 2));
  const sale = r.bayut_ajman_sale, rent = r.bayut_ajman_rent;
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

// ── Proxy config ─────────────────────────────────────────────────────────────
// Try residential AE proxy; Dubizzle works without proxy (UAE CDN serves globally)
let proxyConfiguration;
try {
  proxyConfiguration = await Actor.createProxyConfiguration({
    groups: ['RESIDENTIAL'],
    countryCode: 'AE'
  });
} catch (e) {
  console.log('Proxy config failed, running without proxy:', e.message);
  proxyConfiguration = undefined;
}

// ── Crawler ───────────────────────────────────────────────────────────────────
const crawler = new PlaywrightCrawler({
  proxyConfiguration,
  // Dubizzle doesn't need AE proxy — use no-proxy fallback if AE fails
  proxyRotationProtocol: 'PER_REQUEST',
  maxRequestRetries: 3,
  navigationTimeoutSecs: 90,
  requestHandlerTimeoutSecs: 120,
  // Keep concurrency at 1 to avoid memory overload on 2GB container
  maxConcurrency: 1,
  // Use less memory per page by reusing browser context
  launchContext: {
    launchOptions: {
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',  // fixes shared memory crashes
        '--disable-gpu',
        '--js-flags=--max-old-space-size=512'  // limit V8 heap
      ]
    }
  },

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;
    log.info(`Scraping: ${id} — ${request.url}`);

    // Wait for full network idle to ensure content loaded
    try {
      await page.waitForLoadState('networkidle', { timeout: 30000 });
    } catch {
      // If networkidle times out, fall back to domcontentloaded + extra wait
      await page.waitForLoadState('domcontentloaded');
      await sleep(3000);
    }

    // Extra wait + human-like delay
    await sleep(1500 + Math.random() * 1500);

    // Check for CAPTCHA / bot wall
    const title = await page.title();
    const url = page.url();
    if (/captcha|robot|blocked|challenge/i.test(title) || url.includes('captchaChallenge')) {
      throw new Error(`CAPTCHA detected on ${id}: ${title}`);
    }

    // Get body text safely
    const bodyText = await page.evaluate(() => {
      return document.body ? document.body.innerText : null;
    });

    if (!bodyText) throw new Error(`Empty body on ${id}`);

    let parsed = null;

    switch (id) {
      case 'dubizzle': {
        // First try text parsing
        parsed = parseDubizzle(bodyText);
        if (!parsed) {
          // Fallback: extract counts via DOM selectors directly in page
          parsed = await page.evaluate(() => {
            const result = {};
            const catMap = {
              'furniture': 'furniture_home',
              'home appliances': 'home_appliances',
              'sports': 'sports',
              'mobile': 'mobiles_tablets',
              'electronics': 'electronics',
              'computers': 'computers'
            };
            // Try finding elements with category names and nearby numbers
            document.querySelectorAll('a, h3, h4, div, span').forEach(el => {
              const text = el.textContent?.trim().toLowerCase() || '';
              for (const [keyword, key] of Object.entries(catMap)) {
                if (text.startsWith(keyword) && text.length < 60) {
                  // Look for a sibling or nearby element with a number
                  const parent = el.parentElement;
                  if (parent) {
                    const nums = parent.textContent.match(/[\d,]{4,}/g);
                    if (nums && !result[key]) {
                      result[key] = parseInt(nums[0].replace(/,/g, ''));
                    }
                  }
                }
              }
            });
            // Also try finding category counts from page text patterns
            const allText = document.body.innerText;
            const lines = allText.split('\n').filter(l => l.trim());
            for (let i = 0; i < lines.length - 1; i++) {
              const upper = lines[i].trim().toUpperCase();
              const next = lines[i+1]?.trim();
              if (upper.includes('FURNITURE') && /^[\d,]+$/.test(next)) result.furniture_home = parseInt(next.replace(/,/g,''));
              if (upper.includes('HOME APPL') && /^[\d,]+$/.test(next)) result.home_appliances = parseInt(next.replace(/,/g,''));
              if (upper.includes('SPORTS') && /^[\d,]+$/.test(next)) result.sports = parseInt(next.replace(/,/g,''));
              if (upper.includes('MOBILE') && /^[\d,]+$/.test(next)) result.mobiles_tablets = parseInt(next.replace(/,/g,''));
              if (upper === 'ELECTRONICS' && /^[\d,]+$/.test(next)) result.electronics = parseInt(next.replace(/,/g,''));
              if (upper.includes('COMPUTERS') && /^[\d,]+$/.test(next)) result.computers = parseInt(next.replace(/,/g,''));
            }
            return Object.keys(result).length >= 3 ? result : null;
          });
        }
        if (!parsed) throw new Error('dubizzle parse failed — no category data found');
        results.dubizzle = parsed;
        break;
      }

      case 'bayut_d9':
        parsed = parseBayutD9(bodyText);
        if (!parsed) throw new Error('bayut_d9 parse failed');
        results.bayut_d9 = parsed;
        break;

      case 'bayut_ajman_sale':
        parsed = parseBayutCount(bodyText);
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

    log.info(`✓ ${id}: ${JSON.stringify(parsed).substring(0, 150)}`);

    // Close page after each scrape to free memory
    await page.close();
  },

  failedRequestHandler({ request, log, error }) {
    log.error(`✗ ${request.userData.id} failed permanently: ${error.message}`);
    results.errors.push({ source: request.userData.id, error: error.message });
  }
});

await crawler.run(TARGETS.map(t => ({ url: t.url, userData: { id: t.id } })));

const stress = computeStress(results);

const output = {
  date: TODAY,
  scraped_at: SCRAPED_AT,
  stress: { date: TODAY, total: stress.total, band: stress.band, components: stress.components },
  dubizzle_entry: results.dubizzle ? { date: TODAY, scraped_at: SCRAPED_AT, ...results.dubizzle } : null,
  bayut_entry: results.bayut_d9 ? {
    date: TODAY,
    district9_listings: results.bayut_d9.district9_listings,
    d9_avg_price: results.bayut_d9.avg_sale_price,
    benchmark_price_aed: results.benchmark?.benchmark_price_aed ?? 3200000,
    benchmark_flag: 'NO CHANGE'
  } : null,
  luxury_entry: results.luxury ? { date: TODAY, ...results.luxury } : null,
  ajman_entry: (results.bayut_ajman_sale && results.bayut_ajman_rent) ? {
    date: TODAY, scraped_at: SCRAPED_AT,
    ajman_for_sale: results.bayut_ajman_sale,
    ajman_for_rent: results.bayut_ajman_rent,
    ratio: stress.ratio
  } : null,
  errors: results.errors,
  success: results.errors.length === 0
};

console.log('=== OUTPUT ===');
console.log(JSON.stringify(output, null, 2));

await Actor.pushData(output);
await Actor.setValue('OUTPUT', output);
await Actor.exit();
