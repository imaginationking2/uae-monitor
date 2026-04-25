import { Actor } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

await Actor.init();

const TODAY = new Date().toISOString().split('T')[0];
const SCRAPED_AT = new Date().toISOString();

const results = {
  date: TODAY,
  scraped_at: SCRAPED_AT,
  dubizzle: null,
  bayut_d9: null,
  bayut_ajman_sale: null,
  bayut_ajman_rent: null,
  benchmark: null,
  luxury: null,
  errors: []
};

const UAE_TARGETS = [
  { id: 'dubizzle', url: 'https://uae.dubizzle.com/classified/' },
  { id: 'bayut_d9', url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale', url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent', url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark', url: 'https://www.bayut.com/property/details-13073585.html' }
];

const LUXURY_TARGETS = [
  { id: 'luxury', url: 'https://www.luxurypricedrops.com/dubai/' }
];

function parseDubizzle(bodyText) {
  const lines = bodyText.split('\n').map(l => l.trim()).filter(Boolean);

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
      out[key] = parseInt(lines[i + 1].replace(/,/g, ''), 10);
    }
  }

  if (Object.keys(out).length >= 4) return out;

  const text = bodyText.toUpperCase();

  const patterns = [
    { key: 'furniture_home', re: /FURNITURE[^0-9]{0,50}(\d[\d,]+)/s },
    { key: 'home_appliances', re: /HOME APPLIAN[^0-9]{0,50}(\d[\d,]+)/s },
    { key: 'sports', re: /SPORTS EQUIP[^0-9]{0,50}(\d[\d,]+)/s },
    { key: 'mobiles_tablets', re: /MOBILE PHONE[^0-9]{0,50}(\d[\d,]+)/s },
    { key: 'electronics', re: /ELECTRONICS[^0-9]{0,50}(\d[\d,]+)/s },
    { key: 'computers', re: /COMPUTERS[^0-9]{0,50}(\d[\d,]+)/s }
  ];

  const out2 = {};

  for (const { key, re } of patterns) {
    const match = text.match(re);
    if (match) {
      out2[key] = parseInt(match[1].replace(/,/g, ''), 10);
    }
  }

  return Object.keys(out2).length >= 4 ? out2 : null;
}

function parseBayutCount(bodyText) {
  if (!bodyText) return null;

  const lines = bodyText.split('\n').map(l => l.trim()).filter(Boolean);
  const countLine = lines.find(l => /\d+ to \d+ of [\d,]+ Propert/i.test(l));

  if (countLine) {
    return parseInt(countLine.match(/of ([\d,]+)/)?.[1]?.replace(/,/g, ''), 10);
  }

  const match = bodyText.match(/(\d[\d,]+)\s+Propert/i);
  return match ? parseInt(match[1].replace(/,/g, ''), 10) : null;
}

function parseBayutD9(bodyText) {
  if (!bodyText) return null;

  const count = parseBayutCount(bodyText);
  const lines = bodyText.split('\n').map(l => l.trim()).filter(Boolean);

  const avgLine = lines.find(l => /average sale price.*AED/i.test(l));
  const avg = avgLine
    ? parseInt(avgLine.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g, ''), 10)
    : null;

  return count != null
    ? { district9_listings: count, avg_sale_price: avg }
    : null;
}

function parseBenchmark(pageTitle) {
  const match = pageTitle.match(/AED ([\d.]+)M/i);
  const price = match ? Math.round(parseFloat(match[1]) * 1_000_000) : null;

  return price
    ? { benchmark_price_aed: price, benchmark_flag: 'NO CHANGE' }
    : null;
}

function parseLuxury(pageTitle, bodyText) {
  const countMatch = pageTitle.match(/([\d,]+)\s+Propert/i);
  const dropMatch = pageTitle.match(/Up to ([\d.]+)%/i);

  const count = countMatch
    ? parseInt(countMatch[1].replace(/,/g, ''), 10)
    : null;

  const maxDrop = dropMatch ? parseFloat(dropMatch[1]) : null;

  if (!count) return null;

  const lines = (bodyText || '').split('\n').map(l => l.trim()).filter(Boolean);

  const offMarketLine = lines.find(l => /off-market deals this week/i.test(l));
  const offMarket = offMarketLine
    ? parseInt(offMarketLine.match(/(\d+)/)?.[1], 10)
    : null;

  const avgLine = lines.find(l => /avg.*below asking/i.test(l));
  const avgDropMatch = avgLine?.match(/-?\s*(\d+)%/);
  const avgDrop = avgDropMatch ? parseFloat(avgDropMatch[1]) : 6.5;

  return {
    drop_count: count,
    max_drop_pct: maxDrop,
    avg_drop_pct: avgDrop,
    off_market_deals: offMarket
  };
}

function computeStress(r) {
  const furniturePct = r.dubizzle?.furniture_home
    ? ((r.dubizzle.furniture_home - 133228) / 133228) * 100
    : 0;

  const dubizzleScore = Math.min(25, Math.max(0, Math.round(furniturePct / 0.4)));

  const luxuryScore = r.luxury?.drop_count != null
    ? Math.min(25, Math.max(0, Math.round(((r.luxury.drop_count - 1542) / 1542) * 100 / 2)))
    : 0;

  const d9Count = r.bayut_d9?.district9_listings ?? 31;

  const bayutScore = Math.min(
    25,
    Math.max(0, Math.round(((31 - d9Count) / 31) * 100 / 2))
  );

  const ratio = r.bayut_ajman_sale && r.bayut_ajman_rent
    ? parseFloat((r.bayut_ajman_sale / r.bayut_ajman_rent).toFixed(3))
    : 0;

  const ratioScore = Math.min(25, Math.max(0, Math.round(ratio * 10 - 12)));

  const total = dubizzleScore + luxuryScore + bayutScore + ratioScore;

  const band =
    total < 30 ? 'Stable – no signal'
      : total < 45 ? 'Mild stress building'
      : total < 60 ? 'Clear stress building'
      : total < 75 ? 'High stress – monitor closely'
      : 'Crisis signal';

  return {
    total,
    band,
    components: {
      dubizzle: dubizzleScore,
      luxury: luxuryScore,
      bayut: bayutScore,
      ajman_ratio: ratioScore
    },
    ratio
  };
}

const uaeProxyConfiguration = await Actor.createProxyConfiguration({
  groups: ['RESIDENTIAL'],
  countryCode: 'AE'
});

const globalProxyConfiguration = await Actor.createProxyConfiguration({
  groups: ['RESIDENTIAL']
});

const commonLaunchContext = {
  launchOptions: {
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--js-flags=--max-old-space-size=512'
    ]
  }
};

const uaeCrawler = new PlaywrightCrawler({
  proxyConfiguration: uaeProxyConfiguration,
  maxRequestRetries: 3,
  navigationTimeoutSecs: 90,
  requestHandlerTimeoutSecs: 120,
  maxConcurrency: 1,
  launchContext: commonLaunchContext,

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;

    log.info('Scraping UAE target: ' + id + ' — ' + request.url);

    try {
      await page.waitForLoadState('networkidle', { timeout: 30000 });
    } catch {
      await page.waitForLoadState('domcontentloaded', { timeout: 30000 }).catch(() => {});
      await sleep(3000);
    }

    await sleep(1500 + Math.random() * 1500);

    const title = await page.title();
    const url = page.url();

    if (/captcha|robot|blocked|challenge/i.test(title) || url.includes('captchaChallenge')) {
      throw new Error('CAPTCHA detected on ' + id + ': ' + title);
    }

    const bodyText = await page.evaluate(() => document.body ? document.body.innerText : null);

    if (!bodyText) {
      throw new Error('Empty body on ' + id);
    }

    let parsed = null;

    switch (id) {
      case 'dubizzle': {
        parsed = parseDubizzle(bodyText);

        if (!parsed) {
          parsed = await page.evaluate(() => {
            const result = {};
            const lines = document.body.innerText.split('\n').filter(l => l.trim());

            for (let i = 0; i < lines.length - 1; i++) {
              const upper = lines[i].trim().toUpperCase();
              const next = lines[i + 1]?.trim();

              if (upper.includes('FURNITURE') && /^[\d,]+$/.test(next)) {
                result.furniture_home = parseInt(next.replace(/,/g, ''), 10);
              }

              if (upper.includes('HOME APPL') && /^[\d,]+$/.test(next)) {
                result.home_appliances = parseInt(next.replace(/,/g, ''), 10);
              }

              if (upper.includes('SPORTS') && /^[\d,]+$/.test(next)) {
                result.sports = parseInt(next.replace(/,/g, ''), 10);
              }

              if (upper.includes('MOBILE') && /^[\d,]+$/.test(next)) {
                result.mobiles_tablets = parseInt(next.replace(/,/g, ''), 10);
              }

              if (upper === 'ELECTRONICS' && /^[\d,]+$/.test(next)) {
                result.electronics = parseInt(next.replace(/,/g, ''), 10);
              }

              if (upper.includes('COMPUTERS') && /^[\d,]+$/.test(next)) {
                result.computers = parseInt(next.replace(/,/g, ''), 10);
              }
            }

            return Object.keys(result).length >= 3 ? result : null;
          });
        }

        if (!parsed) throw new Error('dubizzle parse failed');

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

      default:
        throw new Error('Unknown UAE target: ' + id);
    }

    log.info('OK ' + id + ': ' + JSON.stringify(parsed).substring(0, 150));

    await page.close();
  },

  failedRequestHandler({ request, log, error }) {
    log.error('FAILED ' + request.userData.id + ': ' + error.message);
    results.errors.push({
      source: request.userData.id,
      error: error.message
    });
  }
});

const luxuryCrawler = new PlaywrightCrawler({
  proxyConfiguration: globalProxyConfiguration,
  maxRequestRetries: 2,
  navigationTimeoutSecs: 90,
  requestHandlerTimeoutSecs: 120,
  maxConcurrency: 1,
  launchContext: commonLaunchContext,

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;

    log.info('Scraping non-UAE target: ' + id + ' — ' + request.url);

    await page.waitForLoadState('domcontentloaded', { timeout: 30000 }).catch(() => {});
    await sleep(3000);

    const title = await page.title();
    const bodyText = await page.evaluate(() => document.body ? document.body.innerText : '');

    if (/captcha|robot|blocked|challenge/i.test(title)) {
      throw new Error('CAPTCHA detected on luxury: ' + title);
    }

    const parsed = parseLuxury(title, bodyText);

    if (!parsed) {
      throw new Error('luxury parse failed');
    }

    results.luxury = parsed;

    log.info('OK luxury: ' + JSON.stringify(parsed).substring(0, 150));

    await page.close();
  },

  failedRequestHandler({ request, log, error }) {
    log.error('FAILED luxury: ' + error.message);
    results.errors.push({
      source: request.userData.id,
      error: error.message
    });
  }
});

await uaeCrawler.run(
  UAE_TARGETS.map(t => ({
    url: t.url,
    userData: { id: t.id }
  }))
);

await luxuryCrawler.run(
  LUXURY_TARGETS.map(t => ({
    url: t.url,
    userData: { id: t.id }
  }))
);

const stress = computeStress(results);

const output = {
  date: TODAY,
  scraped_at: SCRAPED_AT,

  stress: {
    date: TODAY,
    total: stress.total,
    band: stress.band,
    components: stress.components
  },

  dubizzle_entry: results.dubizzle
    ? {
        date: TODAY,
        scraped_at: SCRAPED_AT,
        ...results.dubizzle
      }
    : null,

  bayut_entry: results.bayut_d9
    ? {
        date: TODAY,
        district9_listings: results.bayut_d9.district9_listings,
        d9_avg_price: results.bayut_d9.avg_sale_price,
        benchmark_price_aed: results.benchmark?.benchmark_price_aed ?? 3200000,
        benchmark_flag: results.benchmark?.benchmark_flag ?? 'NO CHANGE'
      }
    : null,

  luxury_entry: results.luxury
    ? {
        date: TODAY,
        ...results.luxury
      }
    : null,

  ajman_entry: results.bayut_ajman_sale && results.bayut_ajman_rent
    ? {
        date: TODAY,
        scraped_at: SCRAPED_AT,
        ajman_for_sale: results.bayut_ajman_sale,
        ajman_for_rent: results.bayut_ajman_rent,
        ratio: stress.ratio
      }
    : null,

  errors: results.errors,
  success: results.errors.length === 0
};

console.log('=== FINAL OUTPUT ===');
console.log(JSON.stringify(output, null, 2));

await Actor.pushData(output);
await Actor.setValue('OUTPUT', output);

await Actor.exit();
