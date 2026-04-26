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

// ── Parsers ──────────────────────────────────────────────────────────────────

function parseDubizzle(bodyText) {
  if (!bodyText) return null;
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
    if (key && /^[\d,]+$/.test(lines[i + 1]))
      out[key] = parseInt(lines[i + 1].replace(/,/g, ''));
  }
  if (Object.keys(out).length >= 4) return out;

  // Fuzzy fallback
  for (let i = 0; i < lines.length - 1; i++) {
    const upper = lines[i].toUpperCase();
    const next = lines[i + 1];
    if (/^[\d,]+$/.test(next) && parseInt(next.replace(/,/g, '')) > 5000) {
      if (upper.includes('FURNITURE') && !out.furniture_home) out.furniture_home = parseInt(next.replace(/,/g,''));
      if (upper.includes('HOME APPL') && !out.home_appliances) out.home_appliances = parseInt(next.replace(/,/g,''));
      if (upper.includes('SPORTS') && !out.sports) out.sports = parseInt(next.replace(/,/g,''));
      if (upper.includes('MOBILE') && !out.mobiles_tablets) out.mobiles_tablets = parseInt(next.replace(/,/g,''));
      if (upper === 'ELECTRONICS' && !out.electronics) out.electronics = parseInt(next.replace(/,/g,''));
      if (upper.includes('COMPUTERS') && !out.computers) out.computers = parseInt(next.replace(/,/g,''));
    }
  }
  return Object.keys(out).length >= 3 ? out : null;
}

function parseBayutCount(bodyText) {
  if (!bodyText) return null;
  const cl = bodyText.split('\n').map(l => l.trim()).find(l => /\d+ to \d+ of [\d,]+ Propert/i.test(l));
  if (cl) return parseInt(cl.match(/of ([\d,]+)/)?.[1]?.replace(/,/g, ''));
  const m = bodyText.match(/([\d,]+)\s+Propert/i);
  return m ? parseInt(m[1].replace(/,/g, '')) : null;
}

function parseBayutD9(bodyText) {
  if (!bodyText) return null;
  const count = parseBayutCount(bodyText);
  const am = bodyText.split('\n').map(l => l.trim()).find(l => /average sale price.*AED/i.test(l));
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
  const avgDrop = al?.match(/-?\s*(\d+)%/) ? parseFloat(al.match(/-?\s*(\d+)%/)[1]) : 6.5;
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
  const ratio = (r.bayut_ajman_sale && r.bayut_ajman_rent)
    ? parseFloat((r.bayut_ajman_sale / r.bayut_ajman_rent).toFixed(3)) : 0;
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
    groups: ['RESIDENTIAL'],
    countryCode: 'AE'
  });
} catch (e) {
  console.log('Proxy config failed, continuing without proxy:', e.message);
}

// ── Crawler ───────────────────────────────────────────────────────────────────
const crawler = new PlaywrightCrawler({
  proxyConfiguration,
  maxRequestRetries: 3,
  navigationTimeoutSecs: 90,
  requestHandlerTimeoutSecs: 120,
  maxConcurrency: 1,
  launchContext: {
    launchOptions: {
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
    }
  },

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;
    log.info(`Scraping UAE target: ${id} — ${request.url}`);

    // Wait for network idle, fallback to domcontentloaded + delay
    try {
      await page.waitForLoadState('networkidle', { timeout: 30000 });
    } catch {
      await page.waitForLoadState('domcontentloaded');
      await sleep(3000);
    }

    // Check for CAPTCHA
    const title = await page.title();
    const url = page.url();
    if (/captcha|robot|blocked|challenge/i.test(title) || url.includes('captchaChallenge')) {
      throw new Error(`CAPTCHA on ${id}: ${title}`);
    }

    let parsed = null;

    switch (id) {

      case 'dubizzle': {
        // Dubizzle renders counts via React — wait for numbers to appear in DOM
        log.info('Dubizzle: waiting for category counts to render...');
        try {
          await page.waitForFunction(() => {
            const lines = document.body.innerText.split('\n').map(l => l.trim());
            // Look for a line that's a large number (category count)
            return lines.some(l => /^1[2-9]\d,\d{3}$/.test(l) || /^[2-9]\d,\d{3}$/.test(l));
          }, { timeout: 25000 });
        } catch {
          log.warning('Dubizzle: waitForFunction timed out, trying scroll...');
          await page.evaluate(() => window.scrollTo(0, 500));
          await sleep(3000);
        }

        const bodyText = await page.evaluate(() => document.body?.innerText || '');
        parsed = parseDubizzle(bodyText);

        // Last resort: direct DOM extraction inside the page
        if (!parsed) {
          log.warning('Dubizzle: text parse failed, trying DOM extraction...');
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

        if (!parsed) throw new Error('dubizzle parse failed after all strategies');
        results.dubizzle = parsed;
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
        parsed = parseBayutCount(bodyText);
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

      case 'luxury': {
        const bodyText = await page.evaluate(() => document.body?.innerText || '');
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
    date: TODAY,
    scraped_at: SCRAPED_AT,
    ajman_for_sale: results.bayut_ajman_sale,
    ajman_for_rent: results.bayut_ajman_rent,
    ratio: stress.ratio
  } : null,
  errors: results.errors,
  success: results.errors.length === 0
};

console.log(JSON.stringify(output, null, 2));
await Actor.pushData(output);
await Actor.setValue('OUTPUT', output);
await Actor.exit();
