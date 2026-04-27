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
    : total < 75 ? 'High stress – monitor closely' : 'Crisis signal';
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
  requestHandlerTimeoutSecs: 150,
  maxConcurrency: 1,
  launchContext: {
    launchOptions: {
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
    }
  },

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;
    log.info(`Scraping: ${id} — ${request.url}`);

    // Wait for page load
    try {
      await page.waitForLoadState('networkidle', { timeout: 30000 });
    } catch {
      await page.waitForLoadState('domcontentloaded');
      await sleep(3000);
    }

    const title = await page.title();
    const currentUrl = page.url();

    // CAPTCHA check
    if (/captcha|robot|blocked|challenge/i.test(title) || currentUrl.includes('captchaChallenge')) {
      throw new Error(`CAPTCHA on ${id}: ${title}`);
    }

    // Get text — use page.content() as HTML fallback if innerText is empty
    const bodyText = await page.evaluate(() => {
      const inner = document.body?.innerText || '';
      return inner.trim().length > 50 ? inner : document.documentElement?.outerHTML || '';
    });

    log.info(`${id}: got ${bodyText.length} chars, title="${title}"`);

    if (!bodyText || bodyText.length < 100) {
      throw new Error(`Empty body on ${id} (len=${bodyText?.length}, title="${title}")`);
    }

    let parsed = null;

    switch (id) {
      case 'dubizzle': {
        // Try innerText parse first
        parsed = parseDubizzle(bodyText);

        // If that fails, try to find numbers in the raw HTML
        if (!parsed) {
          log.warning(`Dubizzle: innerText parse failed, trying HTML extraction...`);
          const html = await page.content();
          log.info(`Dubizzle HTML length: ${html.length}`);

          // Extract numbers from HTML — category counts appear as data attributes or JSON
          const htmlOut = {};
          // Pattern: category name near a count number in HTML
          const furM = html.match(/[Ff]urniture[^<]{0,200}?(\d{5,6})/s);
          const appM = html.match(/[Hh]ome\s+[Aa]ppli[^<]{0,200}?(\d{5,6})/s);
          const sporM = html.match(/[Ss]ports\s+[Ee]quip[^<]{0,200}?(\d{4,6})/s);
          const mobM = html.match(/[Mm]obile[^<]{0,200}?(\d{4,6})/s);
          const elecM = html.match(/"ELECTRONICS"[^<]{0,100}?(\d{4,6})/s) || html.match(/[Ee]lectronics[^<]{0,100}>(\d{4,6})/s);
          const compM = html.match(/[Cc]omputers[^<]{0,200}?(\d{4,6})/s);

          if (furM) htmlOut.furniture_home = parseInt(furM[1]);
          if (appM) htmlOut.home_appliances = parseInt(appM[1]);
          if (sporM) htmlOut.sports = parseInt(sporM[1]);
          if (mobM) htmlOut.mobiles_tablets = parseInt(mobM[1]);
          if (elecM) htmlOut.electronics = parseInt(elecM[1]);
          if (compM) htmlOut.computers = parseInt(compM[1]);

          if (Object.keys(htmlOut).length >= 3) {
            parsed = htmlOut;
            log.info(`Dubizzle: HTML extraction got ${Object.keys(htmlOut).length} categories`);
          }
        }

        // If still null, log what we got for debugging
        if (!parsed) {
          const sample = bodyText.substring(0, 500);
          log.error(`Dubizzle: all strategies failed. Body sample: ${sample}`);
          throw new Error('dubizzle: all parse strategies failed');
        }

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

    log.info(`OK ${id}: ${JSON.stringify(parsed).substring(0, 150)}`);
    await page.close();
  },

  failedRequestHandler({ request, log, error }) {
    log.error(`FAILED ${request.userData.id}: ${error.message}`);
    results.errors.push({ source: request.userData.id, error: error.message });
  }
});

const TARGETS = [
  { id: 'dubizzle',         url: 'https://uae.dubizzle.com/classified/' },
  { id: 'bayut_d9',         url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale', url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent', url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',        url: 'https://www.bayut.com/property/details-13073585.html' },
  { id: 'luxury',           url: 'https://www.luxurypricedrops.com/dubai/' }
];

await crawler.run(TARGETS.map(t => ({ url: t.url, userData: { id: t.id } })));

const stress = computeStress(results);
const output = {
  date: TODAY, scraped_at: SCRAPED_AT,
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

console.log('=== FINAL OUTPUT ===');
console.log(JSON.stringify(output, null, 2));
await Actor.pushData(output);
await Actor.setValue('OUTPUT', output);
await Actor.exit();
