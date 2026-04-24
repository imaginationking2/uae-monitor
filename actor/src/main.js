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
  { id: 'dubizzle',          url: 'https://uae.dubizzle.com/classified/' },
  { id: 'bayut_d9',          url: 'https://www.bayut.com/for-sale/property/ajman/al-zorah/district-9/' },
  { id: 'bayut_ajman_sale',  url: 'https://www.bayut.com/for-sale/property/ajman/' },
  { id: 'bayut_ajman_rent',  url: 'https://www.bayut.com/to-rent/property/ajman/' },
  { id: 'benchmark',         url: 'https://www.bayut.com/property/details-13073585.html' },
  { id: 'luxury',            url: 'https://www.luxurypricedrops.com/dubai/' }
];

function parseDubizzle(t) {
  const lines = t.split('\n').map(l => l.trim()).filter(Boolean);
  const map = {
    'FURNITURE, HOME & GARDEN': 'furniture_home',
    'HOME APPLIANCES': 'home_appliances',
    'SPORTS EQUIPMENT': 'sports',
    'MOBILE PHONES & TABLETS': 'mobiles_tablets',
    'ELECTRONICS': 'electronics',
    'COMPUTERS & NETWORKING': 'computers'
  };
  const out = {};
  for (let i = 0; i < lines.length - 1; i++) {
    const key = map[lines[i]];
    if (key && /^[\d,]+$/.test(lines[i + 1]))
      out[key] = parseInt(lines[i + 1].replace(/,/g, ''));
  }
  return Object.keys(out).length >= 4 ? out : null;
}

function parseBayutD9(t) {
  const lines = t.split('\n').map(l => l.trim()).filter(Boolean);
  const cl = lines.find(l => /\d+ to \d+ of [\d,]+ Propert/i.test(l));
  const total = cl ? parseInt(cl.match(/of ([\d,]+)/)?.[1]?.replace(/,/g,'')) : null;
  const am = lines.find(l => /average sale price.*AED/i.test(l));
  const avg = am ? parseInt(am.match(/AED ([\d,]+)/)?.[1]?.replace(/,/g,'')) : null;
  return total != null ? { district9_listings: total, avg_sale_price: avg } : null;
}

function parseBayutCount(t) {
  const lines = t.split('\n').map(l => l.trim()).filter(Boolean);
  const cl = lines.find(l => /\d+ to \d+ of [\d,]+ Propert/i.test(l));
  return cl ? parseInt(cl.match(/of ([\d,]+)/)?.[1]?.replace(/,/g,'')) : null;
}

function parseBenchmark(title) {
  const m = title.match(/AED ([\d.]+)M/);
  const price = m ? Math.round(parseFloat(m[1]) * 1e6) : null;
  return price ? { benchmark_price_aed: price, benchmark_flag: 'NO CHANGE' } : null;
}

function parseLuxury(title, t) {
  const cm = title.match(/([\d,]+)\s+Propert/);
  const dm = title.match(/Up to ([\d.]+)%/);
  const count = cm ? parseInt(cm[1].replace(/,/g,'')) : null;
  const maxDrop = dm ? parseFloat(dm[1]) : null;
  const lines = t.split('\n').map(l => l.trim()).filter(Boolean);
  const ol = lines.find(l => /off-market deals this week/i.test(l));
  const offMarket = ol ? parseInt(ol.match(/(\d+)/)?.[1]) : null;
  const al = lines.find(l => /avg.*below asking/i.test(l));
  const avgDrop = al?.match(/-?(\d+)%/) ? parseFloat(al.match(/-?(\d+)%/)[1]) : 6.5;
  return count != null ? { drop_count: count, max_drop_pct: maxDrop, avg_drop_pct: avgDrop, off_market_deals: offMarket } : null;
}

function computeStress(r) {
  const furniturePct = r.dubizzle?.furniture_home
    ? ((r.dubizzle.furniture_home - 133228) / 133228) * 100 : 0;
  const dubizzleScore = Math.min(25, Math.round(furniturePct / 0.4));
  const luxuryScore = r.luxury?.drop_count != null
    ? Math.min(25, Math.round((r.luxury.drop_count - 1542) / 1542 * 100 / 2)) : 0;
  const d9Count = r.bayut_d9?.district9_listings ?? 31;
  const bayutScore = Math.min(25, Math.round((31 - d9Count) / 31 * 100 / 2));
  const ratio = (r.bayut_ajman_sale && r.bayut_ajman_rent)
    ? parseFloat((r.bayut_ajman_sale / r.bayut_ajman_rent).toFixed(3)) : 0;
  const ratioScore = Math.min(25, Math.max(0, Math.round(ratio * 10 - 12)));
  const total = dubizzleScore + luxuryScore + bayutScore + ratioScore;
  const band = total < 30 ? 'Stable – no signal' : total < 45 ? 'Mild stress building'
    : total < 60 ? 'Clear stress building' : total < 75 ? 'High stress – monitor closely' : 'Crisis signal';
  return { total, band, components: { dubizzle: dubizzleScore, luxury: luxuryScore, bayut: bayutScore, ajman_ratio: ratioScore }, ratio };
}

const crawler = new PlaywrightCrawler({
  proxyConfiguration: await Actor.createProxyConfiguration({ groups: ['RESIDENTIAL'], countryCode: 'AE' }),
  maxRequestRetries: 3,
  navigationTimeoutSecs: 60,
  requestHandlerTimeoutSecs: 90,

  async requestHandler({ request, page, log }) {
    const { id } = request.userData;
    log.info('Scraping: ' + id);
    await page.waitForLoadState('domcontentloaded');
    await sleep(2000 + Math.random() * 2000);
    const title = await page.title();
    if (/captcha|robot|blocked|challenge/i.test(title)) throw new Error('CAPTCHA on ' + id);
    const body = await page.evaluate(() => document.body.innerText);
    let parsed = null;
    switch (id) {
      case 'dubizzle':          parsed = parseDubizzle(body); results.dubizzle = parsed; break;
      case 'bayut_d9':          parsed = parseBayutD9(body);  results.bayut_d9 = parsed; break;
      case 'bayut_ajman_sale':  parsed = parseBayutCount(body); results.bayut_ajman_sale = parsed; break;
      case 'bayut_ajman_rent':  parsed = parseBayutCount(body); results.bayut_ajman_rent = parsed; break;
      case 'benchmark':         parsed = parseBenchmark(title); results.benchmark = parsed; break;
      case 'luxury':            parsed = parseLuxury(title, body); results.luxury = parsed; break;
    }
    if (!parsed) throw new Error(id + ' parse failed');
    log.info('OK ' + id + ': ' + JSON.stringify(parsed).substring(0, 100));
  },
  failedRequestHandler({ request, error }) {
    results.errors.push({ source: request.userData.id, error: error.message });
  }
});

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

await Actor.pushData(output);
await Actor.setValue('OUTPUT', output);
await Actor.exit();
