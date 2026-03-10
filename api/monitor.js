// DropSync — 24/7 Monitor Cron
// Runs every hour via Vercel Cron (vercel.json schedule).
// Re-scrapes Amazon, pushes qty/price diffs to eBay.
// Client registers via POST /api/monitor?action=register

const SCOPES = 'https://api.ebay.com/oauth/api_scope https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment';
const EBAY_API = 'https://api.ebay.com';
const fs = require('fs');
const KV_PATH = '/tmp/dropsync_monitor.json';

function readKV() {
  try { return JSON.parse(fs.readFileSync(KV_PATH, 'utf8')); } catch { return null; }
}
function writeKV(data) {
  try { fs.writeFileSync(KV_PATH, JSON.stringify(data)); } catch(e) { console.error('KV write err:', e.message); }
}

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
];
const randUA = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

async function fetchPage(url, ua) {
  const headers = { 'User-Agent': ua || randUA(), 'Accept': 'text/html,*/*', 'Accept-Language': 'en-US,en;q=0.9', 'Cache-Control': 'no-cache' };
  try {
    const r = await fetch(url, { headers, signal: AbortSignal.timeout(10000) });
    const html = await r.text();
    if (html.length > 8000 && html.includes('productTitle')) return html;
  } catch {}
  try {
    const r = await fetch(`https://api.allorigins.win/get?url=${encodeURIComponent(url)}`, { signal: AbortSignal.timeout(12000) });
    const d = await r.json();
    const html = d.contents || '';
    if (html.length > 8000 && html.includes('productTitle')) return html;
  } catch {}
  return null;
}

function extractPrice(html) {
  const patterns = [/"priceAmount"\s*:\s*([\d.]+)/, /class="a-price-whole">([0-9,]+)</, /"price"\s*:\s*"?\$?([\d.]+)"?/, /"buyingPrice"\s*:\s*([\d.]+)/];
  for (const p of patterns) {
    const m = html.match(p);
    if (m) { const v = parseFloat(m[1].replace(/,/g, '')); if (v > 0.5 && v < 5000) return v; }
  }
  return null;
}

async function refreshToken(refreshTok) {
  const creds = Buffer.from(`${process.env.EBAY_CLIENT_ID}:${process.env.EBAY_CLIENT_SECRET}`).toString('base64');
  const r = await fetch('https://api.ebay.com/identity/v1/oauth2/token', {
    method: 'POST',
    headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(refreshTok)}&scope=${encodeURIComponent(SCOPES)}`,
  });
  const d = await r.json();
  return d.access_token || null;
}

async function syncProduct(product, accessToken, results) {
  const { id, title, sourceUrl, ebaySku, hasVariations, quantity, variations } = product;
  if (!sourceUrl || !ebaySku) return;
  const auth = { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
  const label = (title || ebaySku).slice(0, 45);
  const html = await fetchPage(sourceUrl, randUA());
  if (!html) { results.push({ id, title: label, status: 'skip', reason: 'Could not fetch Amazon page' }); return; }
  const freshPrice = extractPrice(html);
  const freshStock = !html.toLowerCase().includes('currently unavailable');
  const priceChanges = [], stockChanges = [];

  if (!hasVariations) {
    const offerList = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
    const offer = (offerList.offers || [])[0];
    const oldPrice = parseFloat(offer?.pricingSummary?.price?.value || 0);
    if (offer?.offerId && freshPrice && Math.abs(freshPrice - oldPrice) > 0.01) {
      await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, { method: 'PUT', headers: auth, body: JSON.stringify({ pricingSummary: { price: { value: String(freshPrice.toFixed(2)), currency: 'USD' } } }) }).catch(() => {});
      priceChanges.push(`$${oldPrice.toFixed(2)} → $${freshPrice.toFixed(2)}`);
    }
    const curInv = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
    const oldQty = curInv?.availability?.shipToLocationAvailability?.quantity ?? -1;
    const newQty = freshStock ? parseInt(quantity) || 1 : 0;
    await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, { method: 'PUT', headers: auth, body: JSON.stringify({ availability: { shipToLocationAvailability: { quantity: newQty } }, condition: 'NEW', product: curInv?.product || {} }) }).catch(() => {});
    if (oldQty >= 0 && oldQty !== newQty) stockChanges.push(freshStock ? `Restocked (qty ${newQty})` : 'Out of stock (qty → 0)');
    results.push({ id, title: label, status: 'ok', priceChanges, stockChanges, wentOos: !freshStock && oldQty > 0, price: freshPrice, inStock: freshStock });
    return;
  }

  const colorGroup = (variations || []).find(v => /color/i.test(v.name));
  const colorToAsin = {};
  for (const [, a, b] of html.matchAll(/"([A-Z0-9]{10})"\s*:\s*\{([^}]{0,200})\}/g)) {
    const cM = b.match(/"color_name"\s*:\s*"([^"]+)"/);
    if (cM) colorToAsin[cM[1]] = a;
  }
  const updated = [];
  if (colorGroup?.values?.length) {
    await Promise.all(colorGroup.values.slice(0, 10).map(async cv => {
      const asin = colorToAsin[cv.value]; if (!asin) return;
      const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, randUA()); if (!h) return;
      const p = extractPrice(h); const s = !h.toLowerCase().includes('currently unavailable');
      if (p !== null) updated.push({ color: cv.value, price: p, inStock: s });
    }));
  }
  const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
  for (const sku of (groupRes.variantSKUs || []).slice(0, 80)) {
    const match = updated.find(u => sku.toLowerCase().includes(u.color.replace(/\s+/g, '_').toLowerCase()));
    if (!match) continue;
    const newQty = match.inStock ? parseInt(quantity) || 1 : 0;
    const curInv = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
    const oldQty = curInv?.availability?.shipToLocationAvailability?.quantity ?? -1;
    await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { method: 'PUT', headers: auth, body: JSON.stringify({ availability: { shipToLocationAvailability: { quantity: newQty } }, condition: 'NEW', product: curInv?.product || {} }) }).catch(() => {});
    if (oldQty >= 0 && oldQty !== newQty && (oldQty === 0 || newQty === 0)) stockChanges.push(match.inStock ? `${match.color}: restocked` : `${match.color}: OOS (qty→0)`);
    const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
    const offerEntry = (ol.offers || [])[0];
    if (offerEntry?.offerId) {
      const oldP = parseFloat(offerEntry.pricingSummary?.price?.value || 0);
      if (Math.abs(match.price - oldP) > 0.01) {
        await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerEntry.offerId}`, { method: 'PUT', headers: auth, body: JSON.stringify({ pricingSummary: { price: { value: String(match.price.toFixed(2)), currency: 'USD' } } }) }).catch(() => {});
        priceChanges.push(`${match.color}: $${oldP.toFixed(2)}→$${match.price.toFixed(2)}`);
      }
    }
  }
  results.push({ id, title: label, status: 'ok', priceChanges, stockChanges, wentOos: updated.some(u => !u.inStock) });
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const action = req.query.action || req.body?.action || 'run';

  if (action === 'register') {
    const { products, refresh_token, markup, quantity } = req.body || {};
    if (!products || !refresh_token) return res.status(400).json({ error: 'products and refresh_token required' });
    const tracked = (products || []).filter(p => p.status === 'listed' && p.sourceUrl && p.ebaySku);
    writeKV({ products: tracked, refresh_token, markup, quantity, registeredAt: new Date().toISOString() });
    return res.json({ success: true, tracked: tracked.length, registeredAt: new Date().toISOString() });
  }

  if (action === 'status') {
    const kv = readKV();
    if (!kv) return res.json({ registered: false, message: 'No products registered for monitoring' });
    return res.json({ registered: true, tracked: kv.products?.length || 0, registeredAt: kv.registeredAt, lastRun: kv.lastRun, lastResults: kv.lastResults, lastSummary: kv.lastSummary });
  }

  const kv = readKV();
  if (!kv || !kv.products?.length) return res.json({ success: true, message: 'No products registered — open DropSync to enable 24/7 monitoring', tracked: 0 });

  const startTime = Date.now();
  const accessToken = await refreshToken(kv.refresh_token);
  if (!accessToken) {
    writeKV({ ...kv, lastRun: new Date().toISOString(), lastResults: [{ status: 'error', reason: 'Token refresh failed' }] });
    return res.json({ success: false, error: 'Could not refresh eBay token. Open DropSync to re-authenticate.' });
  }

  const results = [];
  for (const p of kv.products.slice(0, 30)) {
    await syncProduct(p, accessToken, results);
    await new Promise(r => setTimeout(r, 500));
  }

  const summary = {
    ran: results.length,
    priceUpdates: results.reduce((n, r) => n + (r.priceChanges?.length || 0), 0),
    stockUpdates: results.reduce((n, r) => n + (r.stockChanges?.length || 0), 0),
    oosCount: results.filter(r => r.wentOos).length,
    errors: results.filter(r => r.status !== 'ok').length,
    durationMs: Date.now() - startTime,
  };
  writeKV({ ...kv, lastRun: new Date().toISOString(), lastResults: results.slice(0, 50), lastSummary: summary });
  return res.json({ success: true, ...summary, results: results.slice(0, 20) });
};
