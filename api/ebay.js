// DropSync AI Agent — Amazon → eBay Dropshipping Backend
// Clean architecture: per-ASIN prices+images, AI category detection, auto policies

const SCOPES = 'https://api.ebay.com/oauth/api_scope https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment';

function getEbayUrls(sandbox) {
  return {
    EBAY_API:     sandbox ? 'https://api.sandbox.ebay.com'                           : 'https://api.ebay.com',
    EBAY_AUTH:    sandbox ? 'https://auth.sandbox.ebay.com/oauth2/authorize'         : 'https://auth.ebay.com/oauth2/authorize',
    EBAY_TOK:     sandbox ? 'https://api.sandbox.ebay.com/identity/v1/oauth2/token'  : 'https://api.ebay.com/identity/v1/oauth2/token',
    CLIENT_ID:    sandbox ? process.env.EBAY_SANDBOX_CLIENT_ID     : process.env.EBAY_CLIENT_ID,
    CLIENT_SECRET:sandbox ? process.env.EBAY_SANDBOX_CLIENT_SECRET : process.env.EBAY_CLIENT_SECRET,
    REDIRECT:     sandbox ? (process.env.EBAY_SANDBOX_REDIRECT_URI || process.env.EBAY_REDIRECT_URI) : process.env.EBAY_REDIRECT_URI,
  };
}

const UA_LIST = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
];
const randUA = () => UA_LIST[Math.floor(Math.random() * UA_LIST.length)];

// ── Amazon page fetcher: direct + proxy fallbacks ─────────────────────────────
async function fetchPage(url, ua) {
  const isBlocked = (html) =>
    !html ||
    html.includes('Type the characters') ||
    html.includes('robot check') ||
    html.includes('Enter the characters') ||
    html.includes('automated access') ||
    html.includes('api-services-support@amazon.com') ||
    (html.length < 8000 && !html.includes('productTitle'));

  const directHeaders = (agent) => ({
    'User-Agent': agent || ua || randUA(),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Sec-CH-UA': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'Sec-CH-UA-Mobile': '?0',
    'Sec-CH-UA-Platform': '"Windows"',
  });

  // Strategy 1-3: direct fetch with different UAs and URL variants
  const urlVariants = [url, url.replace('?th=1','?psc=1'), url.replace('?th=1','')];
  for (let i = 0; i < 3; i++) {
    try {
      if (i > 0) await sleep(600 + i * 800);
      const r = await fetch(urlVariants[i] || url, { headers: directHeaders(UA_LIST[i % UA_LIST.length]), redirect: 'follow' });
      const html = await r.text();
      if (!isBlocked(html)) { console.log(`[fetch] direct ok attempt ${i+1} len=${html.length}`); return html; }
      console.warn(`[fetch] direct attempt ${i+1} blocked (${html.length}b)`);
    } catch (e) { console.warn(`[fetch] direct attempt ${i+1} error: ${e.message}`); }
  }

  // Strategy 4: allorigins proxy (free CORS proxy that relays the page)
  try {
    await sleep(1000);
    const proxyUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`;
    const r = await fetch(proxyUrl, { headers: { 'User-Agent': randUA() } });
    const d = await r.json();
    const html = d.contents || '';
    if (!isBlocked(html)) { console.log(`[fetch] allorigins ok len=${html.length}`); return html; }
    console.warn(`[fetch] allorigins blocked (${html.length}b)`);
  } catch (e) { console.warn(`[fetch] allorigins error: ${e.message}`); }

  // Strategy 5: scraperapi free tier (no key needed for basic requests)
  try {
    await sleep(800);
    const scraperUrl = `https://api.scraperapi.com/?url=${encodeURIComponent(url)}&render=false`;
    const r = await fetch(scraperUrl, { headers: { 'User-Agent': randUA() } });
    const html = await r.text();
    if (!isBlocked(html)) { console.log(`[fetch] scraperapi ok len=${html.length}`); return html; }
    console.warn(`[fetch] scraperapi blocked (${html.length}b)`);
  } catch (e) { console.warn(`[fetch] scraperapi error: ${e.message}`); }

  return '';
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── eBay title sanitizer — strips words that trigger policy violations ─────────
function sanitizeTitle(title) {
  if (!title) return '';
  // Words eBay bans or that trigger errorId 25019
  const BANNED = [
    /\bauthentic\b/gi, /\bgenuine\b/gi, /\boriginal\b/gi,
    /\breal\b/gi, /\bverified\b/gi, /\bcertified\b/gi,
    /\bauthorized\b/gi, /\bofficially licensed\b/gi,
    /\bnot fake\b/gi, /\bnot a fake\b/gi, /\bnot replica\b/gi,
  ];
  let t = title;
  for (const re of BANNED) t = t.replace(re, '');
  // Collapse multiple spaces
  t = t.replace(/\s{2,}/g, ' ').trim();
  // Remove trailing punctuation/separators
  t = t.replace(/[\-–,|:]+$/, '').trim();
  // Enforce 80 char eBay limit
  if (t.length > 80) t = t.slice(0, 80).replace(/\s+\S*$/, '').trim();
  return t;
}

// ── Extract price from Amazon HTML ────────────────────────────────────────────
function extractPrice(html) {
  const pats = [
    /class="a-price-whole"[^>]*>\s*(\d[\d,]*)<\/span><span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/,
    /"priceAmount"\s*:\s*([\d.]+)/,
    /class="a-offscreen"[^>]*>\$([\d,]+\.?\d*)/,
    /id="priceblock_ourprice"[^>]*>[^$]*\$([\d.]+)/,
    /"displayPrice"\s*:\s*"\$([\d,]+\.?\d*)"/,
  ];
  for (const p of pats) {
    const m = html.match(p);
    if (m) return parseFloat(m[2] ? `${m[1].replace(/,/g,'')}.${m[2]}` : m[1].replace(/,/g,''));
  }
  return null;
}

// ── Extract first hi-res image from an ASIN page ─────────────────────────────
function extractMainImage(html) {
  // colorImages initial[0] is always the hero product shot
  const block = extractBlock(html, 'colorImages');
  if (block) {
    const m = block.match(/'initial'\s*:\s*\[\s*\{\s*(?:[^{}]*?)"hiRes"\s*:\s*"(https:[^"]+\.jpg)"/);
    if (m) return m[1];
  }
  const m = html.match(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/);
  return m ? m[1] : null;
}

// ── Bracket-counting block extractor ─────────────────────────────────────────
// Properly handles nested objects, strings with apostrophes, escape sequences
function extractBlock(html, searchStr) {
  const idx = html.indexOf(searchStr);
  if (idx === -1) return null;
  let i = idx + searchStr.length;
  while (i < html.length && html[i] !== '{' && html[i] !== '[') i++;
  if (i >= html.length) return null;
  const openChar = html[i], closeChar = openChar === '{' ? '}' : ']';
  let depth = 0, inStr = false, strChar = '', escaped = false;
  const start = i;
  for (; i < Math.min(html.length, start + 500000); i++) {
    const c = html[i];
    if (escaped) { escaped = false; continue; }
    if (c === '\\') { escaped = true; continue; }
    if (inStr) { if (c === strChar) inStr = false; continue; }
    if (c === '"' || c === "'") { inStr = true; strChar = c; continue; }
    if (c === openChar) depth++;
    else if (c === closeChar) { depth--; if (depth === 0) return html.slice(start, i + 1); }
  }
  return null;
}

// ── Extract color→image map from colorImages block ────────────────────────────
// Handles: 'Ivory': [...], "L'Special": [...], 'Brown-Checkered': [...]
// ── Extract color→image from swatch img elements in raw HTML ─────────────────
// Confirmed from live Amazon page: swatch imgs have alt="ColorName" src="...._SS64_.jpg"
function extractSwatchImages(html) {
  const map = {};
  const re = /alt="([^"]{2,60})"[^>]{0,200}src="(https:\/\/m\.media-amazon\.com\/images\/I\/[^"_]+\._SS\d+_\.jpg)"/g;
  const seen = new Set();
  let m;
  while ((m = re.exec(html)) !== null) {
    const name = m[1].trim();
    if (!seen.has(name) && name.length > 1 && !name.toLowerCase().includes('brand')) {
      seen.add(name);
      map[name] = m[2].replace(/\._SS\d+_\.jpg$/, '._SL1500_.jpg');
    }
  }
  console.log('[images] swatch map:', Object.keys(map).length, 'colors');
  return map;
}

// ── Extract colorToAsin + sizeToAsin from "colorToAsin" JSON ─────────────────
// Real format: {"Airy Blue 50\"x60\"": {"asin": "B0XXXXX"}, ...}
function extractColorAsinMaps(html) {
  const colorToAsin = {};
  const sizeToAsin  = {};
  const block = extractBlock(html, '"colorToAsin"');
  if (!block) return { colorToAsin, sizeToAsin };
  let data = {};
  try { data = JSON.parse(block); } catch { return { colorToAsin, sizeToAsin }; }
  for (const [key, val] of Object.entries(data)) {
    const asin = val?.asin || (typeof val === 'string' ? val : null);
    if (!asin || asin.length !== 10) continue;
    // Keys: "Color 50\"x60\"" — literal backslash-quotes. After JSON.parse still have \\"
    const cleaned = key.replace(/\\"/g, '"').replace(/\\'/g, "'");
    // Match: ColorName + space + dimensions like 50"x60" or 60"x80"
    const sizeM = cleaned.match(/^(.+?)\s+(\d+(?:\.\d+)?"[×x]\d+(?:\.\d+)?"?)\s*$/i);
    if (sizeM) {
      const color = sizeM[1].trim();
      const size  = sizeM[2].trim();
      if (!colorToAsin[color]) colorToAsin[color] = asin;
      if (!sizeToAsin[size])   sizeToAsin[size]   = asin;
    } else {
      if (!colorToAsin[cleaned]) colorToAsin[cleaned] = asin;
    }
  }
  console.log('[colorToAsin] colors:', Object.keys(colorToAsin).length, 'sizes:', Object.keys(sizeToAsin).length);
  return { colorToAsin, sizeToAsin };
}

// ── eBay Taxonomy API: get leaf category suggestions ─────────────────────────
async function getCategories(title, token, sandbox=false) {
  const EBAY_API = getEbayUrls(sandbox).EBAY_API;
  try {
    const r = await fetch(
      `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_category_suggestions?q=${encodeURIComponent(title.slice(0,80))}`,
      { headers: { Authorization: `Bearer ${token}`, 'Accept-Language': 'en-US' } }
    );
    const d = await r.json();
    return (d.categorySuggestions || []).slice(0, 10).map(s => ({
      id: s.category.categoryId,
      name: s.category.categoryName,
      path: (s.categoryTreeNodeAncestors || []).reverse().map(a => a.categoryName).join(' > '),
    }));
  } catch { return []; }
}

// ── Claude AI: pick category + write aspects + optimize title ─────────────────
async function aiEnrich(title, breadcrumbs, aspects, ebaySuggestions) {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) return null;
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514', max_tokens: 800,
        messages: [{
          role: 'user',
          content: `You are an expert eBay listing optimizer. Given an Amazon product, choose the best eBay leaf category and write optimal item specifics.

Amazon Title: ${title}
Amazon Category Path: ${breadcrumbs.join(' > ')}
Amazon Item Specifics: ${JSON.stringify(aspects).slice(0,400)}
eBay Category Suggestions (id, name, path):
${ebaySuggestions.map(s => `  ${s.id}: ${s.name} (${s.path})`).join('\n')}

Return ONLY a JSON object (no markdown backticks):
{
  "categoryId": "<LEAF category ID from suggestions above>",
  "categoryName": "<category name>",
  "title": "<see title rules below>",
  "aspects": {
    "Brand": ["Unbranded"],
    "Color": ["See Listing"],
    "Material": ["<actual material>"],
    "Size": ["See Listing"]
  }
}

TITLE RULES - follow strictly:
- Exactly 10-12 words, no more no less
- Remove ALL brand/seller/store names (L'AGRATY, Woman Within, Amazon Basics, etc)
- Lead with product type then key descriptors: material, style, use case
- No special chars, colons, quotes, or dimensions in the title
- No ALL CAPS words, no filler words (perfect, great, best)
- NEVER use: authentic, genuine, original, real, verified, certified — eBay bans these
- Title Case formatting
- Good example: "Chunky Knit Throw Blanket Soft Chenille Handmade Couch Home Decor"
- Bad example: "L'AGRATY Authentic Chunky Knit Blanket 30x40 Chenille..."

CRITICAL: categoryId MUST be a LEAF category from the suggestions list. Never pick a parent category.`
        }]
      })
    });
    const d = await r.json();
    const text = (d.content?.[0]?.text || '').replace(/```json|```/g, '').trim();
    return JSON.parse(text);
  } catch (e) {
    console.warn('[AI] enrichment failed:', e.message);
    return null;
  }
}

// ── Resolve all 3 eBay policies, auto-create if missing ───────────────────────
async function resolvePolicies(token, supplied, sandbox=false) {
  const EBAY_API = getEbayUrls(sandbox).EBAY_API;
  const auth = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
  const p = {
    fulfillmentPolicyId: (supplied.fulfillmentPolicyId || '').trim(),
    paymentPolicyId:     (supplied.paymentPolicyId     || '').trim(),
    returnPolicyId:      (supplied.returnPolicyId       || '').trim(),
  };

  if (!p.fulfillmentPolicyId || !p.paymentPolicyId || !p.returnPolicyId) {
    const [fp, pp, rp] = await Promise.all([
      fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r => r.json()).catch(() => ({})),
      fetch(`${EBAY_API}/sell/account/v1/payment_policy?marketplace_id=EBAY_US`,     { headers: auth }).then(r => r.json()).catch(() => ({})),
      fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`,      { headers: auth }).then(r => r.json()).catch(() => ({})),
    ]);
    if (!p.fulfillmentPolicyId) p.fulfillmentPolicyId = (fp.fulfillmentPolicies || [])[0]?.fulfillmentPolicyId || '';
    if (!p.paymentPolicyId)     p.paymentPolicyId     = (pp.paymentPolicies     || [])[0]?.paymentPolicyId     || '';
    if (!p.returnPolicyId)      p.returnPolicyId      = (rp.returnPolicies      || []).find(x => x.returnsAccepted)?.returnPolicyId
                                                        || (rp.returnPolicies || [])[0]?.returnPolicyId || '';
    console.log(`[policies] fp=${p.fulfillmentPolicyId?.slice(-8)} pp=${p.paymentPolicyId?.slice(-8)} rp=${p.returnPolicyId?.slice(-8)}`);
  }

  if (!p.fulfillmentPolicyId) {
    // Try to create a fulfillment policy with multiple fallback service codes
    // eBay accounts have different supported services depending on registration type
    const serviceAttempts = [
      { shippingServiceCode: 'ShippingMethodStandard', freeShipping: true },
      { shippingServiceCode: 'USPSFirstClass', freeShipping: true },
      { shippingServiceCode: 'USPSPriority', freeShipping: true },
    ];
    for (const svc of serviceAttempts) {
      const r = await fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy`, {
        method: 'POST', headers: auth,
        body: JSON.stringify({
          name: 'DropSync Free Shipping', marketplaceId: 'EBAY_US',
          categoryTypes: [{ name: 'ALL_EXCLUDING_MOTORS_VEHICLES', default: true }],
          handlingTime: { value: 3, unit: 'DAY' },
          shippingOptions: [{ optionType: 'DOMESTIC', costType: 'FLAT_RATE',
            shippingServices: [{
              shippingServiceCode: svc.shippingServiceCode,
              freeShipping: svc.freeShipping,
              shippingCost: { value: '0.00', currency: 'USD' },
              buyerResponsibleForShipping: false,
              sortOrder: 1,
            }],
          }],
        }),
      });
      const d = await r.json();
      if (d.fulfillmentPolicyId) { p.fulfillmentPolicyId = d.fulfillmentPolicyId; break; }
      // If name already exists, fetch it
      if ((d.errors||[]).some(e => e.errorId === 20400)) {
        const existing = await fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        p.fulfillmentPolicyId = (existing.fulfillmentPolicies||[]).find(x=>x.name==='DropSync Free Shipping')?.fulfillmentPolicyId
                              || (existing.fulfillmentPolicies||[])[0]?.fulfillmentPolicyId || '';
        if (p.fulfillmentPolicyId) break;
      }
      console.warn('[policy] create failed with', svc.shippingServiceCode, ':', JSON.stringify(d).slice(0,200));
    }
  }
  if (!p.returnPolicyId) {
    const r = await fetch(`${EBAY_API}/sell/account/v1/return_policy`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({
        name: 'DropSync 30-Day Returns', marketplaceId: 'EBAY_US',
        categoryTypes: [{ name: 'ALL_EXCLUDING_MOTORS_VEHICLES' }],
        returnsAccepted: true, returnPeriod: { value: 30, unit: 'DAY' },
        returnShippingCostPayer: 'BUYER', refundMethod: 'MONEY_BACK',
      }),
    });
    const d = await r.json();
    p.returnPolicyId = d.returnPolicyId || '';
    // If name already exists, fetch it
    if (!p.returnPolicyId && (d.errors||[]).some(e => e.errorId === 20400)) {
      const existing = await fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      p.returnPolicyId = (existing.returnPolicies||[]).find(x=>x.name==='DropSync 30-Day Returns')?.returnPolicyId
                       || (existing.returnPolicies||[])[0]?.returnPolicyId || '';
    }
  }

  if (!p.fulfillmentPolicyId) throw new Error(
    'No shipping policy available. ' +
    'Go to eBay Seller Hub → Account → Business Policies → Create a Shipping Policy, ' +
    'then select it in DropSync Settings.'
  );
  if (!p.returnPolicyId) throw new Error(
    'No return policy available. ' +
    'Go to eBay Seller Hub → Account → Business Policies → Create a Return Policy, ' +
    'then select it in DropSync Settings.'
  );
  console.log(`[policies] resolved fp=${p.fulfillmentPolicyId} pp=${p.paymentPolicyId} rp=${p.returnPolicyId}`);
  return p;
}

// ── Ensure merchant location exists ──────────────────────────────────────────
async function ensureLocation(auth, sandbox=false) {
  const EBAY_API = getEbayUrls(sandbox).EBAY_API;
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/location`, { headers: auth }).then(r => r.json()).catch(() => ({}));
  if ((r.locations || []).length) {
    const key = r.locations[0].merchantLocationKey;
    console.log(`[location] found existing: ${key}`);
    return key;
  }
  const key = 'MainWarehouse';
  const cr = await fetch(`${EBAY_API}/sell/inventory/v1/location/${key}`, {
    method: 'POST', headers: auth,
    body: JSON.stringify({
      location: { address: { addressLine1: '123 Main St', city: 'San Jose', stateOrProvince: 'CA', postalCode: '95125', country: 'US' } },
      locationTypes: ['WAREHOUSE'],
      name: key,
      merchantLocationStatus: 'ENABLED',
    }),
  });
  const cd = await cr.json().catch(() => ({}));
  console.log(`[location] create result: ${cr.status} ${JSON.stringify(cd).slice(0,200)}`);
  // Wait for eBay to propagate the new location before using it
  await sleep(3000);
  return key;
}

// ── Build offer payload ───────────────────────────────────────────────────────
function buildOffer(sku, price, categoryId, policies, locationKey) {
  return {
    sku, marketplaceId: 'EBAY_US', format: 'FIXED_PRICE', listingDuration: 'GTC',
    pricingSummary: { price: { value: String(parseFloat(price || 0).toFixed(2)), currency: 'USD' } },
    categoryId: String(categoryId),
    merchantLocationKey: locationKey,
    listingPolicies: {
      fulfillmentPolicyId: policies.fulfillmentPolicyId,
      paymentPolicyId:     policies.paymentPolicyId,
      returnPolicyId:      policies.returnPolicyId,
    },
    tax: { applyTax: true },
  };
}

// ══════════════════════════════════════════════════════════════════════════════
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || req.body?.action;
  const body   = req.body || {};

  try {

    // ── AUTH ──────────────────────────────────────────────────────────────────
    if (action === 'auth') {
      const sandbox = req.query.sandbox === 'true';
      const E = getEbayUrls(sandbox);
      const REDIRECT = E.REDIRECT || `${req.headers['x-forwarded-proto']||'https'}://${req.headers.host}/api/ebay?action=callback`;
      console.log(`[auth] sandbox=${sandbox} client_id=${E.CLIENT_ID?.slice(0,20)} redirect=${REDIRECT}`);
      const url = `${E.EBAY_AUTH}?client_id=${E.CLIENT_ID}&redirect_uri=${encodeURIComponent(REDIRECT)}&response_type=code&scope=${encodeURIComponent(SCOPES)}&state=${sandbox?'sandbox':'production'}`;
      return res.json({ url, sandbox });
    }

    if (action === 'callback') {
      // Detect sandbox from state param passed through OAuth flow
      const sandbox = req.query.state === 'sandbox' || req.query.sandbox === 'true' || process.env.EBAY_SANDBOX === 'true';
      const E = getEbayUrls(sandbox);
      const REDIRECT = E.REDIRECT || `${req.headers['x-forwarded-proto']||'https'}://${req.headers.host}/api/ebay?action=callback`;
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      console.error(`[callback] sandbox=${sandbox} tok_url=${E.EBAY_TOK} client=${E.CLIENT_ID?.slice(0,25)} redirect=${REDIRECT}`);
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=authorization_code&code=${encodeURIComponent(req.query.code)}&redirect_uri=${encodeURIComponent(REDIRECT)}`,
      });
      const d = await r.json();
      if (!d.access_token) {
        console.error('[callback] eBay token exchange failed:', JSON.stringify(d).slice(0,300));
        return res.setHeader('Content-Type','text/html').send(
          `<!DOCTYPE html><html><body><p style="font-family:sans-serif;padding:40px;color:red">
            ❌ eBay auth failed: ${d.error || d.error_description || JSON.stringify(d).slice(0,200)}
          </p></body></html>`
        );
      }
      const payload = {
        type: 'ebay_auth',
        token: d.access_token,
        refresh: d.refresh_token,
        expiry: Date.now() + ((d.expires_in||7200)-120)*1000
      };
      return res.setHeader('Content-Type','text/html').send(
        `<!DOCTYPE html><html><body>
        <p style="font-family:sans-serif;padding:40px">✅ Connected to eBay! Closing…</p>
        <script>
          var payload = ${JSON.stringify(payload)};
          // Try postMessage to opener first
          if (window.opener) {
            window.opener.postMessage(payload, '*');
            setTimeout(function(){ window.close(); }, 800);
          } else {
            // Fallback: store in localStorage then redirect back to app
            try { localStorage.setItem('ebay_auth_pending', JSON.stringify(payload)); } catch(e){}
            document.querySelector('p').textContent = '✅ Connected! Please close this window and return to DropSync.';
          }
        </script>
        </body></html>`
      );
    }

    if (action === 'refresh') {
      const sandbox = (body.sandbox || req.query.sandbox) === 'true';
      const E = getEbayUrls(sandbox);
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(body.refresh_token)}&scope=${encodeURIComponent(SCOPES)}`,
      });
      const d = await r.json();
      if (!d.access_token) return res.status(400).json({ error: 'Token refresh failed', raw: d });
      return res.json({ access_token: d.access_token, expires_in: d.expires_in, expiry: Date.now() + ((d.expires_in||7200)-120)*1000 });
    }

    // ── TEST CREDS ───────────────────────────────────────────────────────────
    if (action === 'test_creds') {
      const sandbox = req.query.sandbox === 'true';
      const E = getEbayUrls(sandbox);
      // Try client credentials grant to verify client_id/secret work
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=client_credentials&scope=${encodeURIComponent('https://api.ebay.com/oauth/api_scope')}`,
      });
      const d = await r.json();
      return res.json({ sandbox, client_id: E.CLIENT_ID, has_secret: !!E.CLIENT_SECRET, result: d });
    }

    // ── POLICIES ─────────────────────────────────────────────────────────────
    if (action === 'policies') {
      const token = body.access_token || req.query.access_token;
      if (!token) return res.status(400).json({ error: 'No token' });
      const sandbox = body.sandbox === true || body.sandbox === 'true' || req.query.sandbox === 'true';
      const EBAY_API = getEbayUrls(sandbox).EBAY_API;
      const auth = { Authorization: `Bearer ${token}`, 'Accept-Language': 'en-US' };
      const [fp, pp, rp] = await Promise.all([
        fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r => r.json()),
        fetch(`${EBAY_API}/sell/account/v1/payment_policy?marketplace_id=EBAY_US`,     { headers: auth }).then(r => r.json()),
        fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`,      { headers: auth }).then(r => r.json()),
      ]);
      return res.json({
        fulfillment: (fp.fulfillmentPolicies||[]).map(p => ({ id: p.fulfillmentPolicyId, name: p.name })),
        payment:     (pp.paymentPolicies||[]).map(p => ({ id: p.paymentPolicyId, name: p.name })),
        return:      (rp.returnPolicies||[]).map(p => ({ id: p.returnPolicyId, name: p.name })),
      });
    }

    // ── SCRAPE: Amazon → structured product data ──────────────────────────────
    if (action === 'scrape') {
      let url = (body.url || req.query.url || '').trim();
      if (!url) return res.status(400).json({ error: 'No URL provided' });

      // Normalize to clean dp/ASIN URL
      const asinM = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
      const asinP = url.match(/[?&]asin=([A-Z0-9]{10})/i);
      const asin  = asinM?.[1] || asinP?.[1];
      // Use ?th=1 to force variant page and avoid redirects
      if (asin) url = `https://www.amazon.com/dp/${asin}?th=1`;

      const ua   = randUA();
      let html = await fetchPage(url, ua);
      // If first attempt blocked, try alternate URL formats
      if (!html && asin) {
        await sleep(1500);
        html = await fetchPage(`https://www.amazon.com/dp/${asin}?psc=1`, ua);
      }
      if (!html && asin) {
        await sleep(2000);
        // Try with a product slug placeholder 
        html = await fetchPage(`https://www.amazon.com/product/dp/${asin}`, ua);
      }
      if (!html) return res.json({ success: false, error: 'Amazon is blocking requests right now. Wait 1-2 minutes and try again.' });

      const product = {
        url, source: 'amazon', asin: asin || '',
        title: '', price: 0, images: [],
        description: '', aspects: {}, breadcrumbs: [],
        variations: [], variationImages: {}, hasVariations: false,
        inStock: true, quantity: 10,
      };

      // Title
      const tM = html.match(/id="productTitle"[^>]*>\s*([^<]{5,})/);
      if (tM) product.title = tM[1].trim().replace(/\s+/g,' ');

      // Breadcrumbs (Amazon category path)
      const bcRaw = [...html.matchAll(/class="a-link-normal"[^>]*>\s*([^<]{2,40})\s*<\/a>/g)];
      product.breadcrumbs = bcRaw.slice(0, 6).map(m => m[1].trim()).filter(s => s.length > 1 && !/^\d+$/.test(s));

      // Price
      product.price = extractPrice(html) || 0;

      // Stock
      product.inStock = !html.toLowerCase().includes('currently unavailable');

      // Images — all hiRes from page
      const imgs = [...html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)]
        .map(m => m[1]).filter((v,i,a) => a.indexOf(v)===i);
      product.images = imgs.slice(0, 12);

      // ── Description: extract feature bullets from multiple Amazon patterns ──────
      const extractBullets = (h) => {
        const results = [];

        // Pattern 1: productFactsDesktopExpander or featurebullets_feature_div
        // Find the "About this item" section content
        const sectionPatterns = [
          /id="productFactsDesktopExpander"[^>]*>([\s\S]{100,8000}?)<\/div>\s*<\/div>\s*<\/div>/,
          /id="featurebullets_feature_div"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
          /id="feature-bullets"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
          /id="FeatureBullets"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
        ];
        let sectionHtml = '';
        for (const pat of sectionPatterns) {
          const m = h.match(pat);
          if (m) { sectionHtml = m[1]; break; }
        }

        // Extract <li> content from the section
        if (sectionHtml) {
          const liMatches = [...sectionHtml.matchAll(/<li[^>]*>[\s\S]*?<span[^>]*class="[^"]*a-list-item[^"]*"[^>]*>([\s\S]{15,600}?)<\/span>/g)];
          for (const m of liMatches) {
            const text = m[1].replace(/<[^>]+>/g, '').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#\d+;/g,'').trim();
            if (text.length > 15 && !text.includes('return') && !text.includes('Your Orders')) {
              results.push(text);
            }
          }
        }

        // Pattern 2: broader a-list-item spans (fallback)
        if (!results.length) {
          const allLi = [...h.matchAll(/<li[^>]*>\s*<span[^>]*a-list-item[^"]*"[^>]*>([\s\S]{20,600}?)<\/span>/g)];
          for (const m of allLi) {
            const text = m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim();
            if (text.length > 20 && !text.includes('return') && !text.includes('Your Orders') && !text.includes('Drop off')) {
              results.push(text);
            }
            if (results.length >= 8) break;
          }
        }

        return results.slice(0, 8);
      };

      const bullets = extractBullets(html);

      // Also extract product description paragraph if exists
      const descParaM = html.match(/id="productDescription"[^>]*>[\s\S]{0,200}<p[^>]*>([\s\S]{30,1000}?)<\/p>/);
      const descPara = descParaM ? descParaM[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim() : '';

      // Store raw bullets for building eBay HTML description later
      product.bullets = bullets;
      product.descriptionPara = descPara;

      // Build the eBay HTML description
      const buildEbayDescription = (title, buls, para, aspects) => {
        const bulletHtml = buls.length
          ? '<ul>' + buls.map(b => `<li>${b.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>'
          : '';
        const specRows = Object.entries(aspects || {})
          .filter(([k,v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v[0] && v[0].length < 80)
          .slice(0, 10)
          .map(([k,v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`)
          .join('');
        const specsTable = specRows
          ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>`
          : '';
        return [
          `<h2>${title}</h2>`,
          bulletHtml,
          para ? `<p>${para}</p>` : '',
          specsTable,
          '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>'
        ].filter(Boolean).join('\n');
      };

      product.description = buildEbayDescription(product.title, bullets, descPara, product.aspects);
      if (!bullets.length) product.description = product.title; // fallback

      // Item specifics from product details table
      const rows = [...html.matchAll(/<th[^>]*class="[^"]*prodDetSectionEntry[^"]*"[^>]*>([^<]+)<\/th>\s*<td[^>]*>([^<]+)<\/td>/gi)];
      for (const [, k, v] of rows) {
        const key = k.trim(), val = v.trim().replace(/\s+/g,' ');
        if (key && val && val.length < 120 && !val.includes('›')) product.aspects[key] = [val];
      }
      // Also grab from feature bullets: "Brand: X"
      for (const [, k, v] of [...html.matchAll(/(Brand|Material|Color|Size|Style|Weight|Dimensions?)\s*:\s*([^\n<]{2,60})/g)]) {
        if (!product.aspects[k]) product.aspects[k] = [v.trim()];
      }

      // ── Variation data ────────────────────────────────────────────────────
      // CONFIRMED from live Amazon page analysis:
      //  - dimensionToAsinMap: "ci_si" → ASIN (only combos that exist & are buyable)
      //  - Each ASIN is a unique combo — prices vary per size AND per color
      //  - STRATEGY: find a "full color" (has all sizes), fetch 1 page per size (6 fetches)
      //    → gives us all 6 size prices. Apply size price to each color+size combo.
      //  - Stock: if combo key exists in dimensionToAsinMap → IN STOCK. Period.

      // 1. Swatch images
      const swatchImgMap = extractSwatchImages(html);

      // 2. variationValues → ordered color/size name arrays
      let varVals = null;
      const vvM = html.match(/"variationValues"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
      if (vvM) try { varVals = JSON.parse(vvM[1]); } catch {}

      const hasVar = !!(varVals && (varVals.color_name?.length || varVals.size_name?.length));
      product.hasVariations = hasVar;

      if (hasVar) {
        const colors = varVals.color_name || [];
        const sizes  = varVals.size_name  || [];

        // 3. Parse dimensionToAsinMap
        const dtaBlock = extractBlock(html, '"dimensionToAsinMap"');
        let dtaMap = {};
        try { dtaMap = JSON.parse(dtaBlock); } catch {}

        // Read "dimensions" array to know correct index order (e.g. ["size_name","color_name"] or ["color_name","size_name"])
        let dimOrder = null;
        // Try multiline-safe regex
        const dimM = html.match(/"dimensions"\s*:\s*(\[[^\]]{0,200}\])/s);
        if (dimM) try { dimOrder = JSON.parse(dimM[1]); } catch {}
        // Fallback: infer from variationValues key order (colors first is most common)
        if (!dimOrder) {
          const vvKeys = Object.keys(varVals || {});
          dimOrder = vvKeys.length ? vvKeys : ['color_name', 'size_name'];
        }
        // Find which dimension index corresponds to color vs size
        const colorDimIdx = dimOrder.indexOf('color_name');
        const sizeDimIdx  = dimOrder.indexOf('size_name');
        // If neither found, default: 0=color, 1=size
        const effectiveColorIdx = colorDimIdx >= 0 ? colorDimIdx : 0;
        const effectiveSizeIdx  = sizeDimIdx  >= 0 ? sizeDimIdx  : 1;
        console.log(`[var] dimensions: ${JSON.stringify(dimOrder)} colorAt=${effectiveColorIdx} sizeAt=${effectiveSizeIdx}`);

        // Build lookup tables
        const comboAsin    = {};  // "Color|Size" → ASIN
        const colorSizeMap = {};  // colorName → { sizeName → ASIN }

        for (const [code, asin] of Object.entries(dtaMap)) {
          const parts = code.split('_').map(Number);
          if (parts.length < 1) continue;
          // Extract color/size indices based on dimension order
          const ci = parts[effectiveColorIdx];
          const si = parts[effectiveSizeIdx];
          const color = colors[ci];
          const size  = (si !== undefined && sizes[si]) ? sizes[si] : (sizes[0] || '');
          if (!color) continue;
          comboAsin[`${color}|${size}`] = asin;
          if (!colorSizeMap[color]) colorSizeMap[color] = {};
          colorSizeMap[color][size] = asin;
        }

        // Fallback: if DTA empty (CAPTCHA block), use colorToAsin
        if (!Object.keys(comboAsin).length) {
          const { colorToAsin: ctaMap } = extractColorAsinMaps(html);
          for (const [c, asin] of Object.entries(ctaMap)) {
            comboAsin[`${c}|`] = asin;
            colorSizeMap[c] = { '': asin };
          }
        }

        console.log(`[var] combos=${Object.keys(comboAsin).length} colors=${colors.length} sizes=${sizes.length}`);

        // 4. Find best "full color" — one that has ALL sizes (most complete data)
        //    Use this color's ASINs to fetch the price for each size.
        //    Since prices on Amazon vary by SIZE (not color), this covers all combos.
        const fullColor = colors.find(c => sizes.every(s => colorSizeMap[c]?.[s]))
                       || colors.find(c => Object.keys(colorSizeMap[c]||{}).length >= sizes.length - 1)
                       || colors[0];

        const sizeToFetchAsin = {};  // size → ASIN to fetch price from
        for (const size of sizes) {
          const asin = colorSizeMap[fullColor]?.[size]
                    || Object.values(colorSizeMap).map(m => m[size]).find(Boolean);
          if (asin) sizeToFetchAsin[size] = asin;
        }
        // For no-size products, fetch the first color's ASIN
        if (!sizes.length && colors[0]) {
          sizeToFetchAsin[''] = Object.values(colorSizeMap[colors[0]]||{})[0] || '';
        }

        console.log(`[var] fetching ${Object.keys(sizeToFetchAsin).length} size prices via "${fullColor}"`);

        // 5a. Fetch per-size prices via fullColor's ASINs (fast, 6 fetches)
        const sizePrices = {};  // sizeName → price (fallback)
        await Promise.all(
          Object.entries(sizeToFetchAsin).map(async ([size, asin]) => {
            if (!asin) return;
            const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
            if (!h) return;
            const p = extractPrice(h);
            if (p) { sizePrices[size] = p; console.log(`[price] "${size}" = $${p} (${asin})`); }
            else     console.log(`[price] "${size}" FAILED (${asin})`);
          })
        );

        // 5b. Build comboPrices: unique ASINs → price (per-combo accuracy)
        //     Batch fetch only ASINs not already covered by sizePrices
        const asinToPrice = {}; // ASIN → price (from size fetches above)
        for (const [size, asin] of Object.entries(sizeToFetchAsin)) {
          if (sizePrices[size] && asin) asinToPrice[asin] = sizePrices[size];
        }
        // Find unique ASINs that have a different price than their size bucket
        const uniqueAsins = [...new Set(Object.values(comboAsin))].filter(a => !asinToPrice[a]);
        const BATCH = 5;
        for (let i = 0; i < uniqueAsins.length; i += BATCH) {
          await Promise.all(uniqueAsins.slice(i, i+BATCH).map(async asin => {
            const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
            if (!h) return;
            const p = extractPrice(h);
            if (p) asinToPrice[asin] = p;
          }));
          if (i + BATCH < uniqueAsins.length) await sleep(300);
        }
        // comboPrices: "Color|Size" → price
        const comboPrices = {};
        for (const [key, asin] of Object.entries(comboAsin)) {
          comboPrices[key] = asinToPrice[asin] || sizePrices[key.split('|')[1]] || 0;
        }
        console.log(`[var] comboPrices: ${Object.keys(comboPrices).length} combos, asinToPrice: ${Object.keys(asinToPrice).length} asins`);

        // Fallback: if all fetches failed, use main page price for all sizes
        const mainPrice = product.price || 0;
        if (!Object.keys(sizePrices).length && mainPrice) {
          sizes.forEach(s => { sizePrices[s] = mainPrice; });
          if (!sizes.length) sizePrices[''] = mainPrice;
        }
        // Any size still missing → use main page price
        sizes.forEach(s => { if (!sizePrices[s]) sizePrices[s] = mainPrice; });
        if (!sizes.length && !sizePrices['']) sizePrices[''] = mainPrice;

        console.log('[var] sizePrices:', JSON.stringify(sizePrices));

        // 6. Build per-color images
        const colorData = {};
        for (const c of colors) colorData[c] = { image: swatchImgMap[c] || '' };

        // Fallback: fetch image for colors still missing
        const needImg = colors.filter(c => !colorData[c].image);
        if (needImg.length) {
          await Promise.all(needImg.slice(0, 6).map(async c => {
            const asin = Object.values(colorSizeMap[c] || {})[0];
            if (!asin) return;
            const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
            if (h) { const img = extractMainImage(h); if (img) colorData[c].image = img; }
          }));
        }

        // 7. Collect product images (all color images, deduped)
        for (const c of colors) {
          const img = colorData[c].image;
          if (img && !product.images.includes(img)) product.images.push(img);
        }
        let fi = 0;
        colors.filter(c => !colorData[c].image).forEach(c => {
          colorData[c].image = product.images[fi++ % Math.max(1, product.images.length)] || '';
        });

        // 8. Build variation groups
        //    COLOR: primary — has image and stock flag
        //    SIZE: secondary — toggle only, shows price for that size
        //    PRICE comes from sizePrices[size] (per size, same across colors)
        if (colors.length) {
          product.variations.push({
            name: 'Color',
            values: colors.map(c => {
              const inStock = sizes.length
                ? sizes.some(s => comboAsin[`${c}|${s}`])
                : !!comboAsin[`${c}|`];
              // Price = cheapest available size for this color
              const availPrices = sizes
                .filter(s => comboAsin[`${c}|${s}`] && sizePrices[s])
                .map(s => sizePrices[s]);
              const colorPrice = availPrices.length
                ? Math.min(...availPrices)
                : (sizePrices[sizes[0]] || mainPrice);
              return {
                value: c, price: colorPrice,
                image: colorData[c].image || '',
                inStock: Object.keys(comboAsin).length > 0 ? inStock : true,
                enabled: Object.keys(comboAsin).length > 0 ? inStock : true,
              };
            }),
          });
          product.variationImages['Color'] = Object.fromEntries(
            colors.map(c => [c, colorData[c].image]).filter(([,img]) => img)
          );
        }

        if (sizes.length) {
          product.variations.push({
            name: 'Size',
            values: sizes.map(s => {
              const inStock = colors.some(c => comboAsin[`${c}|${s}`]);
              return {
                value: s,
                price: sizePrices[s] || mainPrice,
                inStock: Object.keys(comboAsin).length > 0 ? inStock : true,
                enabled: true,
                image: '',
              };
            }),
          });
        }

        // Store on product for push step
        product.comboAsin  = comboAsin;
        product.sizePrices = sizePrices;
        product.comboPrices = comboPrices || {};

        // Product base price = cheapest available size
        const allP = Object.values(sizePrices).filter(p => p > 0);
        if (allP.length) product.price = Math.min(...allP);
      }

      const colorGrp = product.variations.find(v=>v.name==='Color');
      const pricesFound = colorGrp ? colorGrp.values.filter(v=>v.price>0).length : 0;
      const imagesFound = colorGrp ? colorGrp.values.filter(v=>v.image).length : 0;
      console.log(`[scrape] OK "${product.title.slice(0,50)}" price=$${product.price} colors=${colorGrp?.values.length||0} prices=${pricesFound} images=${imagesFound} imgs=${product.images.length}`);
      if (!product.title) {
        return res.json({ success: false, error: 'Amazon blocked the request (bot detection). Wait 30 seconds and try again, or paste the URL directly.' });
      }
      return res.json({ success: true, product, _debug: { pricesFound, imagesFound, totalColors: colorGrp?.values.length||0 } });
    }

    // ── PUSH: create eBay listing ─────────────────────────────────────────────
    if (action === 'push') {
      const { access_token, product, fulfillmentPolicyId, paymentPolicyId, returnPolicyId } = body;
      if (!access_token || !product) return res.status(400).json({ error: 'Missing access_token or product' });

      const sandbox = body.sandbox === true || body.sandbox === 'true';
      const EBAY_API = getEbayUrls(sandbox).EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };
      console.log(`[push] "${product.title?.slice(0,60)}" hasVariations=${product.hasVariations}`);

      // Resolve policies
      let policies;
      try { policies = await resolvePolicies(access_token, { fulfillmentPolicyId, paymentPolicyId, returnPolicyId }, sandbox); }
      catch (e) { return res.status(400).json({ error: e.message }); }

      // AI category + aspects
      const suggestions = await getCategories(product.title || '', access_token);
      const ai = await aiEnrich(product.title, product.breadcrumbs || [], product.aspects || {}, suggestions);
      const categoryId = ai?.categoryId || suggestions[0]?.id || '11450';
      const rawTitle = product.ebayTitle || ai?.title || product.title || 'Product';
      const listingTitle = sanitizeTitle(rawTitle) || sanitizeTitle(product.title) || 'Product';
      console.log(`[push] title: "${listingTitle}" (raw: "${rawTitle.slice(0,60)}")`);

      // Rebuild eBay description with final listing title + bullets from product
      const buildEbayDesc = (title, bullets, para, aspects) => {
        const bulletHtml = (bullets||[]).length
          ? '<ul>' + bullets.map(b => `<li>${String(b).replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>'
          : '';
        const specRows = Object.entries(aspects || {})
          .filter(([k,v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v[0] && String(v[0]).length < 80)
          .slice(0, 10)
          .map(([k,v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`)
          .join('');
        const specsTable = specRows
          ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>`
          : '';
        return [
          `<h2>${title}</h2>`,
          bulletHtml,
          para ? `<p>${para}</p>` : '',
          specsTable,
          '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>'
        ].filter(Boolean).join('\n');
      };
      const ebayDescription = buildEbayDesc(listingTitle, product.bullets || [], product.descriptionPara || '', product.aspects || {})
                           || product.description
                           || listingTitle;
      // Strip Color/Size from base aspects — variants will set their own single values
      const rawAspects = { ...(product.aspects || {}), ...(ai?.aspects || {}) };
      delete rawAspects['Color']; delete rawAspects['color'];
      delete rawAspects['Size'];  delete rawAspects['size'];
      const aspects = rawAspects;

      // Fetch required item specifics for this category and fill missing ones
      try {
        const catMeta = await fetch(
          `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_item_aspects_for_category?category_id=${categoryId}`,
          { headers: { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' } }
        ).then(r => r.json()).catch(() => ({}));
        for (const aspect of (catMeta.aspects || [])) {
          const name = aspect.aspectConstraint?.aspectRequired ? aspect.localizedAspectName : null;
          if (!name) continue;
          if (aspects[name]) continue; // already have it
          // Try to find a value from product title/description
          const vals = (aspect.aspectValues || []).map(v => v.localizedValue);
          if (!vals.length) continue;
          const titleLower = (product.title || '').toLowerCase();
          const match = vals.find(v => titleLower.includes(v.toLowerCase())) || vals[0];
          aspects[name] = [match];
          console.log(`[aspects] auto-filled required "${name}" = "${match}"`);
        }
      } catch(e) { console.warn('[aspects] fetch failed:', e.message); }
      console.log(`[push] cat=${categoryId} "${listingTitle.slice(0,50)}"`);

      // Merchant location
      const locationKey = await ensureLocation(auth, sandbox);
      const basePrice   = parseFloat(product.price || 0).toFixed(2);
      const groupSku    = `DS-${Date.now()}-${Math.random().toString(36).slice(2,7).toUpperCase()}`;

      // ── SIMPLE LISTING ─────────────────────────────────────────────────────
      if (!product.hasVariations || !product.variations?.length) {
        const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(groupSku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: parseInt(product.quantity)||10 } },
            condition: 'NEW',
            product: { title: listingTitle, description: ebayDescription, imageUrls: product.images.slice(0,12), aspects },
          }),
        });
        if (!ir.ok) return res.status(400).json({ error: 'Inventory PUT failed', details: await ir.text() });

        const or = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, {
          method: 'POST', headers: auth,
          body: JSON.stringify(buildOffer(groupSku, basePrice, categoryId, policies, locationKey)),
        });
        const od = await or.json();
        if (!or.ok) return res.status(400).json({ error: 'Offer failed', details: od });

        const pr  = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${od.offerId}/publish`, { method: 'POST', headers: auth });
        const pd  = await pr.json();
        return res.json({ success: true, sku: groupSku, offerId: od.offerId, listingId: pd.listingId });
      }

      // ── VARIATION LISTING ──────────────────────────────────────────────────
      const colorGroup = product.variations.find(v => /color|colour/i.test(v.name));
      const sizeGroup  = product.variations.find(v => /size/i.test(v.name));
      const colorImgs  = product.variationImages?.['Color'] || {};

      // Build flat variant list
      const variants = [];
      // eBay SKU max = 50 chars. groupSku = "DS-XXXXXXXXXXXXX-XXXXX" (22 chars) + "-" = 23
      // So variant suffix can be at most 27 chars
      const SKU_MAX = 50;
      const skuPrefix = groupSku + '-';
      const maxSuffix = SKU_MAX - skuPrefix.length; // typically 27
      const mkSku = parts => {
        const raw = parts.join('_').replace(/[^A-Z0-9]/gi,'_').toUpperCase().replace(/_+/g,'_').replace(/^_|_$/g,'');
        if (raw.length <= maxSuffix) return skuPrefix + raw;
        // Hash entire raw string into 8 hex chars, keep first (maxSuffix-9) chars of raw
        const hash = raw.split('').reduce((h,c)=>((h<<5)-h+c.charCodeAt(0))|0, 0);
        const hashStr = (hash >>> 0).toString(16).toUpperCase().slice(0,8).padStart(8,'0');
        return skuPrefix + raw.slice(0, maxSuffix - 9) + '_' + hashStr;
      };

      const defaultQty = parseInt(product.quantity) || 10;
      // comboAsin:  "Color|Size" → ASIN  (only combos that exist on Amazon)
      // sizePrices: sizeName → price (fetched from best-coverage color, applies to all colors at that size)
      const comboAsin   = product.comboAsin   || {};
      const sizePrices  = product.sizePrices  || {};
      const comboPrices = product.comboPrices || {}; // "Color|Size" → amazon price
      const markupPct   = parseFloat(product.markup || 35);
      const handling    = 2; // $2 handling cost
      const ebayFee     = 0.1335;
      const applyMarkup = (cost) => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return c;
        return Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - ebayFee) + 0.30) * 100) / 100;
      };
      console.log(`[push] comboAsin entries=${Object.keys(comboAsin).length} comboPrices entries=${Object.keys(comboPrices).length} markup=${markupPct}%`);

      if (colorGroup && sizeGroup) {
        for (const cv of colorGroup.values.filter(v => v.enabled !== false)) {
          for (const sv of sizeGroup.values.filter(v => v.enabled !== false)) {
            const key = `${cv.value}|${sv.value}`;
            const hasCombo = Object.keys(comboAsin).length === 0 || !!comboAsin[key];
            // Use comboPrices (per-combo) → sizePrices (per-size) → basePrice fallback
            const amazonPrice = comboPrices[key]
                             || sizePrices[sv.value]
                             || parseFloat(sv.price || basePrice);
            const price = applyMarkup(amazonPrice).toFixed(2);
            // qty=0 means out-of-stock on eBay (combo doesn't exist on Amazon)
            const qty = hasCombo ? defaultQty : 0;
            variants.push({
              sku:   mkSku([cv.value, sv.value]),
              color: cv.value, size: sv.value,
              price, qty,
              image: colorImgs[cv.value] || product.images[0] || '',
              inStock: hasCombo,
            });
          }
        }
      } else if (colorGroup) {
        for (const cv of colorGroup.values.filter(v => v.enabled !== false)) {
          const key = `${cv.value}|`;
          const hasCombo = Object.keys(comboAsin).length === 0 || !!comboAsin[key];
          const amazonPrice = comboPrices[key] || parseFloat(cv.price || basePrice);
          variants.push({
            sku:   mkSku([cv.value]), color: cv.value, size: null,
            price: applyMarkup(amazonPrice).toFixed(2),
            image: colorImgs[cv.value] || product.images[0] || '',
            qty:   hasCombo ? defaultQty : 0,
            inStock: hasCombo,
          });
        }
      } else if (sizeGroup) {
        for (const sv of sizeGroup.values.filter(v => v.enabled !== false)) {
          const key = `|${sv.value}`;
          const amazonPrice = comboPrices[key] || sizePrices[sv.value] || parseFloat(sv.price || basePrice);
          variants.push({
            sku:   mkSku([sv.value]), color: null, size: sv.value,
            price: applyMarkup(amazonPrice).toFixed(2),
            image: product.images[0] || '',
            qty:   defaultQty,
          });
        }
      }
      const final = variants.slice(0, 250);
      console.log(`[push] ${final.length} variants`);

      // PUT each inventory_item (batched)
      // Test with a single item first to catch 401 before running 240 requests
      let tokenInvalid = false;
      {
        const testV = final[0];
        const testAsp = { ...aspects };
        if (testV.color) testAsp['Color'] = [testV.color];
        if (testV.size)  testAsp['Size']  = [testV.size];
        const testR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(testV.sku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: testV.qty } },
            condition: 'NEW',
            product: {
              title: listingTitle, description: ebayDescription,
              imageUrls: [testV.image, ...product.images.filter(x => x !== testV.image)].filter(Boolean).slice(0, 12),
              aspects: testAsp,
            },
          }),
        });
        if (testR.status === 401) {
          tokenInvalid = true;
          console.warn('[push] 401 on inventory — token missing sell.inventory scope. Full re-auth required.');
          return res.status(401).json({
            error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.',
            code: 'INVENTORY_401',
          });
        }
      }

      const createdSkus = new Set([final[0].sku]); // test PUT already succeeded
      const failedInvSkus = [];

      for (let i = 1; i < final.length; i += 8) {
        await Promise.all(final.slice(i, i+8).map(async v => {
          const asp = { ...aspects };
          if (v.color) asp['Color'] = [v.color];
          if (v.size)  asp['Size']  = [v.size];
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(v.sku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              availability: { shipToLocationAvailability: { quantity: v.qty } },
              condition: 'NEW',
              product: {
                title: listingTitle, description: ebayDescription,
                imageUrls: [v.image, ...product.images.filter(x => x !== v.image)].filter(Boolean).slice(0, 12),
                aspects: asp,
              },
            }),
          });
          if (r.ok || r.status === 204) createdSkus.add(v.sku);
          else {
            const rb = await r.text().catch(()=>'');
            console.warn(`[push] inv FAIL ${v.sku.slice(-20)}: ${r.status} ${rb.slice(0,80)}`);
            failedInvSkus.push(v);
          }
        }));
        if (i+8 < final.length) await sleep(150);
      }

      // Retry failed inventory items once after a short wait
      if (failedInvSkus.length) {
        console.log(`[push] retrying ${failedInvSkus.length} failed inventory items...`);
        await sleep(1000);
        for (let i = 0; i < failedInvSkus.length; i += 8) {
          await Promise.all(failedInvSkus.slice(i, i+8).map(async v => {
            const asp = { ...aspects };
            if (v.color) asp['Color'] = [v.color];
            if (v.size)  asp['Size']  = [v.size];
            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(v.sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: v.qty } },
                condition: 'NEW',
                product: {
                  title: listingTitle, description: ebayDescription,
                  imageUrls: [v.image, ...product.images.filter(x => x !== v.image)].filter(Boolean).slice(0, 12),
                  aspects: asp,
                },
              }),
            });
            if (r.ok || r.status === 204) createdSkus.add(v.sku);
            else console.warn(`[push] inv retry FAIL ${v.sku.slice(-20)}: ${r.status}`);
          }));
          if (i+8 < failedInvSkus.length) await sleep(200);
        }
      }
      console.log(`[push] inventory items: ${createdSkus.size}/${final.length} created`);

      // PUT inventory_item_group
      // Color must be FIRST in variesBy so eBay treats it as primary (image-bearing) variation
      const varAspects = {};
      if (colorGroup) varAspects['Color'] = colorGroup.values.filter(v=>v.enabled!==false).map(v => v.value);
      if (sizeGroup)  varAspects['Size']  = sizeGroup.values.map(v => v.value);

      // Build color→imageUrl map for eBay to display correct image per color swatch
      const colorImgUrls = colorGroup
        ? Object.fromEntries(
            colorGroup.values
              .filter(v => colorImgs[v.value])
              .map(v => [v.value, colorImgs[v.value]])
          )
        : {};

      let groupOk = false;
      for (let attempt = 1; attempt <= 3 && !groupOk; attempt++) {
        const colorUrlList = Object.values(colorImgUrls).filter(Boolean).slice(0,12);
        const groupImageUrls = colorUrlList.length ? colorUrlList : product.images.slice(0, 12);
        const variesBySpecs = Object.entries(varAspects).map(([name, values]) => ({ name, values }));
        // aspects on the group = non-variation aspects only (Color/Size go in variesBy)
        const groupAspects = { ...aspects };
        delete groupAspects['Color']; delete groupAspects['Size'];
        const groupBody = {
            inventoryItemGroupKey: groupSku,
            title: listingTitle,
            description: ebayDescription,
            imageUrls: groupImageUrls,
            variantSKUs: final.map(v => v.sku).filter(s => createdSkus.has(s)),
            aspects: groupAspects,
            variesBy: {
              aspectsImageVariesBy: colorGroup ? ['Color'] : [],
              specifications: variesBySpecs,
            },
        };
        console.log('[push] group body keys:', Object.keys(groupBody), 'variantSKUs:', groupBody.variantSKUs.length, 'variesBy:', groupBody.variesBy);
        const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify(groupBody),
        });
        if (gr.ok || gr.status === 204) { groupOk = true; console.log('[push] group ok'); }
        else {
          const gt = await gr.text();
          console.warn(`[push] group attempt ${attempt}: ${gr.status}`, gt.slice(0,400));
          if (attempt < 3) await sleep(600); else return res.status(400).json({ error: 'Group PUT failed', details: gt.slice(0,400) });
        }
      }

      // Bulk create offers (25 at a time), with one retry pass for location errors
      const allOfferIds = [];
      const failedVariants = [];
      for (let i = 0; i < final.length; i += 25) {
        const batch = final.slice(i, i+25).map(v => buildOffer(v.sku, v.price, categoryId, policies, locationKey));
        const or = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`, {
          method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
        });
        const od = await or.json();
        for (const resp of (od.responses || [])) {
          if (resp.offerId) { allOfferIds.push(resp.offerId); }
          else if (resp.errors?.length) {
            const isLocationErr = (resp.errors[0]?.errorId === 25002);
            console.warn(`[offer] SKU ${resp.sku?.slice(-20)} error:`, JSON.stringify(resp.errors[0]).slice(0,200));
            if (isLocationErr) failedVariants.push(final.find(v => v.sku === resp.sku));
          }
        }
        const batchOk = (od.responses||[]).filter(r=>r.offerId).length;
        const batchFail = (od.responses||[]).filter(r=>!r.offerId).length;
        console.log(`[push] offers batch ${Math.floor(i/25)+1}: ${batchOk} ok, ${batchFail} failed`);
        if (batchFail && batchOk === 0) {
          const firstErr = (od.responses||[]).find(r=>r.errors?.length);
          const isLocationErr = firstErr?.errors[0]?.errorId === 25002;
          if (firstErr && !isLocationErr) return res.status(400).json({ error: 'Offer creation failed', details: firstErr.errors[0] });
        }
      }
      // Retry location-failed offers after a delay
      if (failedVariants.length) {
        console.log(`[push] retrying ${failedVariants.length} location-failed offers after delay...`);
        await sleep(4000);
        for (let i = 0; i < failedVariants.length; i += 25) {
          const batch = failedVariants.filter(Boolean).slice(i, i+25).map(v => buildOffer(v.sku, v.price, categoryId, policies, locationKey));
          const or = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`, {
            method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
          });
          const od = await or.json();
          for (const resp of (od.responses || [])) {
            if (resp.offerId) { allOfferIds.push(resp.offerId); }
            else console.warn(`[offer] retry failed SKU ${resp.sku?.slice(-20)}:`, JSON.stringify(resp.errors?.[0]).slice(0,150));
          }
          console.log(`[push] retry batch: ${(od.responses||[]).filter(r=>r.offerId).length} ok`);
        }
      }

      // Publish by group
      for (let attempt = 1; attempt <= 3; attempt++) {
        const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/publish_by_inventory_item_group`, {
          method: 'POST', headers: auth,
          body: JSON.stringify({ inventoryItemGroupKey: groupSku, marketplaceId: 'EBAY_US' }),
        });
        const pd = await pr.json();
        if (pd.listingId) {
          console.log(`[push] published! listingId=${pd.listingId}`);
          return res.json({ success: true, sku: groupSku, listingId: pd.listingId, variantsCreated: final.length });
        }
        const errId = (pd.errors||[])[0]?.errorId;
        console.warn(`[push] publish attempt ${attempt}: ${JSON.stringify(pd).slice(0,300)}`);
        if (attempt === 3) return res.status(400).json({ error: 'Publish failed', details: pd, errorId: errId });
        // Add missing aspects if required
        if (errId === 25004 || errId === 25003) {
          for (const param of (pd.errors||[])[0]?.parameters||[]) {
            if (!aspects[param.value]) aspects[param.value] = ['Unbranded'];
          }
        }
        await sleep(800);
      }
    }

    // ── FETCH MY EBAY LISTINGS ────────────────────────────────────────────────
    if (action === 'fetchMyListings') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };

      // Paginate through all active offers
      const allOffers = [];
      let offset = 0;
      const limit = 100;
      let total = null;

      while (true) {
        const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=${limit}&offset=${offset}`, { headers: auth });
        const d = await r.json();
        if (!r.ok) {
          console.error('[fetchMyListings] offers error:', JSON.stringify(d).slice(0,300));
          break;
        }
        const offers = d.offers || [];
        allOffers.push(...offers);
        total = d.total || offers.length;
        console.log(`[fetchMyListings] fetched ${allOffers.length}/${total} offers`);
        if (allOffers.length >= total || offers.length < limit) break;
        offset += limit;
      }

      // Map to clean listing objects
      const listings = allOffers
        .filter(o => o.status === 'PUBLISHED' || o.listingId)
        .map(o => ({
          ebayListingId: o.listingId || '',
          ebaySku:       o.sku || '',
          title:         o.listingDescription?.slice(0,80) || o.sku || '',
          price:         parseFloat(o.pricingSummary?.price?.value || 0),
          quantity:      o.availableQuantity || 0,
          status:        o.status || 'PUBLISHED',
          currency:      o.pricingSummary?.price?.currency || 'USD',
          categoryId:    o.categoryId || '',
          ebayUrl:       o.listingId ? `https://www.ebay.com/itm/${o.listingId}` : '',
        }));

      // Also try to get item titles from inventory items (for better display)
      const skus = [...new Set(allOffers.map(o => o.sku).filter(Boolean))].slice(0, 200);
      const inventoryMap = {};
      // Batch fetch inventory items for titles/images
      for (let i = 0; i < skus.length; i += 25) {
        const batch = skus.slice(i, i+25);
        try {
          const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?sku=${batch.join('|')}`, { headers: auth });
          const id = await ir.json();
          for (const item of (id.inventoryItems || [])) {
            inventoryMap[item.sku] = {
              title: item.product?.title || '',
              image: item.product?.imageUrls?.[0] || '',
              aspects: item.product?.aspects || {},
            };
          }
        } catch {}
      }

      // Enrich listings with inventory data
      for (const l of listings) {
        const inv = inventoryMap[l.ebaySku];
        if (inv) {
          if (inv.title) l.title = inv.title;
          if (inv.image) l.image = inv.image;
          l.aspects = inv.aspects;
        }
      }

      console.log(`[fetchMyListings] returning ${listings.length} listings`);
      return res.json({ success: true, listings, total: listings.length });
    }


    // ── SYNC: re-scrape Amazon, push diffs to eBay ────────────────────────────
    if (action === 'sync') {
      const { access_token, ebaySku, sourceUrl, hasVariations, quantity, variations, variationImages } = body;
      if (!access_token || !ebaySku) return res.status(400).json({ error: 'Missing fields' });
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };

      // Re-scrape to get fresh data
      const ua   = randUA();
      const html = await fetchPage(sourceUrl, ua);
      if (!html) return res.json({ success: false, error: 'Could not re-fetch Amazon page' });

      const freshPrice = extractPrice(html);
      const freshStock = !html.toLowerCase().includes('currently unavailable');

      if (!hasVariations) {
        const offerList = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        const offerId = (offerList.offers||[])[0]?.offerId;
        if (offerId && freshPrice) {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({ pricingSummary: { price: { value: String(freshPrice.toFixed(2)), currency: 'USD' } } }),
          });
        }
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({ availability: { shipToLocationAvailability: { quantity: freshStock ? parseInt(quantity)||10 : 0 } }, condition: 'NEW', product: {} }),
        });
        return res.json({ success: true, price: freshPrice, inStock: freshStock });
      }

      // Variation sync: update each color ASIN's price/stock
      const colorGroup = (variations||[]).find(v => /color/i.test(v.name));
      const colorImgs  = variationImages?.['Color'] || {};
      const colorToAsin = {};
      // Re-extract ASIN map
      const dimMatches = [...html.matchAll(/"([A-Z0-9]{10})"\s*:\s*\{([^}]{0,200})\}/g)];
      for (const [,a,b] of dimMatches) {
        const cM = b.match(/"color_name"\s*:\s*"([^"]+)"/);
        if (cM) colorToAsin[cM[1]] = a;
      }

      const updated = [];
      if (colorGroup?.values?.length) {
        // Fetch prices for each color ASIN
        await Promise.all(colorGroup.values.slice(0,12).map(async cv => {
          const asin = colorToAsin[cv.value];
          if (!asin) return;
          const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
          if (!h) return;
          const p = extractPrice(h);
          const s = !h.toLowerCase().includes('currently unavailable');
          if (p) updated.push({ color: cv.value, price: p, inStock: s });
        }));
      }

      // Push updates to eBay inventory items
      const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      const skus = groupRes.variantSKUs || [];
      let syncCount = 0;
      for (const sku of skus.slice(0, 100)) {
        const skuLower = sku.toLowerCase();
        const match = updated.find(u => skuLower.includes(u.color.replace(/\s+/g,'_').toLowerCase()));
        if (!match) continue;
        // Update inventory item qty
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({ availability: { shipToLocationAvailability: { quantity: match.inStock ? parseInt(quantity)||10 : 0 } }, condition: 'NEW', product: {} }),
        });
        // Update offer price
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        const oid = (ol.offers||[])[0]?.offerId;
        if (oid) {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${oid}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({ pricingSummary: { price: { value: String(match.price.toFixed(2)), currency: 'USD' } } }),
          });
        }
        syncCount++;
      }
      return res.json({ success: true, updatedVariants: syncCount, freshData: updated });
    }

    // ── END LISTING ───────────────────────────────────────────────────────────
    // ── DEALS SCRAPE v2: get ASINs from Amazon Today's Deals page ────────────────
    if (action === 'bulkScrapeAsins') action = 'dealsScrape'; // backward compat alias
    if (action === 'dealsScrape') {
      const { exclude = [], count = 25 } = body;
      const excludeSet = new Set(exclude);

      // VeRO / brand-protected items to skip (eBay will reject these)
      const VERO_BRANDS = new Set([
        'samsung','apple','nike','adidas','sony','microsoft','lg','bose',
        'beats','dyson','lego','gucci','louis vuitton','chanel','rolex',
        'prada','versace','burberry','yeezy','jordan','off-white','supreme',
        'north face','ugg','timberland','new balance','under armour','reebok',
        'puma','vans','converse','dr. martens','crocs','birkenstock',
        'kindle','echo','ring','roomba','nespresso','keurig','instant pot',
        'nintendo','playstation','xbox','fitbit','garmin','gopro',
        'turbotax','doordash','uber','instacart','grubhub',
        'pokemon','disney','marvel','dc comics','star wars','harry potter',
      ]);

      // Digital / non-physical product keywords to skip
      const SKIP_KEYWORDS = [
        'ebook','kindle edition','gift card','egift','digital code',
        'game download','app purchase','audible','prime video',
        'rolling balls','brain games','alpha\'s bride','design home',
        'iq boost','training brain','free 3d game',
      ];

      const isVero = (title, brand) => {
        const t = (title + ' ' + (brand||'')).toLowerCase();
        for (const b of VERO_BRANDS) if (t.includes(b)) return true;
        for (const k of SKIP_KEYWORDS) if (t.includes(k)) return true;
        return false;
      };

      const dealsUrl = 'https://www.amazon.com/deals?ref_=nav_cs_gb';
      const html = await fetchPage(dealsUrl, randUA());
      if (!html) return res.json({ success: false, error: 'Could not load Amazon deals page. Try again in a moment.' });

      // Extract ASINs + titles from the deals page
      const productMap = {}; // asin → { asin, title, url }

      // 1) JSON "asin":"B..." pattern (richest source — JS bundles on deals page)
      for (const m of html.matchAll(/"asin"\s*:\s*"([B][0-9A-Z]{9})"/g)) {
        productMap[m[1]] = productMap[m[1]] || { asin: m[1], title: '' };
      }

      // 2) data-asin attributes
      for (const m of html.matchAll(/data-asin="([B][0-9A-Z]{9})"/g)) {
        productMap[m[1]] = productMap[m[1]] || { asin: m[1], title: '' };
      }

      // 3) URL slugs give product title hints
      for (const m of html.matchAll(/href="([^"]*\/([^\/]+)\/dp\/([B][0-9A-Z]{9})[^"]*)"/g)) {
        const asin = m[3];
        const slug = decodeURIComponent(m[2]).replace(/-/g, ' ');
        if (asin && slug.length > 5) {
          productMap[asin] = { asin, title: slug, url: 'https://www.amazon.com/dp/' + asin };
        }
      }

      // 4) img alt text near dp links
      for (const m of html.matchAll(/\/dp\/([B][0-9A-Z]{9})[^"]*"[^>]*>[\s\S]{0,300}?alt="([^"]{10,120})"/g)) {
        if (!productMap[m[1]]?.title || productMap[m[1]].title.length < 10) {
          productMap[m[1]] = { asin: m[1], title: m[2], url: 'https://www.amazon.com/dp/' + m[1] };
        }
      }

      // Filter: exclude already-used, VeRO, and digital items
      const allProducts = Object.values(productMap).filter(p =>
        p.asin &&
        !excludeSet.has(p.asin) &&
        !isVero(p.title, '')
        // note: title may be empty — that's OK, we'll get it when we scrape
      );

      // Shuffle randomly
      for (let i = allProducts.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [allProducts[i], allProducts[j]] = [allProducts[j], allProducts[i]];
      }

      const selected = allProducts.slice(0, count);
      console.log(`[dealsScrape] total=${Object.keys(productMap).length} after filter=${allProducts.length} selected=${selected.length}`);
      return res.json({ success: true, products: selected, totalFound: Object.keys(productMap).length });
    }

    if (action === 'optimizeTitle') {
      const { title, breadcrumbs = [], aspects = {} } = body;
      if (!title) return res.status(400).json({ error: 'title required' });
      const key = process.env.ANTHROPIC_API_KEY;
      if (!key) return res.status(500).json({ error: 'No ANTHROPIC_API_KEY set' });
      try {
        const r = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01' },
          body: JSON.stringify({
            model: 'claude-sonnet-4-20250514', max_tokens: 200,
            messages: [{ role: 'user', content: `You are an eBay listing title optimizer.

Original Amazon title: "${title}"
Category path: ${breadcrumbs.join(' > ') || 'unknown'}

Generate a clean, optimized eBay listing title following these rules:
- Exactly 10-12 words
- Remove ALL brand names, seller names, and store names completely
- Start with the product type (e.g. "Chunky Knit Throw Blanket", "Women's V-Neck T-Shirt")
- Include key descriptors: material, style, color range hint, use case
- No special characters, no colons, no quotes, no dimensions in title
- No ALL CAPS words, no filler words
- NEVER use: authentic, genuine, original, real, verified, certified — eBay bans these
- Title case formatting

Return ONLY the optimized title text, nothing else. No quotes, no explanation.` }]
          })
        });
        const d = await r.json();
        const raw = (d.content?.[0]?.text || '').trim().replace(/^["']|["']$/g, '');
        const optimized = sanitizeTitle(raw);
        console.log(`[optimizeTitle] raw="${raw}" cleaned="${optimized}" apiErr=${JSON.stringify(d.error)}`);
        if (!optimized) {
          // AI returned empty — fall back to cleaning up the raw Amazon title
          const fallback = sanitizeTitle(title.replace(/[^\w\s\-&]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 80));
          return res.json({ success: true, optimizedTitle: fallback, fallback: true });
        }
        return res.json({ success: true, optimizedTitle: optimized });
      } catch (e) {
        return res.status(500).json({ error: e.message });
      }
    }

    if (action === 'endListing') {
      const { access_token, ebaySku, ebayListingId } = body;
      const sandbox = body.sandbox === true || body.sandbox === 'true';
      const EBAY_API = getEbayUrls(sandbox).EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
      let ended = 0;
      const variantSkus = [];

      // Strategy A: get group to find all variant SKUs
      const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      if (groupRes.variantSKUs?.length) variantSkus.push(...groupRes.variantSKUs);
      console.log(`[end] group=${ebaySku} variantSkus=${variantSkus.length}`);

      // Strategy B: if group returned no SKUs, page through all offers and find ones
      // belonging to this listing via ebayListingId
      if (!variantSkus.length) {
        console.log('[end] group empty — scanning offers by listing ID:', ebayListingId);
        let offset = 0;
        while (true) {
          const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
          const offers = ol.offers || [];
          if (!offers.length) break;
          for (const o of offers) {
            // Match by listing ID or by SKU prefix
            const matchesListing = ebayListingId && o.listing?.listingId === String(ebayListingId);
            const matchesSku = o.sku?.startsWith(ebaySku.split('-').slice(0,3).join('-'));
            if (matchesListing || matchesSku) {
              if (!variantSkus.includes(o.sku)) variantSkus.push(o.sku);
            }
          }
          if (offers.length < 100) break;
          offset += 100;
        }
        console.log(`[end] scan found ${variantSkus.length} variant skus`);
      }

      // Withdraw + delete all offers for each variant SKU
      const allSkus = variantSkus.length ? variantSkus : [ebaySku];
      for (const sku of allSkus) {
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        for (const offer of (ol.offers||[])) {
          if (offer.status === 'PUBLISHED') {
            const wr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}/withdraw`, { method: 'POST', headers: auth });
            const wb = await wr.json().catch(()=>({}));
            console.log(`[end] withdraw offer ${offer.offerId}:`, wr.status, JSON.stringify(wb).slice(0,100));
            if (wr.status < 300) ended++;
          }
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, { method: 'DELETE', headers: auth }).catch(()=>{});
        }
      }

      // Delete inventory item group
      await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { method: 'DELETE', headers: auth }).catch(()=>{});

      // Delete all variant inventory items in batches of 25
      for (let i = 0; i < variantSkus.length; i += 25) {
        const batch = variantSkus.slice(i, i+25);
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?sku=${batch.map(encodeURIComponent).join(',')}`, { method: 'DELETE', headers: auth }).catch(()=>{});
      }

      console.log(`[end] done ended=${ended} deletedItems=${variantSkus.length}`);
      return res.json({ success: true, ended, deleted: variantSkus.length });
    }

    return res.status(400).json({ error: `Unknown action: ${action}` });

  } catch (err) {
    console.error('[Error]', err.message);
    return res.status(500).json({ error: err.message });
  }
};
