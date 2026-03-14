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

// ── Shared Amazon scraper — called by both scrape and revise actions ───────────
// Returns the full product object (same shape as scrape action), or null on failure.
async function scrapeAmazonProduct(inputUrl) {
  let url = (inputUrl || '').trim();
  if (!url) return null;

  // Normalize to clean dp/ASIN URL
  const asinM = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
  const asinP = url.match(/[?&]asin=([A-Z0-9]{10})/i);
  const asin  = asinM?.[1] || asinP?.[1];
  if (asin) url = `https://www.amazon.com/dp/${asin}?th=1`;

  const ua = randUA();
  let html = await fetchPage(url, ua);
  if (!html && asin) { await sleep(1500); html = await fetchPage(`https://www.amazon.com/dp/${asin}?psc=1`, ua); }
  if (!html && asin) { await sleep(2000); html = await fetchPage(`https://www.amazon.com/product/dp/${asin}`, ua); }
  if (!html) return null;

  const product = {
    url, source: 'amazon', asin: asin || '',
    title: '', price: 0, images: [],
    description: '', aspects: {}, breadcrumbs: [],
    variations: [], variationImages: {}, hasVariations: false,
    inStock: true, quantity: 1,
    bullets: [], descriptionPara: '',
    comboAsin: {}, sizePrices: {}, comboPrices: {},
  };

  // Title
  const tM = html.match(/id="productTitle"[^>]*>\s*([^<]{5,})/);
  if (tM) product.title = tM[1].trim().replace(/\s+/g,' ');

  // Breadcrumbs
  const bcRaw = [...html.matchAll(/class="a-link-normal"[^>]*>\s*([^<]{2,40})\s*<\/a>/g)];
  product.breadcrumbs = bcRaw.slice(0, 6).map(m => m[1].trim()).filter(s => s.length > 1 && !/^\d+$/.test(s));

  // Price & stock
  product.price = extractPrice(html) || 0;
  product.inStock = !html.toLowerCase().includes('currently unavailable');

  // Images
  const imgs = [...html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)]
    .map(m => m[1]).filter((v,i,a) => a.indexOf(v)===i);
  product.images = imgs.slice(0, 12);

  // Bullets
  const extractBullets = (h) => {
    const results = [];
    const sectionPatterns = [
      /id="productFactsDesktopExpander"[^>]*>([\s\S]{100,8000}?)<\/div>\s*<\/div>\s*<\/div>/,
      /id="featurebullets_feature_div"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
      /id="feature-bullets"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
    ];
    let sectionHtml = '';
    for (const pat of sectionPatterns) { const m = h.match(pat); if (m) { sectionHtml = m[1]; break; } }
    if (sectionHtml) {
      for (const m of [...sectionHtml.matchAll(/<li[^>]*>[\s\S]*?<span[^>]*class="[^"]*a-list-item[^"]*"[^>]*>([\s\S]{15,600}?)<\/span>/g)]) {
        const text = m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#\d+;/g,'').trim();
        if (text.length > 15 && !text.includes('Your Orders')) results.push(text);
      }
    }
    if (!results.length) {
      for (const m of [...h.matchAll(/<li[^>]*>\s*<span[^>]*a-list-item[^"]*"[^>]*>([\s\S]{20,600}?)<\/span>/g)]) {
        const text = m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim();
        if (text.length > 20 && !text.includes('Your Orders') && !text.includes('Drop off')) { results.push(text); }
        if (results.length >= 8) break;
      }
    }
    return results.slice(0, 8);
  };
  product.bullets = extractBullets(html);

  const descParaM = html.match(/id="productDescription"[^>]*>[\s\S]{0,200}<p[^>]*>([\s\S]{30,1000}?)<\/p>/);
  product.descriptionPara = descParaM ? descParaM[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim() : '';

  // Build eBay description
  const buildEbayDesc = (title, buls, para, aspects) => {
    const bulletHtml = buls.length ? '<ul>' + buls.map(b => `<li>${b.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>' : '';
    const specRows = Object.entries(aspects||{}).filter(([k,v])=>!['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k)&&v[0]&&v[0].length<80).slice(0,10).map(([k,v])=>`<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
    const specsTable = specRows ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>` : '';
    return [`<h2>${title}</h2>`,bulletHtml,para?`<p>${para}</p>`:'',specsTable,'<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>'].filter(Boolean).join('\n');
  };
  product.description = buildEbayDesc(product.title, product.bullets, product.descriptionPara, product.aspects) || product.title;

  // Item specifics
  for (const [,k,v] of [...html.matchAll(/<th[^>]*class="[^"]*prodDetSectionEntry[^"]*"[^>]*>([^<]+)<\/th>\s*<td[^>]*>([^<]+)<\/td>/gi)]) {
    const key = k.trim(), val = v.trim().replace(/\s+/g,' ');
    if (key && val && val.length < 120 && !val.includes('›')) product.aspects[key] = [val];
  }
  for (const [,k,v] of [...html.matchAll(/(Brand|Material|Color|Size|Style|Weight|Dimensions?)\s*:\s*([^\n<]{2,60})/g)]) {
    if (!product.aspects[k]) product.aspects[k] = [v.trim()];
  }

  // Variations
  const swatchImgMap = extractSwatchImages(html);
  let varVals = null;
  const vvM = html.match(/"variationValues"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
  if (vvM) try { varVals = JSON.parse(vvM[1]); } catch {}
  const hasVar = !!(varVals && (varVals.color_name?.length || varVals.size_name?.length));
  product.hasVariations = hasVar;

  if (hasVar) {
    const colors = varVals.color_name || [];
    const sizes  = varVals.size_name  || [];
    const dtaBlock = extractBlock(html, '"dimensionToAsinMap"');
    let dtaMap = {};
    try { dtaMap = JSON.parse(dtaBlock); } catch {}
    let dimOrder = null;
    const dimM = html.match(/"dimensions"\s*:\s*(\[[^\]]{0,200}\])/s);
    if (dimM) try { dimOrder = JSON.parse(dimM[1]); } catch {}
    if (!dimOrder) { const vvKeys = Object.keys(varVals||{}); dimOrder = vvKeys.length ? vvKeys : ['color_name','size_name']; }
    const colorDimIdx = dimOrder.indexOf('color_name');
    const sizeDimIdx  = dimOrder.indexOf('size_name');
    const effectiveColorIdx = colorDimIdx >= 0 ? colorDimIdx : 0;
    const effectiveSizeIdx  = sizeDimIdx  >= 0 ? sizeDimIdx  : 1;

    const comboAsin = {}, colorSizeMap = {};
    for (const [code, asin] of Object.entries(dtaMap)) {
      const parts = code.split('_').map(Number);
      if (parts.length < 1) continue;
      const ci = parts[effectiveColorIdx], si = parts[effectiveSizeIdx];
      const color = colors[ci], size = (si !== undefined && sizes[si]) ? sizes[si] : (sizes[0] || '');
      if (!color) continue;
      comboAsin[`${color}|${size}`] = asin;
      if (!colorSizeMap[color]) colorSizeMap[color] = {};
      colorSizeMap[color][size] = asin;
    }
    if (!Object.keys(comboAsin).length) {
      const { colorToAsin: ctaMap } = extractColorAsinMaps(html);
      for (const [c, asin] of Object.entries(ctaMap)) { comboAsin[`${c}|`] = asin; colorSizeMap[c] = { '': asin }; }
    }

    const fullColor = colors.find(c => sizes.every(s => colorSizeMap[c]?.[s]))
                   || colors.find(c => Object.keys(colorSizeMap[c]||{}).length >= sizes.length - 1)
                   || colors[0];
    const sizeToFetchAsin = {};
    for (const size of sizes) {
      const a = colorSizeMap[fullColor]?.[size] || Object.values(colorSizeMap).map(m=>m[size]).find(Boolean);
      if (a) sizeToFetchAsin[size] = a;
    }
    if (!sizes.length && colors[0]) sizeToFetchAsin[''] = Object.values(colorSizeMap[colors[0]]||{})[0] || '';

    const sizePrices = {};
    const asinInStock = {};  // ASIN → boolean (in stock on Amazon)
    await Promise.all(Object.entries(sizeToFetchAsin).map(async ([size, a]) => {
      if (!a) return;
      const h = await fetchPage(`https://www.amazon.com/dp/${a}`, ua);
      if (!h) return;
      const p = extractPrice(h);
      asinInStock[a] = !h.toLowerCase().includes('currently unavailable');
      if (p) { sizePrices[size] = p; console.log(`[price] "${size}" = $${p} inStock=${asinInStock[a]}`); }
    }));

    const asinToPrice = {};
    for (const [size, a] of Object.entries(sizeToFetchAsin)) { if (sizePrices[size] && a) asinToPrice[a] = sizePrices[size]; }
    const uniqueAsins = [...new Set(Object.values(comboAsin))].filter(a => !asinToPrice[a]);
    for (let i = 0; i < uniqueAsins.length; i += 5) {
      await Promise.all(uniqueAsins.slice(i,i+5).map(async a => {
        const h = await fetchPage(`https://www.amazon.com/dp/${a}`, ua);
        if (!h) return;
        const p = extractPrice(h);
        asinInStock[a] = !h.toLowerCase().includes('currently unavailable');
        if (p) asinToPrice[a] = p;
      }));
      if (i + 5 < uniqueAsins.length) await sleep(300);
    }
    const comboPrices = {};
    const comboInStock = {};  // "Color|Size" → boolean
    for (const [key, a] of Object.entries(comboAsin)) {
      comboPrices[key] = asinToPrice[a] || sizePrices[key.split('|')[1]] || 0;
      comboInStock[key] = asinInStock[a] !== false;  // default true if we couldn't check
    }

    const mainPrice = product.price || 0;
    if (!Object.keys(sizePrices).length && mainPrice) { sizes.forEach(s => { sizePrices[s] = mainPrice; }); if (!sizes.length) sizePrices[''] = mainPrice; }
    sizes.forEach(s => { if (!sizePrices[s]) sizePrices[s] = mainPrice; });
    if (!sizes.length && !sizePrices['']) sizePrices[''] = mainPrice;

    const colorData = {};
    for (const c of colors) colorData[c] = { image: swatchImgMap[c] || '' };
    await Promise.all(colors.filter(c=>!colorData[c].image).slice(0,6).map(async c => {
      const a = Object.values(colorSizeMap[c]||{})[0];
      if (!a) return;
      const h = await fetchPage(`https://www.amazon.com/dp/${a}`, ua);
      if (h) { const img = extractMainImage(h); if (img) colorData[c].image = img; }
    }));
    for (const c of colors) { const img = colorData[c].image; if (img && !product.images.includes(img)) product.images.push(img); }
    let fi = 0; colors.filter(c=>!colorData[c].image).forEach(c => { colorData[c].image = product.images[fi++ % Math.max(1,product.images.length)] || ''; });

    if (colors.length) {
      product.variations.push({ name:'Color', values: colors.map(c => {
        const inStock = sizes.length
          ? sizes.some(s => comboInStock[`${c}|${s}`] !== false && comboAsin[`${c}|${s}`])
          : (comboInStock[`${c}|`] !== false && !!comboAsin[`${c}|`]);
        const availPrices = sizes.filter(s=>comboAsin[`${c}|${s}`]&&sizePrices[s]).map(s=>sizePrices[s]);
        const colorPrice = availPrices.length ? Math.min(...availPrices) : (sizePrices[sizes[0]]||mainPrice);
        return { value:c, price:colorPrice, image:colorData[c].image||'', inStock:Object.keys(comboAsin).length>0?inStock:true, enabled:Object.keys(comboAsin).length>0?inStock:true };
      })});
      product.variationImages['Color'] = Object.fromEntries(colors.map(c=>[c,colorData[c].image]).filter(([,img])=>img));
    }
    if (sizes.length) {
      product.variations.push({ name:'Size', values: sizes.map(s => {
        const inStock = colors.some(c => comboAsin[`${c}|${s}`] && comboInStock[`${c}|${s}`] !== false);
        return { value:s, price:sizePrices[s]||mainPrice, inStock:Object.keys(comboAsin).length>0?inStock:true, enabled:Object.keys(comboAsin).length>0?inStock:true, image:'' };
      })});
    }
    product.comboAsin   = comboAsin;
    product.comboInStock = comboInStock;
    product.sizePrices = sizePrices;
    product.comboPrices = comboPrices;
    const allP = Object.values(sizePrices).filter(p=>p>0);
    if (allP.length) product.price = Math.min(...allP);
  }

  console.log(`[scrapeAmazonProduct] "${product.title?.slice(0,50)}" price=$${product.price} imgs=${product.images.length} hasVar=${product.hasVariations}`);
  return product;
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
            setTimeout(function(){ window.location.href = '/'; }, 800);
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
        inStock: true, quantity: 1,
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
        const asinInStock  = {};  // ASIN → boolean (in stock on Amazon)
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
            asinInStock[asin] = !h.toLowerCase().includes('currently unavailable');
            if (p) { sizePrices[size] = p; console.log(`[price] "${size}" = $${p} (${asin}) inStock=${asinInStock[asin]}`); }
            else     console.log(`[price] "${size}" FAILED (${asin})`);
          })
        );

        // 5b. Build comboPrices from size prices (fast — no extra ASIN fetches during sync)
        //     Per-size fetch above already tells us price + stock for each size across all colors.
        //     Unique-ASIN batch fetch is skipped here to stay within Vercel time limits.
        const asinToPrice = {};
        for (const [size, asin] of Object.entries(sizeToFetchAsin)) {
          if (sizePrices[size] && asin) asinToPrice[asin] = sizePrices[size];
        }
        // comboPrices + comboInStock: "Color|Size" — use size-level stock for all colors
        const comboPrices = {};
        const comboInStock = {};
        // Build sizeInStock from the per-size ASIN fetches
        const sizeInStock = {}; // size → boolean
        for (const [size, asin] of Object.entries(sizeToFetchAsin)) {
          sizeInStock[size] = asinInStock[asin] !== false;
        }
        for (const [key, asin] of Object.entries(comboAsin)) {
          const size = key.split('|')[1] || '';
          comboPrices[key]  = asinToPrice[asin] || sizePrices[size] || 0;
          // If we have explicit ASIN stock data use it, otherwise fall back to size-level
          comboInStock[key] = asinInStock[asin] !== undefined
            ? asinInStock[asin]
            : (sizeInStock[size] !== undefined ? sizeInStock[size] : true);
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
                ? sizes.some(s => comboAsin[`${c}|${s}`] && comboInStock[`${c}|${s}`] !== false)
                : (!!comboAsin[`${c}|`] && comboInStock[`${c}|`] !== false);
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
              const inStock = colors.some(c => comboAsin[`${c}|${s}`] && comboInStock[`${c}|${s}`] !== false);
              return {
                value: s,
                price: sizePrices[s] || mainPrice,
                inStock: Object.keys(comboAsin).length > 0 ? inStock : true,
                enabled: Object.keys(comboAsin).length > 0 ? inStock : true,
                image: '',
              };
            }),
          });
        }

        // Store on product for push step
        product.comboAsin   = comboAsin;
        product.comboInStock = comboInStock;
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

      // Build colorAsinMap for worker: color name → ASIN
      // This lets the Railway worker fetch per-color prices independently
      const colorAsinMap = {};
      if (colorGrp?.values?.length) {
        for (const cv of colorGrp.values) {
          if (cv.asin) colorAsinMap[cv.value] = cv.asin;
        }
      }
      // Also pull from comboAsin (color|size → asin)
      if (product.comboAsin) {
        for (const [key, asin] of Object.entries(product.comboAsin)) {
          const color = key.split('|')[0];
          if (color && asin && !colorAsinMap[color]) colorAsinMap[color] = asin;
        }
      }
      if (Object.keys(colorAsinMap).length) product.colorAsinMap = colorAsinMap;

      if (!product.title) {
        return res.json({ success: false, error: 'Amazon blocked the request (bot detection). Wait 30 seconds and try again, or paste the URL directly.' });
      }
      return res.json({ success: true, product, _debug: { pricesFound, imagesFound, totalColors: colorGrp?.values.length||0, colorAsinMapSize: Object.keys(colorAsinMap).length } });
    }

    // ── PUSH: create eBay listing ─────────────────────────────────────────────
    if (action === 'push') {
      const { access_token, product, fulfillmentPolicyId, paymentPolicyId, returnPolicyId } = body;
      if (!access_token || !product) return res.status(400).json({ error: 'Missing access_token or product' });

      const sandbox = body.sandbox === true || body.sandbox === 'true';
      const EBAY_API = getEbayUrls(sandbox).EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };
      console.log(`[push] "${product.title?.slice(0,60)}" hasVariations=${product.hasVariations} images=${product.images?.length||0} price=${product.price}`);

      // Guard: must have at least 1 image
      if (!product.images?.length) {
        return res.status(400).json({ error: 'No images found for this product. Try re-importing it from the Import tab to refresh the images.' });
      }
      // Guard: must have a price
      if (!product.hasVariations && !product.price && !product.cost && !product.myPrice) {
        return res.status(400).json({ error: 'No price found for this product. Try re-importing it from the Import tab to get the current price.' });
      }

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
      // basePrice: scraped price → stored cost → stored myPrice → 0
      const basePrice = parseFloat(product.price || product.cost || product.myPrice || 0);
      const groupSku  = `DS-${Date.now()}-${Math.random().toString(36).slice(2,7).toUpperCase()}`;

      // ── SIMPLE LISTING ─────────────────────────────────────────────────────
      if (!product.hasVariations || !product.variations?.length) {
        const simpleMarkupPct = parseFloat(body.markup ?? product.markup ?? 0);
        const simpleHandling  = parseFloat(body.handlingCost ?? product.handlingCost ?? 2);
        const simpleEbayFee   = 0.1335;
        // Price priority: product.price (freshly scraped) → product.cost → product.myPrice (pre-calculated)
        const simpleCost = parseFloat(product.price || product.cost || 0);
        let simplePrice;
        if (simpleCost > 0) {
          simplePrice = (Math.ceil(((simpleCost + simpleHandling) * (1 + simpleMarkupPct / 100) / (1 - simpleEbayFee) + 0.30) * 100) / 100).toFixed(2);
        } else if (parseFloat(product.myPrice) > 0) {
          simplePrice = parseFloat(product.myPrice).toFixed(2);
        } else {
          simplePrice = '9.99'; // safe fallback — never $0
        }
        // eBay price floor
        if (parseFloat(simplePrice) < 0.99) simplePrice = '0.99';
        console.log(`[push/simple] cost=$${simpleCost} myPrice=$${product.myPrice} markup=${simpleMarkupPct}% handling=$${simpleHandling} → price=$${simplePrice}`);

        const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(groupSku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: parseInt(product.quantity)||1 } },
            condition: 'NEW',
            product: { title: listingTitle, description: ebayDescription, imageUrls: product.images.slice(0,12), aspects },
          }),
        });
        if (!ir.ok) return res.status(400).json({ error: 'Inventory PUT failed', details: await ir.text() });

        const or = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, {
          method: 'POST', headers: auth,
          body: JSON.stringify(buildOffer(groupSku, simplePrice, categoryId, policies, locationKey)),
        });
        const od = await or.json();
        if (!or.ok) return res.status(400).json({ error: 'Offer failed', details: od });

        const pr  = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${od.offerId}/publish`, { method: 'POST', headers: auth });
        const pd  = await pr.json();
        if (!pd.listingId) {
          const firstErr = (pd.errors||[])[0];
          const errId = firstErr?.errorId;
          if (errId === 25002) {
            const existing = firstErr?.parameters?.find(p => p.name === 'listingId')?.value
              || firstErr?.message?.match(/\((\d{12,})\)/)?.[1];
            return res.status(400).json({
              error: `Duplicate listing — already live as eBay item ${existing || '(see eBay)'}. Delete the old listing first or use Sync to update it.`,
              errorId: 25002, existingListingId: existing || null,
            });
          }
          return res.status(400).json({ error: 'Simple publish failed', details: pd, errorId: errId });
        }
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

      const defaultQty = parseInt(product.quantity) || 1;
      // comboAsin:  "Color|Size" → ASIN  (only combos that exist on Amazon)
      // sizePrices: sizeName → price (fetched from best-coverage color, applies to all colors at that size)
      const comboAsin   = product.comboAsin   || {};
      const comboInStock = product.comboInStock || {};
      const sizePrices  = product.sizePrices  || {};
      const comboPrices = product.comboPrices || {}; // "Color|Size" → amazon price
      // Use markup/handling from body (frontend settings), fallback to product, then defaults
      const markupPct   = parseFloat(body.markup ?? product.markup ?? 0);
      const handling    = parseFloat(body.handlingCost ?? product.handlingCost ?? 2);
      const ebayFee     = 0.1335;
      const applyMarkup = (cost) => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return 0; // will be caught by floor below
        const result = Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - ebayFee) + 0.30) * 100) / 100;
        return Math.max(result, 0.99); // eBay price floor
      };
      console.log(`[push] comboAsin entries=${Object.keys(comboAsin).length} comboPrices entries=${Object.keys(comboPrices).length} markup=${markupPct}%`);

      if (colorGroup && sizeGroup) {
        for (const cv of colorGroup.values) {
          for (const sv of sizeGroup.values) {
            const key = `${cv.value}|${sv.value}`;
            // hasCombo: combo must exist on Amazon (comboAsin) AND be in stock (comboInStock)
            // enabled flag is for UI display only — don't use it for qty determination
            const hasCombo = Object.keys(comboAsin).length === 0 || (!!comboAsin[key] && comboInStock[key] !== false);
            // Use comboPrices (per-combo) → sizePrices (per-size) → variant price → basePrice → myPrice fallback
            const amazonPrice = comboPrices[key]
                             || sizePrices[sv.value]
                             || parseFloat(sv.price || cv.price || basePrice || product.myPrice || 0);
            const calcedPrice = applyMarkup(amazonPrice);
            const price = (calcedPrice > 0 ? calcedPrice : parseFloat(product.myPrice || 9.99)).toFixed(2);
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
        for (const cv of colorGroup.values) {
          const key = `${cv.value}|`;
          const hasCombo = Object.keys(comboAsin).length === 0 || (!!comboAsin[key] && comboInStock[key] !== false);
          const amazonPrice = comboPrices[key] || parseFloat(cv.price || basePrice || product.myPrice || 0);
          const calcedPriceC = applyMarkup(amazonPrice);
          variants.push({
            sku:   mkSku([cv.value]), color: cv.value, size: null,
            price: (calcedPriceC > 0 ? calcedPriceC : parseFloat(product.myPrice || 9.99)).toFixed(2),
            image: colorImgs[cv.value] || product.images[0] || '',
            qty:   hasCombo ? defaultQty : 0,
            inStock: hasCombo,
          });
        }
      } else if (sizeGroup) {
        for (const sv of sizeGroup.values.filter(v => v.enabled !== false)) {
          const key = `|${sv.value}`;
          const amazonPrice = comboPrices[key] || sizePrices[sv.value] || parseFloat(sv.price || basePrice || product.myPrice || 0);
          const calcedPriceS = applyMarkup(amazonPrice);
          variants.push({
            sku:   mkSku([sv.value]), color: null, size: sv.value,
            price: (calcedPriceS > 0 ? calcedPriceS : parseFloat(product.myPrice || 9.99)).toFixed(2),
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
      if (colorGroup) varAspects['Color'] = colorGroup.values.map(v => v.value); // all values — OOS handled via qty=0
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
        const firstErr = (pd.errors||[])[0];
        const errId = firstErr?.errorId;
        console.warn(`[push] publish attempt ${attempt}: ${JSON.stringify(pd).slice(0,300)}`);
        // Duplicate listing detected — return the existing listing ID
        if (errId === 25002) {
          const existing = firstErr?.parameters?.find(p => p.name === 'listingId')?.value
            || firstErr?.message?.match(/\((\d{12,})\)/)?.[1];
          return res.status(400).json({
            error: `Duplicate listing — already live as eBay item ${existing || '(see eBay)'}. Delete the old listing first or use Sync to update it.`,
            errorId: 25002, existingListingId: existing || null,
          });
        }
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
      const EBAY_API = getEbayUrls(false).EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };

      const allListings = [];
      const seenIds = new Set();

      // ── Strategy 1: Inventory API offers (catches DropSync-created listings) ──
      try {
        let offset = 0;
        const limit = 100;
        while (true) {
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=${limit}&offset=${offset}`, { headers: auth });
          const d = await r.json();
          if (!r.ok) { console.log('[fetchMyListings] inventory offers err:', d?.errors?.[0]?.message); break; }
          for (const o of (d.offers || [])) {
            if (!o.listingId || seenIds.has(o.listingId)) continue;
            seenIds.add(o.listingId);
            allListings.push({
              ebayListingId: o.listingId,
              ebaySku:       o.sku || '',
              title:         o.sku || '',
              price:         parseFloat(o.pricingSummary?.price?.value || 0),
              quantity:      o.availableQuantity || 0,
              image:         '',
              ebayUrl:       `https://www.ebay.com/itm/${o.listingId}`,
              aspects:       {},
            });
          }
          const total = d.total || 0;
          offset += limit;
          if (offset >= total || (d.offers||[]).length < limit) break;
        }
        console.log(`[fetchMyListings] inventory offers: ${allListings.length}`);
      } catch(e) { console.log('[fetchMyListings] inventory err:', e.message); }

      // ── Strategy 2: Trading API GetMyeBaySelling (catches ALL listings) ──────
      try {
        const TRADING_API = 'https://api.ebay.com/ws/api.dll';
        let pageNum = 1, totalPages = 1;
        while (pageNum <= totalPages && pageNum <= 20) {
          const xmlBody = `<?xml version="1.0" encoding="utf-8"?><GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents"><RequesterCredentials><eBayAuthToken>${access_token}</eBayAuthToken></RequesterCredentials><ActiveList><Include>true</Include><IncludeItemSpecifics>true</IncludeItemSpecifics><Pagination><EntriesPerPage>200</EntriesPerPage><PageNumber>${pageNum}</PageNumber></Pagination></ActiveList><ErrorLanguage>en_US</ErrorLanguage><WarningLevel>High</WarningLevel></GetMyeBaySellingRequest>`;
          const r = await fetch(TRADING_API, {
            method: 'POST',
            headers: {
              'Content-Type': 'text/xml',
              'X-EBAY-API-SITEID': '0',
              'X-EBAY-API-COMPATIBILITY-LEVEL': '1155',
              'X-EBAY-API-CALL-NAME': 'GetMyeBaySelling',
              'X-EBAY-API-IAF-TOKEN': access_token,
            },
            body: xmlBody,
          });
          const xml = await r.text();
          console.log(`[fetchMyListings] trading page=${pageNum} status=${r.status} len=${xml.length}`);
          if (!r.ok) break;

          // Check for eBay error in XML
          const ackMatch = xml.match(/<Ack>(.*?)<\/Ack>/);
          const ack = ackMatch?.[1] || '';
          if (ack === 'Failure') {
            const errMsg = xml.match(/<LongMessage>(.*?)<\/LongMessage>/)?.[1] || xml.slice(0,300);
            console.log('[fetchMyListings] trading API failure:', errMsg);
            break;
          }

          const tpMatch = xml.match(/<TotalNumberOfPages>(\d+)<\/TotalNumberOfPages>/);
          if (tpMatch) totalPages = Math.min(parseInt(tpMatch[1]), 20);

          // Parse items
          const itemRegex = /<Item>([\s\S]*?)<\/Item>/g;
          let count = 0;
          for (const m of xml.matchAll(itemRegex)) {
            const block = m[1];
            const get = tag => block.match(new RegExp(`<${tag}>([\s\S]*?)<\/${tag}>`))?.[1]?.trim() || '';
            const itemId = get('ItemID');
            if (!itemId || seenIds.has(itemId)) continue;
            seenIds.add(itemId);
            count++;
            allListings.push({
              ebayListingId: itemId,
              ebaySku:       get('SKU'),
              title:         get('Title'),
              price:         parseFloat(get('CurrentPrice') || get('BuyItNowPrice') || '0'),
              quantity:      parseInt(get('QuantityAvailable') || get('Quantity') || '0'),
              image:         get('GalleryURL') || get('PictureURL') || '',
              ebayUrl:       `https://www.ebay.com/itm/${itemId}`,
              aspects:       {},
            });
          }
          console.log(`[fetchMyListings] trading page ${pageNum}/${totalPages}: +${count} items`);
          pageNum++;
        }
      } catch(e) { console.log('[fetchMyListings] trading err:', e.message); }

      // ── Enrich titles/images via inventory items for DropSync SKUs ────────────
      const dsItems = allListings.filter(l => l.ebaySku?.startsWith('DS-'));
      if (dsItems.length) {
        const skus = [...new Set(dsItems.map(l => l.ebaySku))].slice(0, 100);
        for (let i = 0; i < skus.length; i += 25) {
          try {
            const batch = skus.slice(i, i+25);
            const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?sku=${batch.join('|')}`, { headers: auth });
            const id = await ir.json();
            for (const item of (id.inventoryItems || [])) {
              const l = allListings.find(x => x.ebaySku === item.sku);
              if (l) {
                if (!l.title && item.product?.title) l.title = item.product.title;
                if (!l.image && item.product?.imageUrls?.[0]) l.image = item.product.imageUrls[0];
              }
            }
          } catch {}
        }
      }

      console.log(`[fetchMyListings] TOTAL: ${allListings.length} listings`);
      return res.json({ success: true, listings: allListings, total: allListings.length });
    }


    // ── FETCH MY LISTINGS DEBUG — returns raw XML ─────────────────────────────
    if (action === 'fetchMyListingsDebug') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const TRADING_API = 'https://api.ebay.com/ws/api.dll';
      const xmlBody = `<?xml version="1.0" encoding="utf-8"?>
<GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials><eBayAuthToken>${access_token}</eBayAuthToken></RequesterCredentials>
  <ActiveList><Include>true</Include><Pagination><EntriesPerPage>10</EntriesPerPage><PageNumber>1</PageNumber></Pagination></ActiveList>
  <ErrorLanguage>en_US</ErrorLanguage><WarningLevel>High</WarningLevel>
</GetMyeBaySellingRequest>`;
      const r = await fetch(TRADING_API, {
        method: 'POST',
        headers: {
          'Content-Type': 'text/xml',
          'X-EBAY-API-SITEID': '0',
          'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
          'X-EBAY-API-CALL-NAME': 'GetMyeBaySelling',
          'X-EBAY-API-IAF-TOKEN': access_token,
        },
        body: xmlBody,
      });
      const xmlText = await r.text();
      console.log('[fetchMyListingsDebug] status:', r.status, 'len:', xmlText.length);
      return res.json({ status: r.status, xml: xmlText.slice(0, 5000) });
    }



    // ── REVISE: update live listing images/price/stock from fresh Amazon scrape ─
    // Does NOT delete or re-publish — revises the inventory items in place
    if (action === 'revise') {
      // ── REVISE: Full in-place update of a live eBay listing ──────────────────
      // Scrapes Amazon fresh, then does a complete wipe-and-replace of:
      //   title, description, all images, stock, price
      // on every existing inventory item + the group.
      // The listing stays live throughout — NO delete/republish.
      const { access_token, ebaySku, sourceUrl } = body;
      if (!access_token || !ebaySku || !sourceUrl) {
        return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });
      }
      const sandbox    = body.sandbox === true || body.sandbox === 'true';
      const EBAY_API   = getEbayUrls(sandbox).EBAY_API;
      const markupPct  = parseFloat(body.markup ?? 0);
      const handling   = parseFloat(body.handlingCost ?? 2);
      const ebayFee    = 0.1335;
      const defaultQty = parseInt(body.quantity) || 1;

      // Same price formula as push
      const applyMk = (cost) => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return 0;
        return Math.max(Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - ebayFee) + 0.30) * 100) / 100, 0.99);
      };

      const auth = {
        Authorization:    `Bearer ${access_token}`,
        'Content-Type':   'application/json',
        'Content-Language': 'en-US',
        'Accept-Language':  'en-US',
      };
      console.log(`[revise] sku=${ebaySku?.slice(0,30)} url=${sourceUrl?.slice(0,60)} markup=${markupPct}%`);

      // ── STEP 1: Full Amazon scrape via self-call ──────────────────────────────
      const baseUrl = process.env.VERCEL_URL
        ? `https://${process.env.VERCEL_URL}`
        : 'https://dropsync-one.vercel.app';
      const scrapeR = await fetch(`${baseUrl}/api/ebay?action=scrape`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: sourceUrl }),
      }).catch(e => { console.warn('[revise] scrape failed:', e.message); return null; });
      const scrapeD = scrapeR?.ok ? await scrapeR.json().catch(() => null) : null;
      let product = scrapeD?.product || null;

      // Fallback: if scrape failed or returned no images, use cached data from frontend
      const fallbackImages  = Array.isArray(body.fallbackImages) ? body.fallbackImages.filter(u => typeof u === 'string' && u.startsWith('http')) : [];
      const fallbackTitle   = typeof body.fallbackTitle === 'string' ? body.fallbackTitle : '';
      const fallbackPrice   = parseFloat(body.fallbackPrice) || 0;
      const fallbackInStock = body.fallbackInStock !== false;

      if (!product && fallbackImages.length) {
        console.log(`[revise] scrape failed — using ${fallbackImages.length} cached fallback images`);
        product = {
          title: fallbackTitle || 'Product', price: fallbackPrice, images: fallbackImages,
          inStock: fallbackInStock, hasVariations: false, variations: [], variationImages: {},
          comboPrices: {}, sizePrices: {}, aspects: {}, breadcrumbs: [], bullets: [], descriptionPara: '',
        };
      } else if (product && !product.images?.length && fallbackImages.length) {
        console.log(`[revise] no images in scrape — using ${fallbackImages.length} cached fallback images`);
        product.images = fallbackImages;
      }

      if (!product || !product.images?.length) {
        // Last resort: pull existing images + stock from the live eBay listing
        console.log('[revise] no images from Amazon or cache — fetching from eBay inventory');
        try {
          // First check if this is a variation listing (has an inventory item group)
          const grpR = await fetch(
            `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`,
            { headers: auth }
          ).then(r => r.ok ? r.json() : null).catch(() => null);
          const existingVarSkus = grpR?.variantSKUs || [];

          if (existingVarSkus.length) {
            // Variation listing — fetch each variant's current qty + images from eBay
            console.log(`[revise] variation fallback: ${existingVarSkus.length} variants`);
            const allImgs = [];
            const comboAsin = {};    // we won't have ASINs but we need keys for getQtyForVariant
            const comboInStock = {}; // key → boolean from current eBay qty
            const skuQtys = {};      // sku → qty

            // Batch fetch all variant inventory items
            for (let i = 0; i < existingVarSkus.length; i += 20) {
              const batch = existingVarSkus.slice(i, i + 20);
              const qs = batch.map(s => `sku=${encodeURIComponent(s)}`).join('&');
              const bd = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?${qs}`, { headers: auth })
                .then(r => r.json()).catch(() => ({}));
              for (const item of (bd.inventoryItems || [])) {
                const asp = item.inventoryItem?.product?.aspects || {};
                const color = (asp['Color'] || asp['color'] || [])[0] || '';
                const size  = (asp['Size']  || asp['size']  || [])[0] || '';
                const key   = `${color}|${size}`;
                const qty   = item.inventoryItem?.availability?.shipToLocationAvailability?.quantity ?? 1;
                comboAsin[key]    = item.sku;  // use sku as stand-in ASIN key
                comboInStock[key] = qty > 0;
                skuQtys[item.sku] = qty;
                const imgs = item.inventoryItem?.product?.imageUrls || [];
                imgs.forEach(u => { if (!allImgs.includes(u)) allImgs.push(u); });
              }
            }
            // Also grab group images
            const grpImgs = grpR.variantSKUSpecifics?.flatMap(s => []) || [];
            const finalImgs = allImgs.length ? allImgs : (grpR.aspects ? [] : []);

            const cachedPrice = parseFloat(body.fallbackPrice) || 0;
            product = {
              title: body.fallbackTitle || 'Product',
              price: cachedPrice,
              images: finalImgs,
              inStock: Object.values(comboInStock).some(Boolean),
              hasVariations: true,
              variations: [], variationImages: {},
              comboPrices: {}, sizePrices: {}, comboAsin, comboInStock,
              aspects: {}, breadcrumbs: [], bullets: [], descriptionPara: '',
            };
            // If we couldn't get images from variants, fall through to simple item fetch
            if (!product.images.length) product = null;
          }

          if (!product || !product.images?.length) {
            // Simple listing or group had no images — fetch the single inventory item
            const invR = await fetch(
              `${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`,
              { headers: auth }
            );
            if (invR.ok) {
              const invD = await invR.json();
              const ebayImgs = invD.product?.imageUrls || [];
              if (ebayImgs.length) {
                console.log(`[revise] using ${ebayImgs.length} images from live eBay simple item`);
                const currentQty = invD.availability?.shipToLocationAvailability?.quantity ?? 1;
                const cachedPrice = parseFloat(body.fallbackPrice) || 0;
                product = {
                  title: body.fallbackTitle || invD.product?.title || 'Product',
                  price: cachedPrice,
                  images: ebayImgs,
                  inStock: currentQty > 0,
                  hasVariations: false, variations: [], variationImages: {},
                  comboPrices: {}, sizePrices: {}, comboAsin: {}, comboInStock: {},
                  aspects: invD.product?.aspects || {},
                  breadcrumbs: [], bullets: [], descriptionPara: '',
                };
              }
            }
          }
        } catch(e) { console.warn('[revise] eBay inventory fallback failed:', e.message); }

        if (!product || !product.images?.length) {
          // Return 503 so the worker skips silently; frontend shows a softer message
          return res.status(503).json({
            error: 'Amazon is blocking right now and no cached images are available. Will retry automatically.',
            skippable: true,
          });
        }
      }
      console.log(`[revise] scraped: "${product.title?.slice(0,50)}" imgs=${product.images.length} price=$${product.price} hasVar=${product.hasVariations}`);

      // ── STEP 2: AI category + title + aspects (same as push) ─────────────────
      const suggestions = await getCategories(product.title || '', access_token).catch(() => []);
      const ai          = await aiEnrich(product.title, product.breadcrumbs || [], product.aspects || {}, suggestions).catch(() => null);
      const categoryId  = ai?.categoryId || suggestions[0]?.id || '11450';
      const rawTitle    = product.ebayTitle || ai?.title || product.title || 'Product';
      const listingTitle = sanitizeTitle(rawTitle) || sanitizeTitle(product.title) || 'Product';
      console.log(`[revise] title="${listingTitle.slice(0,50)}" cat=${categoryId}`);

      // ── STEP 3: Build eBay description (same as push) ────────────────────────
      const buildEbayDesc = (title, bullets, para, aspects) => {
        const bulletHtml = (bullets || []).length
          ? '<ul>' + bullets.map(b => `<li>${String(b).replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>'
          : '';
        const specRows = Object.entries(aspects || {})
          .filter(([k, v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v[0] && String(v[0]).length < 80)
          .slice(0, 10)
          .map(([k, v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
        const specsTable = specRows
          ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>` : '';
        return [
          `<h2>${title}</h2>`, bulletHtml,
          para ? `<p>${para}</p>` : '', specsTable,
          '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>',
        ].filter(Boolean).join('\n');
      };
      const ebayDescription = buildEbayDesc(listingTitle, product.bullets || [], product.descriptionPara || '', product.aspects || {})
                           || product.description || listingTitle;

      // Base aspects — strip Color/Size (variants set their own)
      const aspects = { ...(product.aspects || {}), ...(ai?.aspects || {}) };
      delete aspects['Color']; delete aspects['color'];
      delete aspects['Size'];  delete aspects['size'];

      // Auto-fill required item specifics (same as push)
      try {
        const catMeta = await fetch(
          `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_item_aspects_for_category?category_id=${categoryId}`,
          { headers: { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' } }
        ).then(r => r.json()).catch(() => ({}));
        for (const aspect of (catMeta.aspects || [])) {
          const name = aspect.aspectConstraint?.aspectRequired ? aspect.localizedAspectName : null;
          if (!name || aspects[name]) continue;
          const vals = (aspect.aspectValues || []).map(v => v.localizedValue);
          if (!vals.length) continue;
          const match = vals.find(v => (product.title || '').toLowerCase().includes(v.toLowerCase())) || vals[0];
          aspects[name] = [match];
        }
      } catch(e) { console.warn('[revise] aspects fetch failed:', e.message); }

      const freshStock    = product.inStock !== false;
      const colorGroup    = product.variations?.find(v => /color|colour/i.test(v.name));
      const sizeGroup     = product.variations?.find(v => /size/i.test(v.name));
      const colorImgs     = product.variationImages?.['Color'] || {};
      const comboPrices   = product.comboPrices || {};
      const sizePrices    = product.sizePrices  || {};
      const basePrice     = parseFloat(product.price || 0);

      // ── STEP 4: Get existing variant SKUs from eBay ───────────────────────────
      const groupRes = await fetch(
        `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`,
        { headers: auth }
      ).then(r => r.json()).catch(() => null);
      const variantSkus = groupRes?.variantSKUs || [];
      // isVariation: trust eBay's existing group SKUs (handles eBay-fallback case where variations[] may be empty)
      const isVariation = variantSkus.length > 0;
      console.log(`[revise] mode=${isVariation?'variation':'simple'} existingVarSkus=${variantSkus.length}`);

      // ── STEP 5: Read existing variant aspects to get color+size per SKU ──────
      // This is more reliable than guessing from SKU string
      const skuAspects = {};  // sku → { Color, Size }
      if (isVariation && variantSkus.length) {
        // Batch GET up to 20 inventory items at a time
        for (let i = 0; i < variantSkus.length; i += 20) {
          const batch = variantSkus.slice(i, i + 20);
          const qs = batch.map(s => `sku=${encodeURIComponent(s)}`).join('&');
          const bd = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?${qs}`, { headers: auth })
            .then(r => r.json()).catch(() => ({}));
          for (const item of (bd.inventoryItems || [])) {
            const asp = item.inventoryItem?.product?.aspects || {};
            skuAspects[item.sku] = {
              Color: (asp['Color'] || asp['color'] || [])[0] || null,
              Size:  (asp['Size']  || asp['size']  || [])[0] || null,
            };
          }
          if (i + 20 < variantSkus.length) await sleep(100);
        }
        console.log(`[revise] read aspects for ${Object.keys(skuAspects).length}/${variantSkus.length} SKUs`);
      }

      // ── STEP 6A: Update all variant inventory items (full wipe + replace) ─────
      if (isVariation) {
        const getPriceForVariant = (color, size) => {
          // Mirror push price logic exactly:
          // comboPrices[color|size] → sizePrices[size] → color.price → size.price → basePrice
          const key = `${color||''}|${size||''}`;
          const cv  = colorGroup?.values?.find(v => v.value === color);
          const sv  = sizeGroup?.values?.find(v => v.value === size);
          const amazonPrice = comboPrices[key]
                           || sizePrices[size || '']
                           || parseFloat(cv?.price || sv?.price || basePrice || 0);
          const p = applyMk(amazonPrice);
          return p > 0 ? p : applyMk(basePrice) || 9.99;
        };

        // Map Amazon color names to scraped color images
        // Also build a fallback map from eBay's existing skuAspects color names
        const buildColorImgMap = () => {
          const map = { ...colorImgs }; // Amazon color → image URL
          // Try case-insensitive match for eBay color names that differ slightly
          const lowerMap = {};
          for (const [k, v] of Object.entries(colorImgs)) lowerMap[k.toLowerCase()] = v;
          // Add eBay-side color names as keys too
          for (const { Color } of Object.values(skuAspects)) {
            if (Color && !map[Color]) {
              const match = lowerMap[Color.toLowerCase()];
              if (match) map[Color] = match;
            }
          }
          return map;
        };
        const colorImgMap = buildColorImgMap();

        const getImageForColor = (color) => {
          if (!color) return product.images[0] || '';
          return colorImgMap[color]
            || colorImgs[color]
            || product.images[0]
            || '';
        };

        const comboAsin    = product.comboAsin    || {};
        const comboInStock = product.comboInStock || {};
        console.log(`[revise] comboAsin=${Object.keys(comboAsin).length} comboInStock=${Object.keys(comboInStock).length} freshStock=${freshStock}`);

        const getQtyForVariant = (color, size) => {
          if (!freshStock) return 0;
          // If we have comboAsin data, check if combo exists AND is in stock on Amazon
          if (Object.keys(comboAsin).length) {
            const key = `${color||''}|${size||''}`;
            if (!comboAsin[key]) return 0;           // combo doesn't exist on Amazon
            if (comboInStock[key] === false) return 0; // combo exists but OOS on Amazon
            return defaultQty;
          }
          return defaultQty; // no comboAsin data — assume in stock
        };

        // Full-replace each variant inventory item
        const createdSkus = new Set();
        const failedSkus  = [];

        // First item: test for 401
        if (variantSkus.length > 0) {
          const testSku = variantSkus[0];
          const { Color: testColor, Size: testSize } = skuAspects[testSku] || {};
          const testAsp = { ...aspects };
          if (testColor) testAsp['Color'] = [testColor];
          if (testSize)  testAsp['Size']  = [testSize];
          const testImg = getImageForColor(testColor);
          const testR   = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(testSku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              availability: { shipToLocationAvailability: { quantity: getQtyForVariant(testColor, testSize) } },
              condition: 'NEW',
              product: {
                title:       listingTitle,
                description: ebayDescription,
                imageUrls:   testImg ? [testImg] : product.images.slice(0, 1),
                aspects:     testAsp,
              },
            }),
          });
          if (testR.status === 401) {
            return res.status(401).json({
              error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.',
              code: 'INVENTORY_401',
            });
          }
          if (testR.ok || testR.status === 204) createdSkus.add(testSku);
          else { const t = await testR.text(); console.warn(`[revise] test PUT fail: ${testR.status} ${t.slice(0,100)}`); failedSkus.push(testSku); }
        }

        // Remaining variants in batches of 15
        for (let i = 1; i < variantSkus.length; i += 15) {
          await Promise.all(variantSkus.slice(i, i + 15).map(async (sku) => {
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const asp = { ...aspects };
            if (color) asp['Color'] = [color];
            if (size)  asp['Size']  = [size];
            const img = getImageForColor(color);
            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: getQtyForVariant(color, size) } },
                condition: 'NEW',
                product: {
                  title:       listingTitle,
                  description: ebayDescription,
                  imageUrls:   img ? [img] : product.images.slice(0, 1),
                  aspects:     asp,
                },
              }),
            });
            if (r.ok || r.status === 204) createdSkus.add(sku);
            else { const t = await r.text(); console.warn(`[revise] PUT fail ${sku.slice(-20)}: ${r.status} ${t.slice(0,80)}`); failedSkus.push(sku); }
          }));
          if (i + 15 < variantSkus.length) await sleep(100);
        }

        // One retry pass for failed items
        if (failedSkus.length) {
          await sleep(800);
          for (const sku of failedSkus) {
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const asp = { ...aspects }; if (color) asp['Color'] = [color]; if (size) asp['Size'] = [size];
            const img = getImageForColor(color);
            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: getQtyForVariant(color, size) } },
                condition: 'NEW',
                product: { title: listingTitle, description: ebayDescription,
                           imageUrls: img ? [img] : product.images.slice(0, 1), aspects: asp },
              }),
            });
            if (r.ok || r.status === 204) createdSkus.add(sku);
          }
        }

        console.log(`[revise] inventory items: ${createdSkus.size}/${variantSkus.length} updated`);

        // ── STEP 6B: Full-replace the inventory_item_group ───────────────────────
        const varAspects = {};
        if (colorGroup) {
          varAspects['Color'] = colorGroup.values.map(v => v.value);
        } else {
          // eBay fallback: derive Color/Size lists from what eBay already has on the variants
          const colors = [...new Set(Object.values(skuAspects).map(a => a.Color).filter(Boolean))];
          const sizes  = [...new Set(Object.values(skuAspects).map(a => a.Size).filter(Boolean))];
          if (colors.length) varAspects['Color'] = colors;
          if (sizes.length)  varAspects['Size']  = sizes;
        }
        if (sizeGroup) varAspects['Size'] = sizeGroup.values.map(v => v.value);
        // Group gets ALL product images (up to 12); per-SKU images are on inventory items
        const groupImageUrls = product.images.slice(0, 12);
        const groupAspects = { ...aspects };
        delete groupAspects['Color']; delete groupAspects['Size'];
        const variesBySpecs = Object.entries(varAspects).map(([name, values]) => ({ name, values }));

        for (let attempt = 1; attempt <= 3; attempt++) {
          const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              inventoryItemGroupKey: ebaySku,
              title:       listingTitle,
              description: ebayDescription,
              imageUrls:   groupImageUrls,
              variantSKUs: variantSkus.filter(s => createdSkus.has(s)),
              aspects:     groupAspects,
              variesBy: {
                aspectsImageVariesBy: varAspects['Color'] ? ['Color'] : [],
                specifications: variesBySpecs,
              },
            }),
          });
          if (gr.ok || gr.status === 204) { console.log('[revise] group PUT ok'); break; }
          const gt = await gr.text();
          console.warn(`[revise] group attempt ${attempt}: ${gr.status} ${gt.slice(0, 200)}`);
          if (attempt < 3) await sleep(600);
        }

        // ── STEP 6C: Update all offer prices ────────────────────────────────────
        let pricesUpdated = 0;
        for (let i = 0; i < variantSkus.length; i += 8) {
          await Promise.all(variantSkus.slice(i, i + 8).map(async (sku) => {
            if (!createdSkus.has(sku)) return; // skip failed ones
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const price = getPriceForVariant(color, size);
            const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth })
              .then(r => r.json()).catch(() => ({}));
            const offerId = (ol.offers || [])[0]?.offerId;
            if (offerId) {
              await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, {
                method: 'PUT', headers: auth,
                body: JSON.stringify({ pricingSummary: { price: { value: price.toFixed(2), currency: 'USD' } } }),
              }).catch(() => {});
              pricesUpdated++;
            }
          }));
          if (i + 8 < variantSkus.length) await sleep(100);
        }

        console.log(`[revise] done — ${createdSkus.size} variants updated, ${pricesUpdated} prices updated, ${failedSkus.length} failed`);
        return res.json({
          success:         true,
          type:            'variant',
          updated:         createdSkus.size,
          updatedVariants: createdSkus.size,
          pricesUpdated,
          failed:          failedSkus.length,
          total:           variantSkus.length,
          images:          product.images.length,
          price:           applyMk(product.price),
          inStock:         freshStock,
          title:           listingTitle,
          priceChanges:    [],
          stockChanges:    [],
          imageChanges:    [],
        });

      } else {
        // ── STEP 6 (simple): Full wipe + replace single inventory item ───────────
        const newPrice = applyMk(product.price) || 9.99;
        const newQty   = freshStock ? defaultQty : 0;

        const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: newQty } },
            condition: 'NEW',
            product: {
              title:       listingTitle,
              description: ebayDescription,
              imageUrls:   product.images.slice(0, 12),
              aspects,
            },
          }),
        });
        if (ir.status === 401) {
          return res.status(401).json({
            error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.',
            code: 'INVENTORY_401',
          });
        }
        if (!ir.ok) {
          const t = await ir.text();
          console.warn('[revise/simple] inventory PUT failed:', ir.status, t.slice(0, 150));
          return res.status(400).json({ error: `Inventory update failed: ${t.slice(0,200)}` });
        }

        // Update offer price
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth })
          .then(r => r.json()).catch(() => ({}));
        const offerId = (ol.offers || [])[0]?.offerId;
        if (offerId) {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({ pricingSummary: { price: { value: newPrice.toFixed(2), currency: 'USD' } } }),
          }).catch(() => {});
        }

        console.log(`[revise/simple] title="${listingTitle.slice(0,40)}" price=$${newPrice} qty=${newQty} imgs=${product.images.length}`);
        return res.json({
          success:         true,
          type:            'simple',
          updated:         1,
          updatedVariants: 1,
          images:          product.images.length,
          price:           newPrice,
          ebayPrice:       newPrice,
          inStock:         freshStock,
          title:           listingTitle,
          priceChanges:    [],
          stockChanges:    newQty === 0 ? ['Out of stock'] : [],
          imageChanges:    [],
        });
      }
    }


    // ── SYNC: same strategy as push/revise, keeps existing listing ID ────────────
    if (action === 'sync') {
      // Strategy: identical to revise — full inventory item PUT (title+desc+images+aspects+qty)
      // + group PUT + offer price update. Returns a diff summary of what changed.
      const { access_token, ebaySku, sourceUrl } = body;
      if (!access_token || !ebaySku || !sourceUrl) {
        return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });
      }
      const sandbox    = body.sandbox === true || body.sandbox === 'true';
      const EBAY_API   = getEbayUrls(sandbox).EBAY_API;
      const markupPct  = parseFloat(body.markup ?? 0);
      const handling   = parseFloat(body.handlingCost ?? 2);
      const ebayFee    = 0.1335;
      const defaultQty = parseInt(body.quantity) || 1;
      const applyMk    = (cost) => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return 0;
        return Math.max(Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - ebayFee) + 0.30) * 100) / 100, 0.99);
      };
      const auth = {
        Authorization:      `Bearer ${access_token}`,
        'Content-Type':     'application/json',
        'Content-Language': 'en-US',
        'Accept-Language':  'en-US',
      };
      console.log(`[sync] sku=${ebaySku?.slice(0,35)} markup=${markupPct}%`);

      // ── STEP 1: Full Amazon scrape ────────────────────────────────────────────
      const baseUrl = process.env.VERCEL_URL
        ? `https://${process.env.VERCEL_URL}`
        : 'https://dropsync-one.vercel.app';
      const scrapeR = await fetch(`${baseUrl}/api/ebay?action=scrape`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: sourceUrl }),
      }).catch(e => { console.warn('[sync] scrape failed:', e.message); return null; });
      const scrapeD = scrapeR?.ok ? await scrapeR.json().catch(() => null) : null;
      let product   = scrapeD?.product || null;

      // Fallback to cached frontend data if Amazon blocked
      const fallbackImages  = Array.isArray(body.fallbackImages) ? body.fallbackImages.filter(u => typeof u === 'string' && u.startsWith('http')) : [];
      const fallbackPrice   = parseFloat(body.fallbackPrice) || 0;
      const fallbackInStock = body.fallbackInStock !== false;
      const fallbackTitle   = typeof body.fallbackTitle === 'string' ? body.fallbackTitle : '';

      if (!product) {
        // Amazon blocked — use any cached data from frontend (images, price, title)
        if (fallbackTitle || fallbackPrice > 0 || fallbackImages.length) {
          console.log(`[sync] scrape blocked — using cached fallback: title="${fallbackTitle?.slice(0,40)}" imgs=${fallbackImages.length} price=$${fallbackPrice}`);
          product = {
            title: fallbackTitle || 'Product', price: fallbackPrice, images: fallbackImages,
            inStock: fallbackInStock, hasVariations: false, variations: [], variationImages: {},
            comboPrices: {}, sizePrices: {}, aspects: {}, breadcrumbs: [], bullets: [], descriptionPara: '',
          };
        }
      } else if (!product.images?.length && fallbackImages.length) {
        product.images = fallbackImages;
      }

      if (!product) {
        return res.status(400).json({ error: 'Amazon is blocking requests and no cached data is available. Open the product on the Import tab to refresh it first.' });
      }
      console.log(`[sync] scraped: "${product.title?.slice(0,50)}" imgs=${product.images.length} price=$${product.price} hasVar=${product.hasVariations}`);

      // ── STEP 2: AI category + title + aspects (same as push/revise) ─────────
      const suggestions = await getCategories(product.title || '', access_token).catch(() => []);
      const ai          = await aiEnrich(product.title, product.breadcrumbs || [], product.aspects || {}, suggestions).catch(() => null);
      const categoryId  = ai?.categoryId || suggestions[0]?.id || '11450';
      const rawTitle    = product.ebayTitle || ai?.title || product.title || 'Product';
      const listingTitle = sanitizeTitle(rawTitle) || sanitizeTitle(product.title) || 'Product';

      // ── STEP 3: Build description (same as push/revise) ──────────────────────
      const buildEbayDesc = (title, bullets, para, asp) => {
        const bulletHtml = (bullets || []).length
          ? '<ul>' + bullets.map(b => `<li>${String(b).replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>' : '';
        const specRows = Object.entries(asp || {})
          .filter(([k,v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v[0] && String(v[0]).length < 80)
          .slice(0, 10).map(([k,v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
        const specsTable = specRows
          ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>` : '';
        return [`<h2>${title}</h2>`, bulletHtml, para ? `<p>${para}</p>` : '', specsTable,
          '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>',
        ].filter(Boolean).join('\n');
      };
      const ebayDescription = buildEbayDesc(listingTitle, product.bullets || [], product.descriptionPara || '', product.aspects || '')
                           || product.description || listingTitle;

      const aspects = { ...(product.aspects || {}), ...(ai?.aspects || {}) };
      delete aspects['Color']; delete aspects['color'];
      delete aspects['Size'];  delete aspects['size'];

      // Auto-fill required item specifics
      try {
        const catMeta = await fetch(
          `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_item_aspects_for_category?category_id=${categoryId}`,
          { headers: { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' } }
        ).then(r => r.json()).catch(() => ({}));
        for (const aspect of (catMeta.aspects || [])) {
          const name = aspect.aspectConstraint?.aspectRequired ? aspect.localizedAspectName : null;
          if (!name || aspects[name]) continue;
          const vals = (aspect.aspectValues || []).map(v => v.localizedValue);
          if (!vals.length) continue;
          const match = vals.find(v => (product.title || '').toLowerCase().includes(v.toLowerCase())) || vals[0];
          aspects[name] = [match];
        }
      } catch(e) { console.warn('[sync] aspects fetch failed:', e.message); }

      const freshStock  = product.inStock !== false;
      const colorGroup  = product.variations?.find(v => /color|colour/i.test(v.name));
      const sizeGroup   = product.variations?.find(v => /size/i.test(v.name));
      const colorImgs   = product.variationImages?.['Color'] || {};
      const comboPrices = product.comboPrices || {};
      const sizePrices  = product.sizePrices  || {};
      const comboAsin   = product.comboAsin   || {};
      const comboInStock = product.comboInStock || {};
      const basePrice   = parseFloat(product.price || 0);

      // Per-variant price — exact same logic as push/revise
      const getPriceForVariant = (color, size) => {
        const key = `${color||''}|${size||''}`;
        const cv  = colorGroup?.values?.find(v => v.value === color);
        const sv  = sizeGroup?.values?.find(v => v.value === size);
        const amazonPrice = comboPrices[key]
                         || sizePrices[size || '']
                         || parseFloat(cv?.price || sv?.price || basePrice || 0);
        const p = applyMk(amazonPrice);
        return p > 0 ? p : applyMk(basePrice) || 9.99;
      };

      // Per-variant qty — combo must exist AND be in stock on Amazon
      const getQtyForVariant = (color, size) => {
        if (!freshStock) return 0;
        if (Object.keys(comboAsin).length) {
          const key = `${color||''}|${size||''}`;
          if (!comboAsin[key]) return 0;
          // comboInStock[key] defaults to true if we couldn't check (unknown = assume in stock)
          if (comboInStock[key] === false) return 0;
          return defaultQty;
        }
        return defaultQty;
      };

      // Per-color image — same as push/revise
      const getImageForColor = (color) =>
        (color ? colorImgs[color] : null) || product.images[0] || '';

      console.log(`[sync] comboPrices=${Object.keys(comboPrices).length} sizePrices=${Object.keys(sizePrices).length} comboAsin=${Object.keys(comboAsin).length}`);

      // ── STEP 4: Get existing variant SKUs from eBay ──────────────────────────
      const groupRes    = await fetch(
        `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`,
        { headers: auth }
      ).then(r => r.json()).catch(() => null);
      const variantSkus = groupRes?.variantSKUs || [];
      const isVariation = variantSkus.length > 0;
      console.log(`[sync] type=${isVariation?'variation':'simple'} variantSkus=${variantSkus.length}`);

      // Change tracking
      const priceChanges = [], stockChanges = [], imageChanges = [];

      if (isVariation) {
        // ── VARIATION: full inventory PUT per variant (same as push/revise) ────

        // Read Color+Size aspects for all existing variant SKUs from eBay
        const skuAspects = {};  // sku → { Color, Size }
        for (let i = 0; i < variantSkus.length; i += 20) {
          const batch = variantSkus.slice(i, i + 20);
          const qs    = batch.map(s => `sku=${encodeURIComponent(s)}`).join('&');
          const bd    = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?${qs}`, { headers: auth })
            .then(r => r.json()).catch(() => ({}));
          for (const item of (bd.inventoryItems || [])) {
            const asp = item.inventoryItem?.product?.aspects || {};
            skuAspects[item.sku] = {
              Color: (asp['Color'] || asp['color'] || [])[0] || null,
              Size:  (asp['Size']  || asp['size']  || [])[0] || null,
            };
          }
          if (i + 20 < variantSkus.length) await sleep(100);
        }
        console.log(`[sync] aspects read: ${Object.keys(skuAspects).length}/${variantSkus.length}`);

        // Full inventory PUT per variant — same as push/revise
        // title + description + imageUrls (per-color) + aspects + qty
        const createdSkus = new Set();
        const failedSkus  = [];

        // Test first variant for 401
        if (variantSkus.length > 0) {
          const testSku = variantSkus[0];
          const { Color: testColor, Size: testSize } = skuAspects[testSku] || {};
          const testAsp = { ...aspects };
          if (testColor) testAsp['Color'] = [testColor];
          if (testSize)  testAsp['Size']  = [testSize];
          const testImg = getImageForColor(testColor);

          // Track old qty for stock change detection
          const oldItem = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(testSku)}`, { headers: auth })
            .then(r => r.ok ? r.json() : {}).catch(() => ({}));
          const oldQty  = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;
          const newQty  = getQtyForVariant(testColor, testSize);

          const testR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(testSku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              availability: { shipToLocationAvailability: { quantity: newQty } },
              condition: 'NEW',
              product: {
                title:       listingTitle,
                description: ebayDescription,
                imageUrls:   [testImg, ...product.images.filter(x => x !== testImg)].filter(Boolean).slice(0, 12),
                aspects:     testAsp,
              },
            }),
          });
          if (testR.status === 401) {
            return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.', code: 'INVENTORY_401' });
          }
          if (testR.ok || testR.status === 204) {
            createdSkus.add(testSku);
            if (oldQty >= 0 && oldQty !== newQty) {
              const label = [testColor, testSize].filter(Boolean).join('/') || testSku.slice(-12);
              stockChanges.push(`${label}: qty ${oldQty}→${newQty}`);
              console.log(`[sync] stock ${label}: ${oldQty}→${newQty}`);
            }
          } else {
            const t = await testR.text();
            console.warn(`[sync] test PUT fail: ${testR.status} ${t.slice(0,100)}`);
            failedSkus.push(testSku);
          }
        }

        // Remaining variants in batches of 8 (same as push/revise)
        for (let i = 1; i < variantSkus.length; i += 8) {
          await Promise.all(variantSkus.slice(i, i + 8).map(async (sku) => {
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const asp = { ...aspects };
            if (color) asp['Color'] = [color];
            if (size)  asp['Size']  = [size];
            const img    = getImageForColor(color);
            const newQty = getQtyForVariant(color, size);

            // Track old qty
            const oldItem = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { headers: auth })
              .then(r => r.ok ? r.json() : {}).catch(() => ({}));
            const oldQty = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;

            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: newQty } },
                condition: 'NEW',
                product: {
                  title:       listingTitle,
                  description: ebayDescription,
                  imageUrls:   img ? [img] : product.images.slice(0, 1),
                  aspects:     asp,
                },
              }),
            });
            if (r.ok || r.status === 204) {
              createdSkus.add(sku);
              if (oldQty >= 0 && oldQty !== newQty) {
                const label = [color, size].filter(Boolean).join('/') || sku.slice(-12);
                stockChanges.push(`${label}: qty ${oldQty}→${newQty}`);
              }
            } else {
              const t = await r.text();
              console.warn(`[sync] PUT fail ${sku.slice(-20)}: ${r.status} ${t.slice(0,80)}`);
              failedSkus.push(sku);
            }
          }));
          if (i + 8 < variantSkus.length) await sleep(150);
        }

        // Retry failed items once
        if (failedSkus.length) {
          await sleep(800);
          for (const sku of failedSkus) {
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const asp = { ...aspects }; if (color) asp['Color'] = [color]; if (size) asp['Size'] = [size];
            const img    = getImageForColor(color);
            const newQty = getQtyForVariant(color, size);
            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: newQty } },
                condition: 'NEW',
                product: { title: listingTitle, description: ebayDescription,
                           imageUrls: [img, ...product.images.filter(x => x !== img)].filter(Boolean).slice(0, 12), aspects: asp },
              }),
            });
            if (r.ok || r.status === 204) createdSkus.add(sku);
          }
        }
        console.log(`[sync] inventory items: ${createdSkus.size}/${variantSkus.length} updated, ${failedSkus.length} failed`);

        // ── GROUP PUT: fresh images + variesBy (same as push/revise) ───────────
        const varAspects = {};
        if (colorGroup) varAspects['Color'] = colorGroup.values.map(v => v.value);
        if (sizeGroup)  varAspects['Size']  = sizeGroup.values.map(v => v.value);
        const colorUrlList   = Object.values(colorImgs).filter(Boolean).slice(0, 12);
        const groupImageUrls = colorUrlList.length ? colorUrlList : product.images.slice(0, 12);
        const groupAspects   = { ...aspects }; delete groupAspects['Color']; delete groupAspects['Size'];
        const variesBySpecs  = Object.entries(varAspects).map(([name, values]) => ({ name, values }));

        // Track group image changes
        const prevGroupImages = groupRes?.imageUrls || [];
        if (prevGroupImages.length !== groupImageUrls.length) {
          imageChanges.push(`Gallery: ${prevGroupImages.length}→${groupImageUrls.length} images`);
        }

        for (let attempt = 1; attempt <= 3; attempt++) {
          const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              inventoryItemGroupKey: ebaySku,
              title:       listingTitle,
              description: ebayDescription,
              imageUrls:   groupImageUrls,
              variantSKUs: variantSkus.filter(s => createdSkus.has(s)),
              aspects:     groupAspects,
              variesBy: {
                aspectsImageVariesBy: colorGroup ? ['Color'] : [],
                specifications: variesBySpecs,
              },
            }),
          });
          if (gr.ok || gr.status === 204) { console.log('[sync] group PUT ok'); break; }
          const gt = await gr.text();
          console.warn(`[sync] group attempt ${attempt}: ${gr.status} ${gt.slice(0,200)}`);
          if (attempt < 3) await sleep(600);
        }

        // ── UPDATE OFFER PRICES: per-variant with correct color+size price ───
        let pricesUpdated = 0;
        for (let i = 0; i < variantSkus.length; i += 15) {
          await Promise.all(variantSkus.slice(i, i + 8).map(async (sku) => {
            if (!createdSkus.has(sku)) return;
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const newPrice = getPriceForVariant(color, size);
            const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth })
              .then(r => r.json()).catch(() => ({}));
            const offer = (ol.offers || [])[0];
            if (offer?.offerId) {
              const oldPrice = parseFloat(offer.pricingSummary?.price?.value || 0);
              if (Math.abs(newPrice - oldPrice) > 0.01) {
                await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, {
                  method: 'PUT', headers: auth,
                  body: JSON.stringify({ pricingSummary: { price: { value: newPrice.toFixed(2), currency: 'USD' } } }),
                }).catch(() => {});
                const label = [color, size].filter(Boolean).join('/') || sku.slice(-12);
                priceChanges.push(`${label}: $${oldPrice.toFixed(2)}→$${newPrice.toFixed(2)}`);
                console.log(`[sync] price ${label}: $${oldPrice.toFixed(2)}→$${newPrice.toFixed(2)}`);
              }
              pricesUpdated++;
            }
          }));
          if (i + 15 < variantSkus.length) await sleep(80);
        }

        console.log(`[sync] done — ${createdSkus.size} updated, ${pricesUpdated} prices checked, ${priceChanges.length} price changes, ${stockChanges.length} stock changes`);
        return res.json({
          success:      true,
          type:         'variation',
          updatedVariants: createdSkus.size,
          failed:       failedSkus.length,
          images:       product.images.length,
          price:        applyMk(basePrice),
          inStock:      freshStock,
          priceChanges,
          stockChanges,
          imageChanges,
        });

      } else {
        // ── SIMPLE LISTING: full inventory PUT (same as push/revise) ──────────
        const newPrice = applyMk(product.price) || 9.99;
        const newQty   = freshStock ? defaultQty : 0;

        // Track old values
        const oldItem  = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, { headers: auth })
          .then(r => r.ok ? r.json() : {}).catch(() => ({}));
        const oldQty   = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;
        const oldImgCount = (oldItem?.product?.imageUrls || []).length;

        const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: newQty } },
            condition: 'NEW',
            product: { title: listingTitle, description: ebayDescription, imageUrls: product.images.slice(0, 12), aspects },
          }),
        });
        if (ir.status === 401) {
          return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.', code: 'INVENTORY_401' });
        }
        if (!ir.ok) {
          const t = await ir.text();
          return res.status(400).json({ error: `Inventory update failed: ${t.slice(0,200)}` });
        }

        if (oldQty >= 0 && oldQty !== newQty) {
          stockChanges.push(`qty ${oldQty}→${newQty}`);
          console.log(`[sync/simple] stock: ${oldQty}→${newQty}`);
        }
        if (oldImgCount !== product.images.length) {
          imageChanges.push(`Images: ${oldImgCount}→${product.images.length}`);
        }

        // Update offer price
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth })
          .then(r => r.json()).catch(() => ({}));
        const offer = (ol.offers || [])[0];
        if (offer?.offerId) {
          const oldPrice = parseFloat(offer.pricingSummary?.price?.value || 0);
          if (Math.abs(newPrice - oldPrice) > 0.01) {
            await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({ pricingSummary: { price: { value: newPrice.toFixed(2), currency: 'USD' } } }),
            }).catch(() => {});
            priceChanges.push(`$${oldPrice.toFixed(2)} → $${newPrice.toFixed(2)}`);
            console.log(`[sync/simple] price: $${oldPrice.toFixed(2)} → $${newPrice.toFixed(2)}`);
          }
        }

        console.log(`[sync/simple] title="${listingTitle.slice(0,40)}" price=$${newPrice} qty=${newQty} imgs=${product.images.length}`);
        return res.json({
          success:      true,
          type:         'simple',
          updatedVariants: 1,
          images:       product.images.length,
          price:        newPrice,
          inStock:      freshStock,
          priceChanges,
          stockChanges,
          imageChanges,
        });
      }
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

      // Scrape multiple Amazon pages to get a large, diverse ASIN pool
      const AMAZON_PAGES = [
        'https://www.amazon.com/deals?ref_=nav_cs_gb',
        'https://www.amazon.com/s?i=fashion&rh=n%3A7141123011&fs=true&ref=lp_7141123011_sar',
        'https://www.amazon.com/s?i=fashion-womens&bbn=1040660&rh=n%3A1040660%2Cn%3A7147441011&ref=nav_em',
        'https://www.amazon.com/s?k=womens+clothing&i=fashion-womens&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=home+decor&i=garden&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=kitchen+gadgets&i=kitchen&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=jewelry+women&i=jewelry&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=baby+products&i=baby-products&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=sports+fitness&i=sporting&ref=nb_sb_noss',
        'https://www.amazon.com/gp/bestsellers/fashion/ref=zg_bs_nav_fashion_0',
        'https://www.amazon.com/gp/bestsellers/garden/ref=zg_bs_nav_garden_0',
        'https://www.amazon.com/gp/new-releases/fashion/ref=zg_bsnr_nav_fashion_0',
      ];

      const productMap = {}; // asin → { asin, title, url }
      let pagesLoaded = 0;

      // Fetch pages in parallel (up to 4 at a time) until we have enough ASINs
      const fetchPage_ = async (url) => {
        try {
          const h = await fetchPage(url, randUA());
          if (!h || h.length < 5000) return;
          // Extract ASINs using all patterns
          for (const m of h.matchAll(/"asin"\s*:\s*"([B][0-9A-Z]{9})"/g))
            productMap[m[1]] = productMap[m[1]] || { asin: m[1], title: '' };
          for (const m of h.matchAll(/data-asin="([B][0-9A-Z]{9})"/g))
            productMap[m[1]] = productMap[m[1]] || { asin: m[1], title: '' };
          for (const m of h.matchAll(/href="[^"]*\/([^\/\s"]{5,})\/dp\/([B][0-9A-Z]{9})[^"]*"/g)) {
            const slug = decodeURIComponent(m[1]).replace(/-/g,' ');
            if (slug.length > 5) productMap[m[2]] = { asin: m[2], title: slug, url: 'https://www.amazon.com/dp/'+m[2] };
          }
          for (const m of h.matchAll(/\/dp\/([B][0-9A-Z]{9})[^"]*"[^>]*>[\s\S]{0,300}?alt="([^"]{10,120})"/g)) {
            if (!productMap[m[1]]?.title || productMap[m[1]].title.length < 10)
              productMap[m[1]] = { asin: m[1], title: m[2], url: 'https://www.amazon.com/dp/'+m[1] };
          }
          pagesLoaded++;
          console.log('[dealsScrape] loaded page', url.slice(0,60), '→ pool='+Object.keys(productMap).length);
        } catch(e) { console.error('[dealsScrape] page error:', e.message); }
      };

      // Always fetch deals page + rotate through category pages based on exclude set size
      // so every "Fetch More" call hits different pages
      const pageOffset = Math.floor(excludeSet.size / 20) % AMAZON_PAGES.length;
      const pagesToFetch = [
        AMAZON_PAGES[0], // deals always
        ...AMAZON_PAGES.slice(1).sort(() => Math.random()-0.5).slice(0, 4)
      ];

      // Fetch in batches of 2 (avoid hammering)
      for (let i = 0; i < pagesToFetch.length; i += 2) {
        await Promise.all(pagesToFetch.slice(i, i+2).map(fetchPage_));
        if (Object.keys(productMap).length >= excludeSet.size + count + 50) break;
        if (i+2 < pagesToFetch.length) await new Promise(r => setTimeout(r, 600));
      }

      if (Object.keys(productMap).length === 0)
        return res.json({ success: false, error: 'Could not load Amazon pages. Try again in a moment.' });

      console.log('[dealsScrape] total pool after', pagesLoaded, 'pages:', Object.keys(productMap).length);
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

    if (action === 'getOrders') {
      const { access_token, limit = 50 } = body;
      const fromDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();
      const r = await fetch(
        `https://api.ebay.com/sell/fulfillment/v1/order?limit=${limit}&filter=lastmodifieddate:[${fromDate}..]`,
        { headers: { 'Authorization': `Bearer ${access_token}`, 'Content-Type': 'application/json' } }
      );
      const d = await r.json();
      if (!r.ok) return res.status(r.status).json({ error: d.errors?.[0]?.message || JSON.stringify(d) });
      return res.json({ orders: d.orders || [], total: d.total || 0 });
    }

    // ── RECOVER SKUS — maps all eBay listing IDs → real SKUs ────────────────
    if (action === 'recoverSkus') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const EBAY_API = getEbayUrls(false).EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' };
      const skuMap = {}; // listingId → sku
      let offset = 0, hasMore = true;
      while (hasMore && offset < 2000) {
        const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset}`, { headers: auth });
        const d = await r.json();
        const offers = d.offers || [];
        for (const o of offers) {
          const lid = o.listing?.listingId;
          if (lid && o.sku) skuMap[lid] = o.sku;
        }
        hasMore = offers.length === 100;
        offset += 100;
        if (offers.length) await new Promise(r => setTimeout(r, 100));
      }
      return res.json({ skuMap, total: Object.keys(skuMap).length });
    }

    return res.status(400).json({ error: `Unknown action: ${action}` });

  } catch (err) {
    console.error('[Error]', err.message);
    return res.status(500).json({ error: err.message });
  }
};
