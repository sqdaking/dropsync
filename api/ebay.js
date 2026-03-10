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
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];
const randUA = () => UA_LIST[Math.floor(Math.random() * UA_LIST.length)];

// ── Amazon page fetcher with retry + CAPTCHA detection ────────────────────────
async function fetchPage(url, ua) {
  for (let i = 0; i < 3; i++) {
    try {
      if (i > 0) await sleep(1500 * i);
      const r = await fetch(url, {
        headers: {
          'User-Agent': ua || randUA(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Cache-Control': 'no-cache',
          'Upgrade-Insecure-Requests': '1',
        },
        redirect: 'follow',
      });
      const html = await r.text();
      if (html.includes('Type the characters') || html.includes('robot check')) {
        if (i < 2) continue;
        return '';
      }
      return html;
    } catch (e) { if (i === 2) return ''; }
  }
  return '';
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

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
  "title": "<eBay-optimized title, under 80 chars, keyword-rich>",
  "aspects": {
    "Brand": ["Unbranded"],
    "Color": ["See Listing"],
    "Material": ["Polyester"],
    "Size": ["See Listing"],
    ... include 5-10 relevant aspects for this category
  }
}

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
      if (asin) url = `https://www.amazon.com/dp/${asin}`;

      const ua   = randUA();
      const html = await fetchPage(url, ua);
      if (!html) return res.json({ success: false, error: 'Could not load page — Amazon may be rate limiting. Try again in 1 min.' });

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

      // Description from bullet points
      const bullets = [...html.matchAll(/<span class="a-list-item">\s*([^<]{20,500})\s*<\/span>/g)]
        .map(m => m[1].trim()).slice(0, 5);
      product.description = bullets.join('\n') || product.title;

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

        // Build lookup tables
        const comboAsin    = {};  // "Color|Size" → ASIN
        const colorSizeMap = {};  // colorName → { sizeName → ASIN }

        for (const [code, asin] of Object.entries(dtaMap)) {
          const parts = code.split('_');
          if (parts.length !== 2) continue;
          const ci = parseInt(parts[0]), si = parseInt(parts[1]);
          const color = colors[ci], size = sizes[si];
          if (!color) continue;
          comboAsin[`${color}|${size || ''}`] = asin;
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

        // 5. Fetch one page per size to get the real prices (6 fetches max)
        const sizePrices = {};  // sizeName → price
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

        // Product base price = cheapest available size
        const allP = Object.values(sizePrices).filter(p => p > 0);
        if (allP.length) product.price = Math.min(...allP);
      }

      const colorGrp = product.variations.find(v=>v.name==='Color');
      const pricesFound = colorGrp ? colorGrp.values.filter(v=>v.price>0).length : 0;
      const imagesFound = colorGrp ? colorGrp.values.filter(v=>v.image).length : 0;
      console.log(`[scrape] OK "${product.title.slice(0,50)}" price=$${product.price} colors=${colorGrp?.values.length||0} prices=${pricesFound} images=${imagesFound} imgs=${product.images.length}`);
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
      const listingTitle = (ai?.title || product.title || 'Product').slice(0, 80);
      const aspects = { ...(product.aspects || {}), ...(ai?.aspects || {}) };
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
            product: { title: listingTitle, description: product.description || listingTitle, imageUrls: product.images.slice(0,12), aspects },
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
      const mkSku = parts => `${groupSku}-${parts.join('-').replace(/[^A-Z0-9]/gi,'_').toUpperCase().slice(0,40)}`;

      const defaultQty = parseInt(product.quantity) || 10;
      // comboAsin:  "Color|Size" → ASIN  (only combos that exist on Amazon)
      // sizePrices: sizeName → price (fetched from best-coverage color, applies to all colors at that size)
      const comboAsin  = product.comboAsin  || {};
      const sizePrices = product.sizePrices || {};
      console.log(`[push] comboAsin entries=${Object.keys(comboAsin).length} sizePrices entries=${Object.keys(sizePrices).length}`);

      if (colorGroup && sizeGroup) {
        for (const cv of colorGroup.values.filter(v => v.enabled !== false)) {
          for (const sv of sizeGroup.values.filter(v => v.enabled !== false)) {
            // Only push combos that actually exist on Amazon
            const key = `${cv.value}|${sv.value}`;
            if (Object.keys(comboAsin).length > 0 && !comboAsin[key]) continue;
            // Price comes from sizePrices (size determines price on this product type)
            const price = parseFloat(sizePrices[sv.value] || sv.price || basePrice).toFixed(2);
            variants.push({
              sku:   mkSku([cv.value, sv.value]),
              color: cv.value, size: sv.value,
              price,
              image: colorImgs[cv.value] || product.images[0] || '',
              qty:   defaultQty,
            });
          }
        }
      } else if (colorGroup) {
        for (const cv of colorGroup.values.filter(v => v.enabled !== false)) {
          variants.push({
            sku:   mkSku([cv.value]), color: cv.value, size: null,
            price: parseFloat(sizePrices[''] || cv.price || basePrice).toFixed(2),
            image: colorImgs[cv.value] || product.images[0] || '',
            qty:   defaultQty,
          });
        }
      } else if (sizeGroup) {
        for (const sv of sizeGroup.values.filter(v => v.enabled !== false)) {
          variants.push({
            sku:   mkSku([sv.value]), color: null, size: sv.value,
            price: parseFloat(sizePrices[sv.value] || sv.price || basePrice).toFixed(2),
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
              title: listingTitle, description: product.description || listingTitle,
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

      let okInv = 1; // count the test PUT above
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
                title: listingTitle, description: product.description || listingTitle,
                imageUrls: [v.image, ...product.images.filter(x => x !== v.image)].filter(Boolean).slice(0, 12),
                aspects: asp,
              },
            }),
          });
          if (r.ok || r.status === 204) okInv++;
          else console.warn(`[push] inv ${v.sku.slice(-15)}: ${r.status}`);
        }));
        if (i+8 < final.length) await sleep(150);
      }
      console.log(`[push] inventory items: ${okInv}/${final.length}`);

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
        const groupBody = {
            inventoryItemGroupKey: groupSku,
            title: listingTitle,
            description: product.description || listingTitle,
            imageUrls: groupImageUrls,
            variantSKUs: final.map(v => v.sku),
            aspects: varAspects,
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

      // Bulk create offers (25 at a time)
      const allOfferIds = [];
      for (let i = 0; i < final.length; i += 25) {
        const batch = final.slice(i, i+25).map(v => buildOffer(v.sku, v.price, categoryId, policies, locationKey));
        const or = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`, {
          method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
        });
        const od = await or.json();
        for (const resp of (od.responses || [])) {
          if (resp.offerId) { allOfferIds.push(resp.offerId); }
          else if (resp.errors?.length) {
            console.warn(`[offer] SKU ${resp.sku?.slice(-20)} error:`, JSON.stringify(resp.errors[0]).slice(0,200));
          }
        }
        const batchOk = (od.responses||[]).filter(r=>r.offerId).length;
        const batchFail = (od.responses||[]).filter(r=>!r.offerId).length;
        console.log(`[push] offers batch ${Math.floor(i/25)+1}: ${batchOk} ok, ${batchFail} failed`);
        if (batchFail && batchOk === 0) {
          // Return the first error to the frontend
          const firstErr = (od.responses||[]).find(r=>r.errors?.length);
          if (firstErr) return res.status(400).json({ error: 'Offer creation failed', details: firstErr.errors[0] });
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
    if (action === 'endListing') {
      const { access_token, ebaySku } = body;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
      const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      let ended = 0;
      for (const offer of (ol.offers||[])) {
        if (offer.status === 'PUBLISHED') {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}/withdraw`, { method: 'POST', headers: auth });
          ended++;
        }
      }
      return res.json({ success: true, ended });
    }

    return res.status(400).json({ error: `Unknown action: ${action}` });

  } catch (err) {
    console.error('[Error]', err.message);
    return res.status(500).json({ error: err.message });
  }
};
